"""PostgreSQL database backend.

Provides a connection wrapper that makes psycopg2 behave like sqlite3
from the caller's perspective — same execute/fetchone/fetchall interface,
automatic SQL dialect translation, and dict-style row access.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any


# ── SQL dialect translation ───────────────────────────────────────────────

# Patterns that differ between SQLite and PostgreSQL
_INSERT_OR_IGNORE_RE = re.compile(r"\bINSERT\s+OR\s+IGNORE\b", re.IGNORECASE)
_INSERT_OR_REPLACE_RE = re.compile(r"\bINSERT\s+OR\s+REPLACE\b", re.IGNORECASE)
_STRFTIME_NOW_RE = re.compile(
    r"strftime\s*\(\s*'[^']*'\s*,\s*'now'\s*\)", re.IGNORECASE
)
_JULIANDAY_RE = re.compile(r"\bjulianday\s*\(", re.IGNORECASE)
_INTEGER_PRIMARY_KEY_RE = re.compile(
    r"\bINTEGER\s+PRIMARY\s+KEY\b", re.IGNORECASE
)
_AUTOINCREMENT_RE = re.compile(r"\bAUTOINCREMENT\b", re.IGNORECASE)


_UPDATE_OR_IGNORE_RE = re.compile(r"\bUPDATE\s+OR\s+IGNORE\b", re.IGNORECASE)


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL into statements on ; but respect strings and parens."""
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    paren_depth = 0

    for ch in sql:
        if in_string:
            current.append(ch)
            if ch == "'":
                in_string = False
        elif ch == "'":
            in_string = True
            current.append(ch)
        elif ch == "(":
            paren_depth += 1
            current.append(ch)
        elif ch == ")":
            paren_depth = max(0, paren_depth - 1)
            current.append(ch)
        elif ch == ";" and paren_depth == 0:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)

    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


_COLLATE_NOCASE_RE = re.compile(r"\bCOLLATE\s+NOCASE\b", re.IGNORECASE)


def translate_sql(sql: str, is_ddl: bool = False) -> str:
    """Translate SQLite SQL to PostgreSQL-compatible SQL.

    Args:
        is_ddl: If True, skip parameter translation (no ? → %s).
    """
    out = sql

    # COLLATE NOCASE → ILIKE or just remove (PG text comparison is already case-sensitive,
    # and we use ILIKE for case-insensitive. For = comparisons, use LOWER())
    out = _COLLATE_NOCASE_RE.sub("", out)

    # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    if _INSERT_OR_IGNORE_RE.search(out):
        out = _INSERT_OR_IGNORE_RE.sub("INSERT", out)
        if "ON CONFLICT" not in out.upper():
            out = out.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # INSERT OR REPLACE → INSERT ... ON CONFLICT DO UPDATE
    if _INSERT_OR_REPLACE_RE.search(out):
        out = _INSERT_OR_REPLACE_RE.sub("INSERT", out)
        if "schema_version" in out.lower() and "ON CONFLICT" not in out.upper():
            out = out.rstrip().rstrip(";") + " ON CONFLICT (version) DO UPDATE SET version = EXCLUDED.version"
        elif "sync_state" in out.lower() and "ON CONFLICT" not in out.upper():
            out = out.rstrip().rstrip(";") + " ON CONFLICT (folder) DO UPDATE SET uidvalidity = EXCLUDED.uidvalidity, last_uid = EXCLUDED.last_uid, last_sync = EXCLUDED.last_sync, sync_token = EXCLUDED.sync_token"
        elif "calendar_events" in out.lower() and "ON CONFLICT" not in out.upper():
            out = out.rstrip().rstrip(";") + " ON CONFLICT (event_id) DO UPDATE SET title = EXCLUDED.title, description = EXCLUDED.description, location = EXCLUDED.location, start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time, status = EXCLUDED.status, attendees = EXCLUDED.attendees, updated_at = EXCLUDED.updated_at, fetched_at = EXCLUDED.fetched_at"
        elif "ON CONFLICT" not in out.upper():
            # Generic fallback: convert to upsert-or-nothing
            out = out.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # UPDATE OR IGNORE → UPDATE (PG just ignores constraint errors with ON CONFLICT)
    if _UPDATE_OR_IGNORE_RE.search(out):
        out = _UPDATE_OR_IGNORE_RE.sub("UPDATE", out)

    # strftime('%...', 'now') → CURRENT_TIMESTAMP
    out = _STRFTIME_NOW_RE.sub("CURRENT_TIMESTAMP", out)

    # datetime('now', '-N day/hour/...') → text-cast timestamp for comparison with TEXT columns
    out = re.sub(
        r"datetime\('now',\s*'(-?\d+)\s+(day|hour|minute|second)s?'\)",
        r"CAST((NOW() + INTERVAL '\1 \2') AS TEXT)",
        out, flags=re.IGNORECASE,
    )

    # date(?, '-N days') / date(?, '+N days') → CAST((CAST(? AS TIMESTAMP) + INTERVAL 'N days') AS TEXT)
    # SQLite date() with a base value and modifier. Keep ? for _translate_params to handle.
    out = re.sub(
        r"date\(\s*(\?)\s*,\s*'([+-]?\d+)\s+(day|hour|minute|second)s?'\s*\)",
        r"CAST((CAST(\1 AS TIMESTAMP) + INTERVAL '\2 \3') AS TEXT)",
        out, flags=re.IGNORECASE,
    )

    # INTEGER PRIMARY KEY → SERIAL PRIMARY KEY (for auto-increment)
    out = _INTEGER_PRIMARY_KEY_RE.sub("SERIAL PRIMARY KEY", out)
    out = _AUTOINCREMENT_RE.sub("", out)

    # json_group_array(DISTINCT x) → json_agg(DISTINCT x)
    # json_group_array(x) → json_agg(x)
    out = re.sub(r'json_group_array\(', 'json_agg(', out, flags=re.IGNORECASE)

    # SQLite MIN(a, b) / MAX(a, b) as scalar two-value functions → LEAST / GREATEST
    # Only match the two-arg form (not aggregate MIN/MAX which take one arg).
    # Use [^,)]+ for first arg to avoid matching across separate function calls.
    out = re.sub(r'\bMIN\(([^,)]+),\s*([^)]+)\)', r'LEAST(\1, \2)', out, flags=re.IGNORECASE)
    out = re.sub(r'\bMAX\(([^,)]+),\s*([^)]+)\)', r'GREATEST(\1, \2)', out, flags=re.IGNORECASE)

    # SQLite ? params → PostgreSQL %s params (skip for DDL)
    if not is_ddl:
        out = _translate_params(out)

    return out


def _translate_params(sql: str) -> str:
    """Replace ? placeholders with %s, but not inside strings."""
    result = []
    in_string = False
    quote_char = None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_string:
            result.append(ch)
            if ch == quote_char:
                # Check for escaped quote
                if i + 1 < len(sql) and sql[i + 1] == quote_char:
                    result.append(sql[i + 1])
                    i += 2
                    continue
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            quote_char = ch
            result.append(ch)
        elif ch == "?":
            result.append("%s")
        else:
            result.append(ch)
        i += 1
    return "".join(result)


# ── PostgreSQL connection wrapper ─────────────────────────────────────────

class PostgresRow:
    """Dict-like row that mimics sqlite3.Row."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return list(self._data.values())[key]
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def keys(self) -> list[str]:
        return list(self._data.keys())


class PostgresCursor:
    """Wraps a psycopg2 cursor to provide sqlite3-compatible interface."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self._lastrowid: int | None = None

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    # Alias used in some places
    @property
    def changes(self) -> int:
        return self._cursor.rowcount

    def fetchone(self) -> PostgresRow | None:
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._cursor.description:
            cols = [d[0] for d in self._cursor.description]
            return PostgresRow(dict(zip(cols, row)))
        return PostgresRow({str(i): v for i, v in enumerate(row)})

    def fetchall(self) -> list[PostgresRow]:
        rows = self._cursor.fetchall()
        if not rows or not self._cursor.description:
            return []
        cols = [d[0] for d in self._cursor.description]
        return [PostgresRow(dict(zip(cols, row))) for row in rows]


class PostgresConnection:
    """Wraps a psycopg2 connection to provide sqlite3-compatible interface.

    Translates SQL automatically and provides dict-style row access.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def row_factory(self) -> Any:
        return None

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        pass  # Ignored — we always use dict-style rows

    def execute(self, sql: str, params: tuple | list | dict = ()) -> PostgresCursor:
        pg_sql = translate_sql(sql)
        cursor = self._conn.cursor()

        # Handle named params (:name style) — convert to %(name)s
        if isinstance(params, dict):
            pg_sql = re.sub(r":(\w+)", r"%(\1)s", pg_sql)

        try:
            cursor.execute(pg_sql, params if params else None)
        except Exception:
            self._conn.rollback()
            raise

        wrapped = PostgresCursor(cursor)

        # Try to get lastrowid for INSERT statements
        if pg_sql.strip().upper().startswith("INSERT") and "RETURNING" not in pg_sql.upper():
            try:
                # Use savepoint so a failed lastval() doesn't poison the transaction
                cursor.execute("SAVEPOINT _lastval_check")
                cursor.execute("SELECT lastval()")
                result = cursor.fetchone()
                if result:
                    wrapped._lastrowid = result[0]
                cursor.execute("RELEASE SAVEPOINT _lastval_check")
            except Exception:
                cursor.execute("ROLLBACK TO SAVEPOINT _lastval_check")
                cursor.execute("RELEASE SAVEPOINT _lastval_check")

        return wrapped

    def executemany(self, sql: str, params_list: list) -> None:
        pg_sql = translate_sql(sql)
        cursor = self._conn.cursor()

        for params in params_list:
            if isinstance(params, dict):
                pg_sql_named = re.sub(r":(\w+)", r"%(\1)s", pg_sql)
                cursor.execute(pg_sql_named, params)
            else:
                cursor.execute(pg_sql, params)

    def executescript(self, sql: str) -> None:
        """Execute multiple SQL statements. Translates each one.

        Uses savepoints so a failed statement doesn't roll back the whole batch.
        """
        statements = _split_sql_statements(sql)
        cursor = self._conn.cursor()
        for i, stmt in enumerate(statements):
            pg_stmt = translate_sql(stmt, is_ddl=True)
            if not pg_stmt.strip():
                continue
            # Skip comment-only statements
            stripped = pg_stmt.strip().lstrip("-").strip()
            if not stripped or stripped.startswith("--"):
                continue
            sp = f"sp_{i}"
            try:
                cursor.execute(f"SAVEPOINT {sp}")
                cursor.execute(pg_stmt)
                cursor.execute(f"RELEASE SAVEPOINT {sp}")
            except Exception as e:
                import logging
                logging.getLogger("email_manager.db").warning(
                    "DDL statement failed (continuing): %s — %s", str(e)[:100], pg_stmt[:100]
                )
                cursor.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        self._conn.commit()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def cursor(self) -> PostgresCursor:
        return PostgresCursor(self._conn.cursor())


def get_postgres_connection(url: str) -> PostgresConnection:
    """Create a PostgreSQL connection from a URL."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 is required for PostgreSQL support. "
            "Install it with: pip install psycopg2-binary"
        )

    conn = psycopg2.connect(url)
    conn.autocommit = False
    return PostgresConnection(conn)
