from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from email_manager.config import Config

SCHEMA_VERSION = 3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS emails (
    id              INTEGER PRIMARY KEY,
    message_id      TEXT UNIQUE NOT NULL,
    thread_id       TEXT,
    subject         TEXT,
    normalised_subject TEXT,
    from_address    TEXT NOT NULL,
    from_name       TEXT,
    to_addresses    TEXT,
    cc_addresses    TEXT,
    date            TEXT NOT NULL,
    body_text       TEXT,
    body_html       TEXT,
    raw_headers     TEXT,
    folder          TEXT,
    size_bytes      INTEGER,
    has_attachments INTEGER DEFAULT 0,
    fetched_at      TEXT NOT NULL,
    gmail_id        TEXT
);

CREATE INDEX IF NOT EXISTS idx_emails_thread ON emails(thread_id);
CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date);
CREATE INDEX IF NOT EXISTS idx_emails_from ON emails(from_address);
CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(message_id);

CREATE TABLE IF NOT EXISTS sync_state (
    folder          TEXT PRIMARY KEY,
    uidvalidity     INTEGER NOT NULL,
    last_uid        INTEGER NOT NULL DEFAULT 0,
    last_sync       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contacts (
    id              INTEGER PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    name            TEXT,
    company         TEXT,
    first_seen      TEXT,
    last_seen       TEXT,
    email_count     INTEGER DEFAULT 0,
    sent_count      INTEGER DEFAULT 0,
    received_count  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS threads (
    id              INTEGER PRIMARY KEY,
    thread_id       TEXT UNIQUE NOT NULL,
    subject         TEXT,
    email_count     INTEGER DEFAULT 0,
    first_date      TEXT,
    last_date       TEXT,
    participants    TEXT,
    summary         TEXT,
    summary_model   TEXT
);

CREATE TABLE IF NOT EXISTS projects (
    id              INTEGER PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    description     TEXT,
    department      TEXT,
    workstream      TEXT,
    created_at      TEXT,
    is_auto         INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS email_projects (
    email_id        INTEGER REFERENCES emails(id),
    project_id      INTEGER REFERENCES projects(id),
    confidence      REAL,
    assigned_by     TEXT,
    PRIMARY KEY (email_id, project_id)
);

CREATE TABLE IF NOT EXISTS entities (
    id              INTEGER PRIMARY KEY,
    email_id        INTEGER REFERENCES emails(id),
    entity_type     TEXT NOT NULL,
    value           TEXT NOT NULL,
    context         TEXT,
    confidence      REAL
);

CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_value ON entities(value);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              INTEGER PRIMARY KEY,
    stage           TEXT NOT NULL,
    email_id        INTEGER REFERENCES emails(id),
    status          TEXT NOT NULL,
    model_used      TEXT,
    started_at      TEXT,
    completed_at    TEXT,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_status ON pipeline_runs(stage, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_email_stage ON pipeline_runs(email_id, stage);

CREATE TABLE IF NOT EXISTS co_email_stats (
    email_a         TEXT NOT NULL,
    email_b         TEXT NOT NULL,
    co_email_count  INTEGER DEFAULT 0,
    first_co_email  TEXT,
    last_co_email   TEXT,
    PRIMARY KEY (email_a, email_b)
);

CREATE INDEX IF NOT EXISTS idx_co_email_a ON co_email_stats(email_a);
CREATE INDEX IF NOT EXISTS idx_co_email_b ON co_email_stats(email_b);

CREATE TABLE IF NOT EXISTS contact_memories (
    email           TEXT PRIMARY KEY,
    name            TEXT,
    relationship    TEXT,
    summary         TEXT,
    discussions     TEXT,
    key_facts       TEXT,
    model_used      TEXT,
    strategy_used   TEXT,
    version         INTEGER DEFAULT 1,
    generated_at    TEXT,
    emails_hash     TEXT
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""


def get_db(config: Config) -> sqlite3.Connection:
    db_path = config.db_abs_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    _init_schema(conn)
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    # Track schema version
    row = conn.execute(
        "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
    ).fetchone()
    current_version = row[0] if row else 0
    if current_version == 0:
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        conn.commit()
    if current_version < 2:
        # Add gmail_id column if missing (migration v1 -> v2)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(emails)").fetchall()}
        if "gmail_id" not in cols:
            conn.execute("ALTER TABLE emails ADD COLUMN gmail_id TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_gmail_id ON emails(gmail_id)")
        conn.execute(
            "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        conn.commit()
    if current_version < 3:
        _migrate_to_v3(conn)


def _migrate_to_v3(conn: sqlite3.Connection) -> None:
    """Migration v2 -> v3: add normalised_subject, email_references table."""
    import json
    import re

    SUBJECT_PREFIX_RE = re.compile(
        r"^(\s*(Re|Fwd?|Fw)\s*(\[\d+\])?\s*:\s*)+", re.IGNORECASE
    )

    def _norm(subject: str | None) -> str:
        if not subject:
            return ""
        return SUBJECT_PREFIX_RE.sub("", subject).strip().lower()

    def _extract_ids(header_value: str) -> list[str]:
        if not header_value:
            return []
        return re.findall(r"<([^>]+)>", header_value)

    cols = {r[1] for r in conn.execute("PRAGMA table_info(emails)").fetchall()}
    if "normalised_subject" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN normalised_subject TEXT")

    conn.execute("""CREATE TABLE IF NOT EXISTS email_references (
        email_id        INTEGER NOT NULL REFERENCES emails(id),
        referenced_id   TEXT NOT NULL,
        PRIMARY KEY (email_id, referenced_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email_refs_referenced ON email_references(referenced_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_norm_subject ON emails(normalised_subject)")

    # Backfill normalised_subject in batches
    BATCH = 5000
    total = conn.execute("SELECT COUNT(*) FROM emails WHERE normalised_subject IS NULL").fetchone()[0]
    processed = 0
    while True:
        rows = conn.execute(
            "SELECT id, subject FROM emails WHERE normalised_subject IS NULL LIMIT ?",
            (BATCH,),
        ).fetchall()
        if not rows:
            break
        conn.executemany(
            "UPDATE emails SET normalised_subject = ? WHERE id = ?",
            [(_norm(r[1]), r[0]) for r in rows],
        )
        conn.commit()
        processed += len(rows)
        if total > 0:
            print(f"  [migration v3] normalised_subject: {processed}/{total}")

    # Backfill email_references
    total = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    existing_refs = conn.execute("SELECT COUNT(*) FROM email_references").fetchone()[0]
    if existing_refs == 0 and total > 0:
        processed = 0
        offset = 0
        while True:
            rows = conn.execute(
                "SELECT id, raw_headers FROM emails ORDER BY id LIMIT ? OFFSET ?",
                (BATCH, offset),
            ).fetchall()
            if not rows:
                break
            ref_rows = []
            for r in rows:
                headers = json.loads(r[1]) if r[1] else {}
                refs = _extract_ids(headers.get("references", ""))
                in_reply = _extract_ids(headers.get("in_reply_to", ""))
                seen = set()
                for ref_id in refs + in_reply:
                    if ref_id not in seen:
                        ref_rows.append((r[0], ref_id))
                        seen.add(ref_id)
            if ref_rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO email_references (email_id, referenced_id) VALUES (?, ?)",
                    ref_rows,
                )
            conn.commit()
            processed += len(rows)
            offset += BATCH
            print(f"  [migration v3] email_references: {processed}/{total}")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v3] complete")


def execute(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Cursor:
    return conn.execute(sql, params)


def fetchall(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[Any]:
    return conn.execute(sql, params).fetchall()


def fetchone(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any | None:
    return conn.execute(sql, params).fetchone()
