from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeElapsedColumn

BATCH_SIZE = 5000


class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        # Iterative path compression to avoid RecursionError on deep chains
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1


SUBJECT_PREFIX_RE = re.compile(
    r"^(\s*(Re|Fwd?|Fw)\s*(\[\d+\])?\s*:\s*)+", re.IGNORECASE
)


def normalise_subject(subject: str | None) -> str:
    if not subject:
        return ""
    return SUBJECT_PREFIX_RE.sub("", subject).strip().lower()


def extract_message_ids(header_value: str) -> list[str]:
    if not header_value:
        return []
    return re.findall(r"<([^>]+)>", header_value)


def _get_total_emails(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) as cnt FROM emails").fetchone()
    return row["cnt"] if row else 0


def _build_union_find(conn: sqlite3.Connection, progress: Progress | None = None) -> UnionFind:
    """Phases 1-2: Build the full UnionFind from headers and subject fallback."""
    uf = UnionFind()
    total = _get_total_emails(conn)

    # Phase 1: Link via References and In-Reply-To headers
    task = progress.add_task("Linking by headers", total=total) if progress else None
    cursor = conn.execute("SELECT message_id, raw_headers FROM emails")
    while True:
        batch = cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break
        for row in batch:
            headers = json.loads(row["raw_headers"]) if row["raw_headers"] else {}
            msg_id = row["message_id"]
            refs = extract_message_ids(headers.get("references", ""))
            in_reply_to = extract_message_ids(headers.get("in_reply_to", ""))
            for related_id in refs + in_reply_to:
                uf.union(msg_id, related_id)
        if progress and task is not None:
            progress.update(task, advance=len(batch))

    # Phase 2: Fallback — group by normalised subject within 90-day windows
    if progress and task is not None:
        progress.remove_task(task)
    task = progress.add_task("Grouping by subject", total=total) if progress else None

    subject_groups: dict[str, list[tuple[str, str]]] = {}
    cursor = conn.execute("SELECT message_id, subject, date FROM emails")
    while True:
        batch = cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break
        for row in batch:
            norm_subj = normalise_subject(row["subject"])
            if not norm_subj:
                continue
            msg_id = row["message_id"]
            root = uf.find(msg_id)
            if root == msg_id:  # not linked to anything yet
                subject_groups.setdefault(norm_subj, []).append(
                    (msg_id, row["date"])
                )
        if progress and task is not None:
            progress.update(task, advance=len(batch))

    for _subj, msgs in subject_groups.items():
        if len(msgs) < 2:
            continue
        msgs.sort(key=lambda x: x[1])
        for i in range(1, len(msgs)):
            try:
                d1 = datetime.fromisoformat(msgs[i - 1][1])
                d2 = datetime.fromisoformat(msgs[i][1])
                if abs((d2 - d1).days) <= 90:
                    uf.union(msgs[i - 1][0], msgs[i][0])
            except (ValueError, TypeError):
                uf.union(msgs[i - 1][0], msgs[i][0])

    if progress and task is not None:
        progress.remove_task(task)

    return uf


def compute_threads(conn: sqlite3.Connection, console: Console | None = None) -> int:
    """Compute and assign thread IDs, resumable across crashes.

    Each batch of UPDATEs is committed independently so progress survives
    an interruption.  On the next run the expensive UnionFind is rebuilt
    (it is pure computation, not I/O-bound) and only rows still missing a
    thread_id are written.
    """
    progress_columns = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ]
    with Progress(*progress_columns, console=console or Console(), transient=True) as progress:
        uf = _build_union_find(conn, progress)

        # Phase 3: Assign thread_id — only to emails that don't have one yet.
        # Committed per-batch so a crash preserves earlier progress.
        remaining = conn.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE thread_id IS NULL"
        ).fetchone()["cnt"]
        task = progress.add_task("Writing thread IDs", total=remaining)

        updated = 0
        cursor = conn.execute(
            "SELECT id, message_id FROM emails WHERE thread_id IS NULL"
        )
        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break
            updates = [(uf.find(row["message_id"]), row["id"]) for row in batch]
            conn.executemany(
                "UPDATE emails SET thread_id = ? WHERE id = ?", updates
            )
            conn.commit()
            updated += len(updates)
            progress.update(task, advance=len(updates))

        progress.remove_task(task)

        # Phase 4: Update/create thread summary rows
        task = progress.add_task("Updating thread summaries", total=None)
        _update_thread_table(conn)
        conn.commit()

    return updated


def _update_thread_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO threads (thread_id, subject, email_count, first_date, last_date, participants)
        SELECT
            thread_id,
            MIN(subject),
            COUNT(*),
            MIN(date),
            MAX(date),
            json_group_array(DISTINCT from_address)
        FROM emails
        WHERE thread_id IS NOT NULL
        GROUP BY thread_id
    """)
