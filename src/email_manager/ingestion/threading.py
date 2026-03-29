from __future__ import annotations

import re
import sqlite3

from email_manager.db import fetchall


class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

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


def compute_threads(conn: sqlite3.Connection) -> int:
    rows = fetchall(
        conn,
        "SELECT id, message_id, raw_headers, subject, date FROM emails",
    )

    uf = UnionFind()
    updated = 0

    # Phase 1: Link via References and In-Reply-To headers
    for row in rows:
        import json

        headers = json.loads(row["raw_headers"]) if row["raw_headers"] else {}
        msg_id = row["message_id"]

        refs = extract_message_ids(headers.get("references", ""))
        in_reply_to = extract_message_ids(headers.get("in_reply_to", ""))

        all_related = refs + in_reply_to
        for related_id in all_related:
            uf.union(msg_id, related_id)

    # Phase 2: Fallback — group by normalised subject within 90-day windows
    subject_groups: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        norm_subj = normalise_subject(row["subject"])
        if not norm_subj:
            continue
        # Only use subject fallback for emails not already linked
        msg_id = row["message_id"]
        root = uf.find(msg_id)
        if root == msg_id:  # not linked to anything yet
            subject_groups.setdefault(norm_subj, []).append(
                (msg_id, row["date"])
            )

    for _subj, msgs in subject_groups.items():
        if len(msgs) < 2:
            continue
        msgs.sort(key=lambda x: x[1])
        for i in range(1, len(msgs)):
            # Only link if within 90 days of previous
            from datetime import datetime, timedelta

            try:
                d1 = datetime.fromisoformat(msgs[i - 1][1])
                d2 = datetime.fromisoformat(msgs[i][1])
                if abs((d2 - d1).days) <= 90:
                    uf.union(msgs[i - 1][0], msgs[i][0])
            except (ValueError, TypeError):
                uf.union(msgs[i - 1][0], msgs[i][0])

    # Phase 3: Assign thread_id to each email
    for row in rows:
        thread_id = uf.find(row["message_id"])
        conn.execute(
            "UPDATE emails SET thread_id = ? WHERE id = ?",
            (thread_id, row["id"]),
        )
        updated += 1

    # Phase 4: Update/create thread summary rows
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
