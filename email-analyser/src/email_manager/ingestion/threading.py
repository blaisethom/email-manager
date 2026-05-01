from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeElapsedColumn

BATCH_SIZE = 5000
SUBJECT_WINDOW_DAYS = 90


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


def _progress_columns():
    return [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_threads(
    conn: sqlite3.Connection,
    console: Console | None = None,
    force_rebuild: bool = False,
) -> int:
    """Compute and assign thread IDs, using incremental mode when possible.

    Args:
        conn: SQLite connection.
        console: Rich console for progress output.
        force_rebuild: If True, do a full UnionFind rebuild instead of incremental.

    Returns:
        Number of emails whose thread_id was updated.
    """
    console = console or Console()

    unthreaded = conn.execute(
        "SELECT COUNT(*) as cnt FROM emails WHERE thread_id IS NULL"
    ).fetchone()["cnt"]

    if unthreaded == 0 and not force_rebuild:
        console.print("[dim]All emails already threaded, nothing to do.[/dim]")
        return 0

    # Use full rebuild when forced, or when the email_references table hasn't
    # been populated yet (pre-v3 databases that just migrated).
    refs_count = conn.execute("SELECT COUNT(*) FROM email_references").fetchone()[0]
    total_emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]

    if force_rebuild:
        console.print("[bold]Full thread rebuild requested.[/bold]")
        return _full_rebuild(conn, console)

    if refs_count == 0 and total_emails > 0:
        console.print(
            f"[bold]First threading run — full rebuild over {total_emails:,} emails.[/bold]"
        )
        return _full_rebuild(conn, console)

    console.print(
        f"[bold]Incremental threading: {unthreaded:,} new email(s) to process.[/bold]"
    )
    return _incremental_thread(conn, console)


# ---------------------------------------------------------------------------
# Incremental threading — O(K) for K new emails
# ---------------------------------------------------------------------------

def _incremental_thread(conn: sqlite3.Connection, console: Console) -> int:
    """Thread only emails with thread_id IS NULL using indexed lookups."""

    unthreaded_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM emails WHERE thread_id IS NULL"
    ).fetchone()["cnt"]

    if unthreaded_count == 0:
        return 0

    with Progress(*_progress_columns(), console=console, transient=True) as progress:
        task = progress.add_task("Threading new emails", total=unthreaded_count)
        updated = 0
        dirty_threads: set[str] = set()

        # Process unthreaded emails oldest-first in batches
        while True:
            rows = conn.execute(
                """SELECT id, message_id, normalised_subject, date
                   FROM emails
                   WHERE thread_id IS NULL
                   ORDER BY date ASC
                   LIMIT ?""",
                (BATCH_SIZE,),
            ).fetchall()

            if not rows:
                break

            for row in rows:
                email_id = row["id"]
                message_id = row["message_id"]
                norm_subj = row["normalised_subject"] or ""
                email_date = row["date"]

                thread_id = _find_thread_for_email(
                    conn, email_id, message_id, norm_subj, email_date, dirty_threads
                )

                conn.execute(
                    "UPDATE emails SET thread_id = ? WHERE id = ?",
                    (thread_id, email_id),
                )
                dirty_threads.add(thread_id)
                updated += 1
                progress.update(task, advance=1)

            conn.commit()
            console.print(
                f"  [dim]Committed batch — {updated:,}/{unthreaded_count:,} threaded[/dim]"
            )

        progress.remove_task(task)

        # Update only the thread summaries that changed
        if dirty_threads:
            task = progress.add_task(
                "Updating thread summaries", total=len(dirty_threads)
            )
            _update_dirty_threads(conn, dirty_threads, progress, task)
            conn.commit()
            console.print(
                f"  [dim]Updated {len(dirty_threads):,} thread summaries[/dim]"
            )

    return updated


def _find_thread_for_email(
    conn: sqlite3.Connection,
    email_id: int,
    message_id: str,
    norm_subj: str,
    email_date: str,
    dirty_threads: set[str],
) -> str:
    """Determine the thread_id for a single unthreaded email.

    Returns the thread_id to assign (may trigger merges as a side effect).
    """
    found_thread_ids: set[str] = set()

    # Forward lookup: emails that THIS email references
    rows = conn.execute(
        """SELECT DISTINCT e.thread_id
           FROM email_references er
           JOIN emails e ON e.message_id = er.referenced_id
           WHERE er.email_id = ? AND e.thread_id IS NOT NULL""",
        (str(email_id),),
    ).fetchall()
    for r in rows:
        found_thread_ids.add(r["thread_id"])

    # Reverse lookup: emails that reference THIS email
    rows = conn.execute(
        """SELECT DISTINCT e.thread_id
           FROM email_references er
           JOIN emails e ON er.email_id = CAST(e.id AS TEXT)
           WHERE er.referenced_id = ? AND e.thread_id IS NOT NULL""",
        (message_id,),
    ).fetchall()
    for r in rows:
        found_thread_ids.add(r["thread_id"])

    if len(found_thread_ids) == 1:
        return found_thread_ids.pop()

    if len(found_thread_ids) > 1:
        # Merge: pick the thread with the most emails as winner
        winner = _merge_threads(conn, found_thread_ids, dirty_threads)
        return winner

    # Fallback: subject matching within time window, requiring participant overlap
    if norm_subj:
        # Collect participants of this email
        addr_row = conn.execute(
            "SELECT from_address, to_addresses, cc_addresses FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()
        my_addrs: set[str] = set()
        if addr_row:
            if addr_row["from_address"]:
                my_addrs.add(addr_row["from_address"].lower())
            for field in ("to_addresses", "cc_addresses"):
                val = addr_row[field]
                if val:
                    try:
                        for a in json.loads(val):
                            if a:
                                my_addrs.add(a.lower())
                    except (json.JSONDecodeError, TypeError):
                        pass

        # Find candidate threads with matching subject within time window
        candidates = conn.execute(
            """SELECT thread_id, from_address, to_addresses, cc_addresses FROM emails
               WHERE normalised_subject = ?
                 AND thread_id IS NOT NULL
                 AND ABS(julianday(?) - julianday(date)) <= ?
               ORDER BY date DESC
               LIMIT 20""",
            (norm_subj, email_date, SUBJECT_WINDOW_DAYS),
        ).fetchall()

        for cand in candidates:
            cand_addrs: set[str] = set()
            if cand["from_address"]:
                cand_addrs.add(cand["from_address"].lower())
            for field in ("to_addresses", "cc_addresses"):
                val = cand[field]
                if val:
                    try:
                        for a in json.loads(val):
                            if a:
                                cand_addrs.add(a.lower())
                    except (json.JSONDecodeError, TypeError):
                        pass
            # Require at least one participant in common
            if my_addrs & cand_addrs:
                return cand["thread_id"]

    # No match — start a new thread
    return message_id


def _merge_threads(
    conn: sqlite3.Connection,
    thread_ids: set[str],
    dirty_threads: set[str],
) -> str:
    """Merge multiple threads into one. Returns the winning thread_id."""
    # Pick winner: thread with the most emails
    counts = []
    for tid in thread_ids:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE thread_id = ?", (tid,)
        ).fetchone()
        counts.append((row["cnt"], tid))
    counts.sort(reverse=True)
    winner = counts[0][1]
    losers = [tid for _, tid in counts[1:]]

    for loser in losers:
        conn.execute(
            "UPDATE emails SET thread_id = ? WHERE thread_id = ?",
            (winner, loser),
        )
        # Update all tables that reference thread_id
        conn.execute(
            "UPDATE event_ledger SET thread_id = ? WHERE thread_id = ?",
            (winner, loser),
        )
        # discussion_threads has a composite PK — avoid duplicates
        conn.execute(
            "DELETE FROM discussion_threads WHERE thread_id = ? AND discussion_id IN "
            "(SELECT discussion_id FROM discussion_threads WHERE thread_id = ?)",
            (loser, winner),
        )
        conn.execute(
            "UPDATE discussion_threads SET thread_id = ? WHERE thread_id = ?",
            (winner, loser),
        )
        conn.execute(
            "DELETE FROM thread_search_docs WHERE thread_id = ?", (loser,)
        )
        conn.execute(
            "DELETE FROM thread_embeddings WHERE thread_id = ?", (loser,)
        )
        conn.execute("DELETE FROM threads WHERE thread_id = ?", (loser,))
        dirty_threads.discard(loser)

    dirty_threads.add(winner)
    return winner


# ---------------------------------------------------------------------------
# Full rebuild — for first run or --rebuild-threads
# ---------------------------------------------------------------------------

def _full_rebuild(conn: sqlite3.Connection, console: Console) -> int:
    """Full UnionFind rebuild using email_references table (no JSON parsing)."""

    total = conn.execute("SELECT COUNT(*) as cnt FROM emails").fetchone()["cnt"]

    with Progress(*_progress_columns(), console=console, transient=True) as progress:
        uf = _build_union_find(conn, progress, total)

        # Clear all thread assignments and rebuild
        conn.execute("UPDATE emails SET thread_id = NULL")
        conn.execute("DELETE FROM threads")
        conn.commit()
        console.print("  [dim]Cleared existing thread assignments[/dim]")

        # Assign thread_id from UnionFind in batches
        task = progress.add_task("Writing thread IDs", total=total)
        updated = 0
        offset = 0

        while True:
            rows = conn.execute(
                "SELECT id, message_id FROM emails ORDER BY id LIMIT ? OFFSET ?",
                (BATCH_SIZE, offset),
            ).fetchall()
            if not rows:
                break

            updates = [(uf.find(r["message_id"]), r["id"]) for r in rows]
            conn.executemany(
                "UPDATE emails SET thread_id = ? WHERE id = ?", updates
            )
            conn.commit()
            updated += len(rows)
            offset += BATCH_SIZE
            progress.update(task, advance=len(rows))
            console.print(
                f"  [dim]Assigned thread IDs: {updated:,}/{total:,}[/dim]"
            )

        progress.remove_task(task)

        # Rebuild all thread summaries
        task = progress.add_task("Rebuilding thread summaries", total=None)
        _update_thread_table(conn)
        conn.commit()
        console.print("  [dim]Thread summaries rebuilt[/dim]")

    return updated


def _build_union_find(
    conn: sqlite3.Connection, progress: Progress, total: int
) -> UnionFind:
    """Build the full UnionFind using pre-extracted email_references."""
    uf = UnionFind()

    # Phase 1: Link via email_references table (no JSON parsing needed)
    ref_count = conn.execute("SELECT COUNT(*) FROM email_references").fetchone()[0]
    task = progress.add_task("Linking by references", total=ref_count)

    offset = 0
    while True:
        rows = conn.execute(
            """SELECT e.message_id, er.referenced_id
               FROM email_references er
               JOIN emails e ON er.email_id = CAST(e.id AS TEXT)
               ORDER BY er.email_id
               LIMIT ? OFFSET ?""",
            (BATCH_SIZE, offset),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            uf.union(row["message_id"], row["referenced_id"])
        progress.update(task, advance=len(rows))
        offset += BATCH_SIZE

    progress.remove_task(task)
    progress.console.print(
        f"  [dim]Phase 1 complete: linked {ref_count:,} references[/dim]"
    )

    # Phase 2: Subject-based fallback grouping (with participant overlap check)
    task = progress.add_task("Grouping by subject", total=total)

    # Collect unlinked emails with their subject, date, and participants
    subject_groups: dict[str, list[tuple[str, str, set[str]]]] = {}
    offset = 0
    while True:
        rows = conn.execute(
            """SELECT message_id, normalised_subject, date,
                      from_address, to_addresses, cc_addresses
               FROM emails ORDER BY id LIMIT ? OFFSET ?""",
            (BATCH_SIZE, offset),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            norm_subj = row["normalised_subject"]
            if not norm_subj:
                continue
            msg_id = row["message_id"]
            root = uf.find(msg_id)
            if root == msg_id:  # not linked to anything yet
                addrs: set[str] = set()
                if row["from_address"]:
                    addrs.add(row["from_address"].lower())
                for field in ("to_addresses", "cc_addresses"):
                    val = row[field]
                    if val:
                        try:
                            for a in json.loads(val):
                                if a:
                                    addrs.add(a.lower())
                        except (json.JSONDecodeError, TypeError):
                            pass
                subject_groups.setdefault(norm_subj, []).append(
                    (msg_id, row["date"], addrs)
                )
        progress.update(task, advance=len(rows))
        offset += BATCH_SIZE

    # Link within subject groups using time window AND participant overlap
    for _subj, msgs in subject_groups.items():
        if len(msgs) < 2:
            continue
        msgs.sort(key=lambda x: x[1])
        for i in range(1, len(msgs)):
            try:
                d1 = datetime.fromisoformat(msgs[i - 1][1])
                d2 = datetime.fromisoformat(msgs[i][1])
                if abs((d2 - d1).days) <= SUBJECT_WINDOW_DAYS and (msgs[i - 1][2] & msgs[i][2]):
                    uf.union(msgs[i - 1][0], msgs[i][0])
            except (ValueError, TypeError):
                # On date parse failure, still require participant overlap
                if msgs[i - 1][2] & msgs[i][2]:
                    uf.union(msgs[i - 1][0], msgs[i][0])

    progress.remove_task(task)
    progress.console.print(
        f"  [dim]Phase 2 complete: grouped {len(subject_groups):,} unique subjects[/dim]"
    )

    return uf


# ---------------------------------------------------------------------------
# Thread summary helpers
# ---------------------------------------------------------------------------

def _update_dirty_threads(
    conn: sqlite3.Connection,
    dirty_threads: set[str],
    progress: Progress | None = None,
    task=None,
) -> None:
    """Update thread summary rows only for the given thread_ids."""
    batch = list(dirty_threads)
    for i in range(0, len(batch), BATCH_SIZE):
        chunk = batch[i : i + BATCH_SIZE]
        placeholders = ",".join("?" * len(chunk))
        conn.execute(
            f"""INSERT OR REPLACE INTO threads
                (thread_id, subject, email_count, first_date, last_date, participants)
                SELECT
                    thread_id,
                    MIN(subject),
                    COUNT(*),
                    MIN(date),
                    MAX(date),
                    json_group_array(DISTINCT from_address)
                FROM emails
                WHERE thread_id IN ({placeholders})
                GROUP BY thread_id""",
            chunk,
        )
        if progress and task is not None:
            progress.update(task, advance=len(chunk))


def _update_thread_table(conn: sqlite3.Connection) -> None:
    """Rebuild all thread summary rows (used by full rebuild)."""
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


# ---------------------------------------------------------------------------
# Ingestion helper — populate email_references at insert time
# ---------------------------------------------------------------------------

def insert_email_references(
    conn: sqlite3.Connection, email_id: int, raw_headers: dict
) -> None:
    """Extract References/In-Reply-To from raw_headers and insert into email_references."""
    refs = extract_message_ids(raw_headers.get("references", ""))
    in_reply = extract_message_ids(raw_headers.get("in_reply_to", ""))
    seen: set[str] = set()
    rows = []
    for ref_id in refs + in_reply:
        if ref_id not in seen:
            rows.append((email_id, ref_id))
            seen.add(ref_id)
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO email_references (email_id, referenced_id) VALUES (?, ?)",
            rows,
        )
