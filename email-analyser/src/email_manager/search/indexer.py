"""Build search index: construct search documents, tsvector, and embeddings."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Callable

from rich.console import Console

from email_manager.db import fetchall, fetchone

logger = logging.getLogger("email_manager.search.indexer")

MAX_DOC_CHARS = 8000
MAX_EMAILS_PER_THREAD = 20


def _build_doc_text(
    thread: dict,
    emails: list[dict],
    company_name: str | None,
) -> str:
    """Construct a search document from thread metadata and email bodies.

    Discussion title/summary are intentionally NOT included here — discussions
    are indexed separately so that evolving discussion text doesn't invalidate
    thread embeddings.
    """
    parts: list[str] = []

    # Header
    subject = thread.get("subject") or "(no subject)"
    parts.append(f"Subject: {subject}")

    participants = thread.get("participants") or ""
    if participants:
        parts.append(f"Participants: {participants}")

    if company_name:
        parts.append(f"Company: {company_name}")

    first_date = (thread.get("first_date") or "")[:10]
    last_date = (thread.get("last_date") or "")[:10]
    if first_date:
        parts.append(f"Date: {first_date} to {last_date}")

    parts.append("---")

    # Email bodies (newest first, capped)
    char_budget = MAX_DOC_CHARS
    for email in emails[:MAX_EMAILS_PER_THREAD]:
        body = (email.get("body_text") or "").strip()
        if not body:
            continue
        # Skip very short auto-replies
        if len(body) < 20:
            continue
        # Truncate individual emails
        if len(body) > 2000:
            body = body[:2000] + "..."

        if char_budget <= 0:
            break
        chunk = body[:char_budget]
        parts.append(chunk)
        char_budget -= len(chunk)

    return "\n".join(parts)


def _compute_hash(emails: list[dict]) -> str:
    """Hash email IDs + dates to detect changes."""
    h = hashlib.md5()
    for e in emails:
        h.update(f"{e.get('message_id', '')}:{e.get('date', '')}".encode())
    return h.hexdigest()


def build_search_index(
    conn: sqlite3.Connection,
    console: Console | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    force: bool = False,
) -> int:
    """Build or update the search index for all threads.

    Returns the number of documents created/updated.
    """
    console = console or Console()
    now = datetime.now(timezone.utc).isoformat()
    is_postgres = type(conn).__name__ == "PostgresConnection"

    # Get all threads
    threads = fetchall(
        conn,
        """SELECT t.thread_id, t.subject, t.email_count, t.first_date, t.last_date,
                  t.participants, t.summary
           FROM threads t
           WHERE t.email_count > 0
           ORDER BY t.last_date DESC""",
    )
    total = len(threads)
    console.print(f"  [dim]Found {total} threads to index[/dim]")

    # Get existing doc hashes for incremental updates
    existing: dict[str, str | None] = {}
    if not force:
        rows = fetchall(conn, "SELECT thread_id, doc_hash FROM thread_search_docs")
        existing = {r["thread_id"]: r["doc_hash"] for r in rows}

    # Get company domains for threads (via email addresses)
    # Build a lookup: thread_id -> (company_domain, company_name, is_important)
    company_lookup: dict[str, tuple[str | None, str | None, bool]] = {}

    # Get labelled company domains (important)
    important_domains: set[str] = set()
    label_rows = fetchall(
        conn,
        "SELECT DISTINCT c.domain FROM companies c JOIN company_labels cl ON cl.company_id = c.id",
    )
    for r in label_rows:
        if r["domain"]:
            important_domains.add(r["domain"].lower())

    updated = 0
    batch: list[tuple] = []

    for i, thread in enumerate(threads):
        tid = thread["thread_id"]

        # Fetch emails for this thread
        emails = fetchall(
            conn,
            """SELECT message_id, from_address, to_addresses, date, body_text
               FROM emails WHERE thread_id = ? ORDER BY date DESC""",
            (tid,),
        )

        # Check if we need to update
        doc_hash = _compute_hash(emails)
        if not force and tid in existing and existing[tid] == doc_hash:
            continue

        # Determine company domain from email addresses
        company_domain = None
        company_name = None
        is_important = False
        for email in emails:
            addr = email.get("from_address") or ""
            if "@" in addr:
                domain = addr.split("@")[1].lower()
                # Look up company
                comp = fetchone(
                    conn,
                    "SELECT domain, name FROM companies WHERE LOWER(domain) = ?",
                    (domain,),
                )
                if comp and comp["domain"]:
                    company_domain = comp["domain"]
                    company_name = comp["name"]
                    is_important = company_domain.lower() in important_domains
                    break

        # Build document
        doc_text = _build_doc_text(thread, emails, company_name)

        batch.append((tid, doc_text, company_domain, is_important, doc_hash, now, now))
        updated += 1

        if on_progress and (updated % 100 == 0 or i == total - 1):
            on_progress(i + 1, total)

        # Batch insert every 500
        if len(batch) >= 500:
            _flush_batch(conn, batch, is_postgres)
            batch.clear()

    # Flush remaining
    if batch:
        _flush_batch(conn, batch, is_postgres)

    if updated > 0:
        console.print(f"  [green]build_search_index: indexed {updated} threads[/green]")
    else:
        console.print(f"  [dim]build_search_index: all {total} threads up to date[/dim]")

    # Refresh outreach scores across all threads (always — new sent emails change scores
    # regardless of whether the doc_text changed).
    _refresh_outreach_scores(conn, console, is_postgres)

    return updated


def _flush_batch(
    conn: sqlite3.Connection,
    batch: list[tuple],
    is_postgres: bool,
) -> None:
    """Insert/update a batch of search documents."""
    for row in batch:
        tid, doc_text, company_domain, is_important, doc_hash, created, updated = row

        if is_postgres:
            # PostgreSQL: use BOOLEAN and tsvector
            # Inline boolean literal to avoid psycopg2/wrapper integer conversion
            imp = 'true' if is_important else 'false'
            conn.execute(
                "INSERT INTO thread_search_docs"
                " (thread_id, doc_text, doc_tsv, doc_tsv_simple, company_domain, is_important, doc_hash, created_at, updated_at)"
                f" VALUES (?, ?, to_tsvector('english', ?), to_tsvector('simple', ?), ?, {imp}, ?, ?, ?)"
                " ON CONFLICT (thread_id) DO UPDATE SET"
                " doc_text = EXCLUDED.doc_text,"
                " doc_tsv = to_tsvector('english', EXCLUDED.doc_text),"
                " doc_tsv_simple = to_tsvector('simple', EXCLUDED.doc_text),"
                " company_domain = EXCLUDED.company_domain,"
                " is_important = EXCLUDED.is_important,"
                " doc_hash = EXCLUDED.doc_hash,"
                " updated_at = EXCLUDED.updated_at",
                (tid, doc_text, doc_text, doc_text, company_domain, doc_hash, created, updated),
            )
        else:
            # SQLite: no tsvector, store doc_text for LIKE search
            conn.execute(
                """INSERT OR REPLACE INTO thread_search_docs
                   (thread_id, doc_text, company_domain, is_important, doc_hash, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (tid, doc_text, company_domain, int(is_important), doc_hash, created, updated),
            )

    conn.commit()


# ── Outreach scoring ──────────────────────────────────────────────────────

SENT_FOLDERS = ("SENT", "Sent", "Sent Items", "Sent Mail", "[Gmail]/Sent Mail")
ME_MIN_SENT = 50


def _refresh_outreach_scores(
    conn: sqlite3.Connection,
    console: Console,
    is_postgres: bool,
) -> None:
    """Populate thread_search_docs.outreach_score for all threads.

    outreach_score = sum over thread participants of log(1 + count_of_emails_I_sent_to_them).
    Boosts threads where I've actively emailed the other party, so newsletters and
    blast-lists sink in ranking.
    """
    console.print("  [dim]Computing outreach scores...[/dim]")

    folder_placeholders = ",".join("?" * len(SENT_FOLDERS))
    me_rows = fetchall(
        conn,
        f"""SELECT LOWER(from_address) AS addr, COUNT(*) AS cnt FROM emails
            WHERE folder IN ({folder_placeholders}) AND from_address IS NOT NULL
            GROUP BY LOWER(from_address)
            HAVING COUNT(*) > ?""",
        (*SENT_FOLDERS, ME_MIN_SENT),
    )
    me_addresses = {r["addr"] for r in me_rows if r["addr"]}

    if not me_addresses:
        console.print("  [dim]  no 'me' addresses (no sent folder with > 50 emails) — skipping outreach[/dim]")
        return

    console.print(f"  [dim]  'me' addresses: {sorted(me_addresses)}[/dim]")

    # Per-recipient outreach counts from emails I sent.
    me_placeholders = ",".join("?" * len(me_addresses))
    sent_rows = fetchall(
        conn,
        f"""SELECT to_addresses, cc_addresses FROM emails
            WHERE LOWER(from_address) IN ({me_placeholders})""",
        tuple(me_addresses),
    )

    outreach_count: dict[str, int] = {}
    for row in sent_rows:
        for field in ("to_addresses", "cc_addresses"):
            raw = row[field]
            if not raw:
                continue
            try:
                addresses = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            for a in addresses:
                a = (a or "").strip().lower()
                if a and "@" in a and a not in me_addresses:
                    outreach_count[a] = outreach_count.get(a, 0) + 1

    console.print(f"  [dim]  unique recipients I've emailed: {len(outreach_count)}[/dim]")

    # Aggregate per-thread.
    thread_rows = fetchall(conn, "SELECT thread_id, participants FROM threads")

    updates: list[tuple[float, str]] = []
    for t in thread_rows:
        raw = t["participants"] or "[]"
        try:
            participants = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            participants = []
        score = 0.0
        for p in participants:
            p = (p or "").strip().lower()
            if not p or p in me_addresses:
                continue
            c = outreach_count.get(p, 0)
            if c:
                score += math.log1p(c)
        updates.append((score, t["thread_id"]))

    # Batch UPDATE against thread_search_docs (rows missing will simply be no-ops).
    nonzero = 0
    BATCH = 1000
    for i in range(0, len(updates), BATCH):
        chunk = updates[i : i + BATCH]
        for score, tid in chunk:
            conn.execute(
                "UPDATE thread_search_docs SET outreach_score = ? WHERE thread_id = ?",
                (score, tid),
            )
            if score > 0:
                nonzero += 1
        conn.commit()

    console.print(
        f"  [green]  outreach scores: {nonzero}/{len(updates)} threads boosted[/green]"
    )


# ── Embedding generation ──────────────────────────────────────────────────


def generate_embeddings(
    conn: sqlite3.Connection,
    config: object,
    console: Console | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    force: bool = False,
) -> int:
    """Generate embeddings for threads in thread_search_docs.

    Uses the fast model for all threads. If a thread is marked is_important,
    also generates embeddings with the quality model.

    Returns the number of embeddings generated.
    """
    from email_manager.search.embeddings import get_embedding_backend

    console = console or Console()
    is_postgres = type(conn).__name__ == "PostgresConnection"
    now = datetime.now(timezone.utc).isoformat()

    # Get config values
    backend_name = getattr(config, "embedding_backend", "voyage")
    api_key = getattr(config, "voyage_api_key", "")
    ollama_url = getattr(config, "ollama_url", "http://localhost:11434")
    fast_model = getattr(config, "embedding_model_fast", "voyage-3-lite")
    quality_model = getattr(config, "embedding_model_quality", "voyage-3")

    fast_backend = get_embedding_backend(backend_name, fast_model, api_key, ollama_url)
    console.print(f"  [dim]Embedding backend: {backend_name}, fast model: {fast_model} ({fast_backend.dims}d)[/dim]")

    # Find threads needing fast embeddings.
    # A thread needs re-embedding when no row exists for (thread_id, model_name) OR the
    # stored doc_hash doesn't match thread_search_docs.doc_hash (i.e. content changed).
    if force:
        threads_needing_fast = fetchall(
            conn,
            "SELECT thread_id, doc_text, doc_hash FROM thread_search_docs ORDER BY updated_at DESC",
        )
    else:
        threads_needing_fast = fetchall(
            conn,
            """SELECT tsd.thread_id, tsd.doc_text, tsd.doc_hash
               FROM thread_search_docs tsd
               WHERE NOT EXISTS (
                   SELECT 1 FROM thread_embeddings te
                   WHERE te.thread_id = tsd.thread_id
                     AND te.model_name = ?
                     AND COALESCE(te.doc_hash, '') = COALESCE(tsd.doc_hash, '')
               )
               ORDER BY tsd.updated_at DESC""",
            (fast_model,),
        )

    total_fast = len(threads_needing_fast)
    console.print(f"  [dim]{total_fast} threads need fast embeddings[/dim]")

    generated = 0

    if total_fast > 0:
        generated += _embed_and_store(
            conn, fast_backend, threads_needing_fast, is_postgres, now,
            console, on_progress, "fast",
        )

    # Quality embeddings for important threads only
    if force:
        threads_needing_quality = fetchall(
            conn,
            "SELECT thread_id, doc_text, doc_hash FROM thread_search_docs WHERE is_important = true ORDER BY updated_at DESC",
        )
    else:
        threads_needing_quality = fetchall(
            conn,
            """SELECT tsd.thread_id, tsd.doc_text, tsd.doc_hash
               FROM thread_search_docs tsd
               WHERE tsd.is_important = true
                 AND NOT EXISTS (
                     SELECT 1 FROM thread_embeddings te
                     WHERE te.thread_id = tsd.thread_id
                       AND te.model_name = ?
                       AND COALESCE(te.doc_hash, '') = COALESCE(tsd.doc_hash, '')
                 )
               ORDER BY tsd.updated_at DESC""",
            (quality_model,),
        )

    total_quality = len(threads_needing_quality)
    if total_quality > 0:
        console.print(f"  [dim]{total_quality} important threads need quality embeddings ({quality_model})[/dim]")
        quality_backend = get_embedding_backend(backend_name, quality_model, api_key, ollama_url)
        generated += _embed_and_store(
            conn, quality_backend, threads_needing_quality, is_postgres, now,
            console, None, "quality",
        )

    if generated > 0:
        console.print(f"  [green]generate_embeddings: created {generated} embeddings[/green]")
    else:
        console.print(f"  [dim]generate_embeddings: all embeddings up to date[/dim]")

    return generated


def _embed_and_store(
    conn: sqlite3.Connection,
    backend: object,
    threads: list[dict],
    is_postgres: bool,
    now: str,
    console: Console,
    on_progress: Callable[[int, int], None] | None,
    label: str,
) -> int:
    """Embed threads in batches and store in thread_embeddings."""
    BATCH_SIZE = 32
    total = len(threads)
    stored = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = threads[batch_start : batch_start + BATCH_SIZE]
        texts = [t["doc_text"][:2000] for t in batch]  # cap for embedding input

        try:
            embeddings = backend.embed_batch(texts)  # type: ignore[union-attr]
        except Exception as e:
            console.print(f"  [red]Embedding batch failed at {batch_start}: {e}[/red]")
            logger.exception("Embedding batch failed at %d", batch_start)
            continue

        for thread, embedding in zip(batch, embeddings):
            tid = thread["thread_id"]
            model_name = backend.model_name  # type: ignore[union-attr]
            doc_hash = thread.get("doc_hash")

            if is_postgres:
                vec_str = "[" + ",".join(str(v) for v in embedding) + "]"
                conn.execute(
                    "INSERT INTO thread_embeddings (thread_id, model_name, embedding, doc_hash, created_at)"
                    " VALUES (?, ?, ?::vector, ?, ?)"
                    " ON CONFLICT (thread_id, model_name) DO UPDATE SET"
                    " embedding = EXCLUDED.embedding,"
                    " doc_hash = EXCLUDED.doc_hash,"
                    " created_at = EXCLUDED.created_at",
                    (tid, model_name, vec_str, doc_hash, now),
                )
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO thread_embeddings (thread_id, model_name, doc_hash, created_at)"
                    " VALUES (?, ?, ?, ?)",
                    (tid, model_name, doc_hash, now),
                )

            stored += 1

        conn.commit()

        done = min(batch_start + BATCH_SIZE, total)
        if on_progress:
            on_progress(done, total)
        logger.info("Embedded %d/%d threads (%s, %s)", done, total, label, backend.model_name)  # type: ignore[union-attr]

    return stored


# ── Discussion search index ───────────────────────────────────────────────

MAX_DISCUSSION_DOC_CHARS = 4000


def _build_discussion_doc_text(disc: dict, company_name: str | None) -> str:
    """Doc text for a discussion — the evolving narrative, distinct from thread bodies."""
    parts: list[str] = []
    title = disc.get("title") or "(untitled)"
    parts.append(f"Title: {title}")
    if company_name:
        parts.append(f"Company: {company_name}")
    if disc.get("category"):
        parts.append(f"Category: {disc['category']}")
    if disc.get("current_state"):
        parts.append(f"State: {disc['current_state']}")

    participants_raw = disc.get("participants") or ""
    if participants_raw:
        try:
            plist = json.loads(participants_raw)
            if isinstance(plist, list) and plist:
                parts.append(f"Participants: {', '.join(plist)}")
        except (json.JSONDecodeError, TypeError):
            pass

    first_seen = (disc.get("first_seen") or "")[:10]
    last_seen = (disc.get("last_seen") or "")[:10]
    if first_seen:
        parts.append(f"Dates: {first_seen} to {last_seen}")

    parts.append("---")

    summary = (disc.get("summary") or "").strip()
    if summary:
        parts.append(summary[:MAX_DISCUSSION_DOC_CHARS])

    return "\n".join(parts)


def _compute_discussion_hash(disc: dict) -> str:
    """Hash discussion fields that feed into doc_text — recomputed when content changes."""
    h = hashlib.md5()
    for k in ("title", "summary", "category", "current_state", "participants", "first_seen", "last_seen"):
        h.update(f"{k}:{disc.get(k) or ''}|".encode())
    return h.hexdigest()


def build_discussion_search_index(
    conn: sqlite3.Connection,
    console: Console | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    force: bool = False,
) -> int:
    """Build or update the search index for all discussions."""
    console = console or Console()
    now = datetime.now(timezone.utc).isoformat()
    is_postgres = type(conn).__name__ == "PostgresConnection"

    discussions = fetchall(
        conn,
        """SELECT d.id, d.title, d.summary, d.category, d.current_state,
                  d.participants, d.first_seen, d.last_seen, d.company_id,
                  c.domain AS company_domain, c.name AS company_name
           FROM discussions d
           LEFT JOIN companies c ON c.id = d.company_id
           WHERE d.parent_id IS NULL
           ORDER BY d.last_seen DESC""",
    )
    total = len(discussions)
    console.print(f"  [dim]Found {total} discussions to index[/dim]")

    existing: dict[int, str | None] = {}
    if not force:
        rows = fetchall(conn, "SELECT discussion_id, doc_hash FROM discussion_search_docs")
        existing = {r["discussion_id"]: r["doc_hash"] for r in rows}

    updated = 0
    batch: list[tuple] = []

    for i, disc in enumerate(discussions):
        doc_hash = _compute_discussion_hash(disc)
        if not force and existing.get(disc["id"]) == doc_hash:
            continue

        doc_text = _build_discussion_doc_text(disc, disc.get("company_name"))
        batch.append((
            disc["id"], doc_text, doc_hash, disc.get("company_domain"),
            disc.get("category"), disc.get("current_state"), now, now,
        ))
        updated += 1

        if on_progress and (updated % 100 == 0 or i == total - 1):
            on_progress(i + 1, total)

        if len(batch) >= 500:
            _flush_discussion_batch(conn, batch, is_postgres)
            batch.clear()

    if batch:
        _flush_discussion_batch(conn, batch, is_postgres)

    if updated > 0:
        console.print(f"  [green]build_discussion_search_index: indexed {updated} discussions[/green]")
    else:
        console.print(f"  [dim]build_discussion_search_index: all {total} discussions up to date[/dim]")

    return updated


def _flush_discussion_batch(
    conn: sqlite3.Connection,
    batch: list[tuple],
    is_postgres: bool,
) -> None:
    for row in batch:
        did, doc_text, doc_hash, company_domain, category, current_state, created, updated = row
        if is_postgres:
            conn.execute(
                "INSERT INTO discussion_search_docs"
                " (discussion_id, doc_text, doc_tsv, doc_tsv_simple, doc_hash, company_domain, category, current_state, created_at, updated_at)"
                " VALUES (?, ?, to_tsvector('english', ?), to_tsvector('simple', ?), ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT (discussion_id) DO UPDATE SET"
                " doc_text = EXCLUDED.doc_text,"
                " doc_tsv = to_tsvector('english', EXCLUDED.doc_text),"
                " doc_tsv_simple = to_tsvector('simple', EXCLUDED.doc_text),"
                " doc_hash = EXCLUDED.doc_hash,"
                " company_domain = EXCLUDED.company_domain,"
                " category = EXCLUDED.category,"
                " current_state = EXCLUDED.current_state,"
                " updated_at = EXCLUDED.updated_at",
                (did, doc_text, doc_text, doc_text, doc_hash, company_domain, category, current_state, created, updated),
            )
        else:
            conn.execute(
                """INSERT OR REPLACE INTO discussion_search_docs
                   (discussion_id, doc_text, doc_hash, company_domain, category, current_state, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (did, doc_text, doc_hash, company_domain, category, current_state, created, updated),
            )
    conn.commit()


def generate_discussion_embeddings(
    conn: sqlite3.Connection,
    config: object,
    console: Console | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    force: bool = False,
) -> int:
    """Generate embeddings for discussion_search_docs (fast model only, same as threads)."""
    from email_manager.search.embeddings import get_embedding_backend

    console = console or Console()
    is_postgres = type(conn).__name__ == "PostgresConnection"
    now = datetime.now(timezone.utc).isoformat()

    backend_name = getattr(config, "embedding_backend", "voyage")
    api_key = getattr(config, "voyage_api_key", "")
    ollama_url = getattr(config, "ollama_url", "http://localhost:11434")
    fast_model = getattr(config, "embedding_model_fast", "voyage-3-lite")
    fast_backend = get_embedding_backend(backend_name, fast_model, api_key, ollama_url)

    if force:
        rows = fetchall(
            conn,
            "SELECT discussion_id, doc_text, doc_hash FROM discussion_search_docs ORDER BY updated_at DESC",
        )
    else:
        rows = fetchall(
            conn,
            """SELECT dsd.discussion_id, dsd.doc_text, dsd.doc_hash
               FROM discussion_search_docs dsd
               WHERE NOT EXISTS (
                   SELECT 1 FROM discussion_embeddings de
                   WHERE de.discussion_id = dsd.discussion_id
                     AND de.model_name = ?
                     AND COALESCE(de.doc_hash, '') = COALESCE(dsd.doc_hash, '')
               )
               ORDER BY dsd.updated_at DESC""",
            (fast_model,),
        )

    total = len(rows)
    console.print(f"  [dim]{total} discussions need embeddings[/dim]")
    if total == 0:
        return 0

    BATCH = 32
    stored = 0
    for start in range(0, total, BATCH):
        chunk = rows[start : start + BATCH]
        texts = [r["doc_text"][:2000] for r in chunk]
        try:
            embeddings = fast_backend.embed_batch(texts)
        except Exception as e:
            console.print(f"  [red]Discussion embed batch failed at {start}: {e}[/red]")
            logger.exception("Discussion embed batch failed at %d", start)
            continue

        for row, emb in zip(chunk, embeddings):
            did = row["discussion_id"]
            doc_hash = row.get("doc_hash")
            if is_postgres:
                vec_str = "[" + ",".join(str(v) for v in emb) + "]"
                conn.execute(
                    "INSERT INTO discussion_embeddings (discussion_id, model_name, embedding, doc_hash, created_at)"
                    " VALUES (?, ?, ?::vector, ?, ?)"
                    " ON CONFLICT (discussion_id, model_name) DO UPDATE SET"
                    " embedding = EXCLUDED.embedding,"
                    " doc_hash = EXCLUDED.doc_hash,"
                    " created_at = EXCLUDED.created_at",
                    (did, fast_model, vec_str, doc_hash, now),
                )
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO discussion_embeddings (discussion_id, model_name, doc_hash, created_at)"
                    " VALUES (?, ?, ?, ?)",
                    (did, fast_model, doc_hash, now),
                )
            stored += 1

        conn.commit()
        if on_progress:
            on_progress(min(start + BATCH, total), total)

    if stored > 0:
        console.print(f"  [green]generate_discussion_embeddings: created {stored} embeddings[/green]")
    return stored
