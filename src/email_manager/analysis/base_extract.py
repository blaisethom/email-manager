"""Extract contacts, companies, and domains from email headers — no AI needed."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.db import fetchall, fetchone


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    )


def extract_base(conn: sqlite3.Connection, console: Console = None, limit: int | None = None) -> int:
    """Extract all structured data from email headers: contacts, companies, domains, entities."""
    if console is None:
        console = Console()

    entities_count = _extract_header_entities(conn, console=console, limit=limit)
    contacts_count = _extract_contacts(conn, console=console)
    co_email_count = _compute_co_email_stats(conn, console=console)
    conn.commit()
    return entities_count + contacts_count + co_email_count


def _extract_contacts(conn: sqlite3.Connection, console: Console = None) -> int:
    """Build/update contacts table from all email addresses in headers."""

    # Upsert contacts from the 'from' field
    conn.execute("""
        INSERT OR IGNORE INTO contacts (email, name, first_seen, last_seen, email_count)
        SELECT
            from_address,
            from_name,
            MIN(date),
            MAX(date),
            COUNT(*)
        FROM emails
        GROUP BY from_address
    """)

    # Extract contacts from to_addresses and cc_addresses (stored as JSON arrays)
    rows = fetchall(conn, "SELECT id, to_addresses, cc_addresses, date FROM emails")
    for row in rows:
        for field in ("to_addresses", "cc_addresses"):
            raw = row[field]
            if not raw:
                continue
            try:
                addresses = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            for addr in addresses:
                addr = addr.strip().lower()
                if not addr or "@" not in addr:
                    continue
                conn.execute(
                    """INSERT OR IGNORE INTO contacts (email, first_seen, last_seen, email_count)
                    VALUES (?, ?, ?, 0)""",
                    (addr, row["date"], row["date"]),
                )

    # Now update all contacts with accurate counts and best-known names
    conn.execute("""
        UPDATE contacts SET
            name = COALESCE(
                (SELECT from_name FROM emails WHERE from_address = contacts.email
                 AND from_name IS NOT NULL AND from_name != '' ORDER BY date DESC LIMIT 1),
                contacts.name
            ),
            first_seen = COALESCE(
                (SELECT MIN(date) FROM emails WHERE from_address = contacts.email),
                contacts.first_seen
            ),
            last_seen = COALESCE(
                (SELECT MAX(date) FROM emails WHERE from_address = contacts.email),
                contacts.last_seen
            ),
            received_count = (SELECT COUNT(*) FROM emails WHERE from_address = contacts.email)
    """)

    # Count sent/received in a single pass over emails (avoids N*M LIKE scans)
    sent_counts: dict[str, int] = {}
    recv_counts: dict[str, int] = {}

    email_rows = fetchall(conn, "SELECT from_address, to_addresses, cc_addresses FROM emails")

    with _make_progress(console) as progress:
        task = progress.add_task("Counting sent/received", total=len(email_rows))
        for row in email_rows:
            addr = row["from_address"]
            recv_counts[addr] = recv_counts.get(addr, 0) + 1

            for field in ("to_addresses", "cc_addresses"):
                raw = row[field]
                if not raw:
                    continue
                try:
                    addresses = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                for a in addresses:
                    a = a.strip().lower()
                    if a and "@" in a:
                        sent_counts[a] = sent_counts.get(a, 0) + 1

            progress.advance(task)

    all_contacts = fetchall(conn, "SELECT id, email FROM contacts")
    updated = 0

    with _make_progress(console) as progress:
        task = progress.add_task("Updating contacts", total=len(all_contacts))

        for contact in all_contacts:
            email_addr = contact["email"]
            sent = sent_counts.get(email_addr, 0)
            received = recv_counts.get(email_addr, 0)

            domain = email_addr.split("@")[-1] if "@" in email_addr else None
            company = _domain_to_company(domain) if domain else None

            conn.execute(
                """UPDATE contacts SET sent_count = ?, received_count = ?, email_count = ?, company = COALESCE(company, ?)
                   WHERE id = ?""",
                (sent, received, sent + received, company, contact["id"]),
            )
            updated += 1
            progress.update(task, completed=updated)

    return updated


def _extract_header_entities(
    conn: sqlite3.Connection, console: Console = None, limit: int | None = None
) -> int:
    """Extract person and company entities from email headers into the entities table."""

    sql = """SELECT e.id, e.from_address, e.from_name, e.to_addresses, e.cc_addresses, e.date
             FROM emails e
             LEFT JOIN pipeline_runs pr ON e.id = pr.email_id AND pr.stage = 'extract_base'
             WHERE pr.id IS NULL OR pr.status = 'error'
             ORDER BY e.date DESC"""
    if limit:
        sql += f" LIMIT {int(limit)}"

    unprocessed = fetchall(conn, sql)
    if not unprocessed:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    processed = 0

    with _make_progress(console) as progress:
        task = progress.add_task("Extracting header entities", total=len(unprocessed))

        for row in unprocessed:
            email_id = row["id"]

            # Extract person entities from From, To, Cc
            _extract_person_entity(conn, email_id, row["from_address"], row["from_name"], "sender")

            for field, role in [("to_addresses", "recipient"), ("cc_addresses", "cc")]:
                raw = row[field]
                if not raw:
                    continue
                try:
                    addresses = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                for addr in addresses:
                    addr = addr.strip().lower()
                    if addr and "@" in addr:
                        _extract_person_entity(conn, email_id, addr, None, role)

            # Extract company entities from domains
            all_addresses = [row["from_address"]]
            for field in ("to_addresses", "cc_addresses"):
                raw = row[field]
                if raw:
                    try:
                        all_addresses.extend(json.loads(raw))
                    except (json.JSONDecodeError, TypeError):
                        pass

            seen_companies = set()
            for addr in all_addresses:
                addr = addr.strip().lower()
                if "@" not in addr:
                    continue
                domain = addr.split("@")[-1]
                company = _domain_to_company(domain)
                if company and company not in seen_companies:
                    seen_companies.add(company)
                    conn.execute(
                        """INSERT INTO entities (email_id, entity_type, value, context, confidence)
                        VALUES (?, 'company', ?, ?, 1.0)""",
                        (email_id, company, f"domain: {domain}"),
                    )

            # Mark as processed
            conn.execute(
                """INSERT OR REPLACE INTO pipeline_runs (stage, email_id, status, model_used, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                ("extract_base", email_id, "complete", "header-parser", now, now),
            )
            processed += 1
            progress.update(task, completed=processed)

    conn.commit()
    return processed


def _compute_co_email_stats(conn: sqlite3.Connection, console: Console = None) -> int:
    """Compute co-emailing stats for every pair of addresses that appear on the same email."""

    rows = fetchall(
        conn,
        "SELECT id, from_address, to_addresses, cc_addresses, date FROM emails",
    )

    if not rows:
        return 0

    # Clear and rebuild — this is fast enough for 10-100k emails
    conn.execute("DELETE FROM co_email_stats")

    # Accumulate pairs in memory then bulk insert
    pair_stats: dict[tuple[str, str], list] = {}  # (a, b) -> [count, first_date, last_date]

    with _make_progress(console) as progress:
        task = progress.add_task("Computing co-email stats", total=len(rows))

        for row in rows:
            # Collect all participants on this email
            participants = set()
            participants.add(row["from_address"].lower())

            for field in ("to_addresses", "cc_addresses"):
                raw = row[field]
                if not raw:
                    continue
                try:
                    addresses = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                for addr in addresses:
                    addr = addr.strip().lower()
                    if addr and "@" in addr:
                        participants.add(addr)

            # Generate all unique pairs (sorted so a < b, avoids duplicates)
            participants = sorted(participants)
            date = row["date"]

            for i in range(len(participants)):
                for j in range(i + 1, len(participants)):
                    pair = (participants[i], participants[j])
                    if pair not in pair_stats:
                        pair_stats[pair] = [0, date, date]
                    stats = pair_stats[pair]
                    stats[0] += 1
                    if date < stats[1]:
                        stats[1] = date
                    if date > stats[2]:
                        stats[2] = date

            progress.advance(task)

    # Bulk insert
    with _make_progress(console) as progress:
        total_pairs = len(pair_stats)
        task = progress.add_task("Writing co-email pairs", total=total_pairs)
        inserted = 0

        for (email_a, email_b), (count, first_date, last_date) in pair_stats.items():
            conn.execute(
                """INSERT INTO co_email_stats (email_a, email_b, co_email_count, first_co_email, last_co_email)
                VALUES (?, ?, ?, ?, ?)""",
                (email_a, email_b, count, first_date, last_date),
            )
            inserted += 1
            if inserted % 1000 == 0:
                progress.update(task, completed=inserted)

        progress.update(task, completed=inserted)

    return total_pairs


def _extract_person_entity(
    conn: sqlite3.Connection, email_id: int, address: str, name: str | None, role: str
) -> None:
    if not address or "@" not in address:
        return
    display = name if name and name.strip() else address
    conn.execute(
        """INSERT INTO entities (email_id, entity_type, value, context, confidence)
        VALUES (?, 'person', ?, ?, 1.0)""",
        (email_id, display, role),
    )


def _domain_to_company(domain: str) -> str | None:
    free_providers = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "aol.com", "icloud.com", "mail.com", "protonmail.com",
        "proton.me", "live.com", "msn.com", "googlemail.com",
        "ymail.com", "me.com", "mac.com",
    }
    if domain.lower() in free_providers:
        return None
    parts = domain.split(".")
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return domain
