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


def extract_base(conn: sqlite3.Connection, console: Console = None, limit: int | None = None, force: bool = False) -> int:
    """Extract all structured data from email headers: contacts, companies, domains."""
    if console is None:
        console = Console()

    # Skip if nothing has changed since the last run
    if not force:
        last_run = fetchone(
            conn,
            "SELECT completed_at FROM pipeline_runs WHERE stage = 'extract_base' AND status = 'success' ORDER BY completed_at DESC LIMIT 1",
        )
        if last_run and last_run["completed_at"]:
            new_emails = fetchone(
                conn,
                "SELECT COUNT(*) as cnt FROM emails WHERE fetched_at > ?",
                (last_run["completed_at"],),
            )
            if new_emails and new_emails["cnt"] == 0:
                console.print("  [dim]No new emails since last extract_base — skipping.[/dim]")
                return 0

    companies_count = _extract_companies(conn, console=console, limit=limit)
    contacts_count = _extract_contacts(conn, console=console)
    co_email_count = _compute_co_email_stats(conn, console=console)

    # Record this run
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO pipeline_runs (stage, email_id, status, started_at, completed_at) VALUES ('extract_base', 0, 'success', ?, ?)",
        (now, now),
    )
    conn.commit()
    return companies_count + contacts_count + co_email_count


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

    # Update best-known names and received count (dates are set in the single-pass below)
    conn.execute("""
        UPDATE contacts SET
            name = COALESCE(
                (SELECT from_name FROM emails WHERE from_address = contacts.email
                 AND from_name IS NOT NULL AND from_name != '' ORDER BY date DESC LIMIT 1),
                contacts.name
            ),
            received_count = (SELECT COUNT(*) FROM emails WHERE from_address = contacts.email)
    """)

    # Count sent/received and track dates in a single pass (avoids N*M LIKE scans)
    sent_counts: dict[str, int] = {}
    recv_counts: dict[str, int] = {}
    first_dates: dict[str, str] = {}
    last_dates: dict[str, str] = {}

    def _update_dates(addr: str, date: str) -> None:
        if addr not in first_dates or date < first_dates[addr]:
            first_dates[addr] = date
        if addr not in last_dates or date > last_dates[addr]:
            last_dates[addr] = date

    email_rows = fetchall(conn, "SELECT from_address, to_addresses, cc_addresses, date FROM emails")

    with _make_progress(console) as progress:
        task = progress.add_task("Counting sent/received", total=len(email_rows))
        for row in email_rows:
            addr = row["from_address"]
            date = row["date"]
            recv_counts[addr] = recv_counts.get(addr, 0) + 1
            _update_dates(addr, date)

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
                        _update_dates(a, date)

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
                """UPDATE contacts SET sent_count = ?, received_count = ?, email_count = ?,
                   first_seen = ?, last_seen = ?, company = COALESCE(company, ?)
                   WHERE id = ?""",
                (sent, received, sent + received,
                 first_dates.get(email_addr), last_dates.get(email_addr),
                 company, contact["id"]),
            )
            updated += 1
            progress.update(task, completed=updated)

    return updated


def _extract_companies(
    conn: sqlite3.Connection, console: Console = None, limit: int | None = None
) -> int:
    """Extract companies and their associated email addresses from email headers."""

    sql = """SELECT e.id, e.from_address, e.to_addresses, e.cc_addresses, e.date
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
        task = progress.add_task("Extracting companies", total=len(unprocessed))

        for row in unprocessed:
            email_id = row["id"]
            date = row["date"]

            # Collect all addresses from this email
            all_addresses = [row["from_address"]]
            for field in ("to_addresses", "cc_addresses"):
                raw = row[field]
                if raw:
                    try:
                        all_addresses.extend(json.loads(raw))
                    except (json.JSONDecodeError, TypeError):
                        pass

            for addr in all_addresses:
                addr = addr.strip().lower()
                if "@" not in addr:
                    continue
                domain = addr.split("@")[-1]
                company_name = _domain_to_company(domain)
                if not company_name:
                    continue

                # Upsert company
                conn.execute(
                    """INSERT INTO companies (name, domain, email_count, first_seen, last_seen)
                    VALUES (?, ?, 1, ?, ?)
                    ON CONFLICT(domain) DO UPDATE SET
                        email_count = email_count + 1,
                        first_seen = MIN(first_seen, excluded.first_seen),
                        last_seen = MAX(last_seen, excluded.last_seen)""",
                    (company_name, domain, date, date),
                )

                # Link contact email to company
                company_row = fetchone(conn, "SELECT id FROM companies WHERE domain = ?", (domain,))
                if company_row:
                    conn.execute(
                        "INSERT OR IGNORE INTO company_contacts (company_id, contact_email) VALUES (?, ?)",
                        (company_row["id"], addr),
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



def _domain_to_company(domain: str) -> str | None:
    free_providers = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "aol.com", "icloud.com", "mail.com", "protonmail.com",
        "proton.me", "live.com", "msn.com", "googlemail.com",
        "ymail.com", "me.com", "mac.com",
    }
    if domain.lower() in free_providers:
        return None
    # Two-level TLDs where the second-level part is generic (e.g. .com.au, .co.uk)
    _SECOND_LEVEL = {
        "com", "co", "org", "net", "edu", "gov", "ac", "mil",
        "gen", "biz", "info", "nom", "sch", "nhs",
    }
    parts = domain.split(".")
    # For domains like fourhats.com.au: parts = [fourhats, com, au]
    # If second-to-last part is a generic SLD, take the part before it
    if len(parts) >= 3 and parts[-2].lower() in _SECOND_LEVEL:
        return parts[-3].capitalize()
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return domain
