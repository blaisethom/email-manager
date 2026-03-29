from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from email_manager.db import fetchall, fetchone


def build_crm(conn: sqlite3.Connection) -> int:
    # Aggregate contacts from email data
    # Collect all unique email addresses from from_address, to_addresses, cc_addresses

    # First: contacts from 'from' field
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

    # Update counts for existing contacts
    conn.execute("""
        UPDATE contacts SET
            name = COALESCE(
                (SELECT from_name FROM emails WHERE from_address = contacts.email AND from_name IS NOT NULL ORDER BY date DESC LIMIT 1),
                contacts.name
            ),
            first_seen = (SELECT MIN(date) FROM emails WHERE from_address = contacts.email),
            last_seen = (SELECT MAX(date) FROM emails WHERE from_address = contacts.email),
            received_count = (SELECT COUNT(*) FROM emails WHERE from_address = contacts.email)
    """)

    # Count sent emails (where contact appears in to_addresses)
    all_contacts = fetchall(conn, "SELECT id, email FROM contacts")
    updated = 0

    for contact in all_contacts:
        email_addr = contact["email"]
        # Count emails where this contact is in to_addresses or cc_addresses
        sent = fetchone(
            conn,
            """SELECT COUNT(*) as cnt FROM emails
               WHERE to_addresses LIKE ? OR cc_addresses LIKE ?""",
            (f'%"{email_addr}"%', f'%"{email_addr}"%'),
        )
        sent_count = sent["cnt"] if sent else 0

        total = (contact.get("received_count") or 0) + sent_count

        # Infer company from email domain
        domain = email_addr.split("@")[-1] if "@" in email_addr else None
        company = _domain_to_company(domain) if domain else None

        conn.execute(
            """UPDATE contacts SET sent_count = ?, email_count = ?, company = COALESCE(company, ?)
               WHERE id = ?""",
            (sent_count, total, company, contact["id"]),
        )
        updated += 1

    conn.commit()
    return updated


def _domain_to_company(domain: str) -> str | None:
    # Skip common free email providers
    free_providers = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "aol.com", "icloud.com", "mail.com", "protonmail.com",
        "proton.me", "live.com", "msn.com",
    }
    if domain.lower() in free_providers:
        return None
    # Use domain as company name (strip TLD for readability)
    parts = domain.split(".")
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return domain
