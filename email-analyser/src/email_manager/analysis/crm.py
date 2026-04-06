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

    # Update counts and names for existing contacts
    conn.execute("""
        UPDATE contacts SET
            name = COALESCE(
                (SELECT from_name FROM emails WHERE from_address = contacts.email AND from_name IS NOT NULL ORDER BY date DESC LIMIT 1),
                contacts.name
            ),
            received_count = (SELECT COUNT(*) FROM emails WHERE from_address = contacts.email)
    """)

    # Count sent emails (where contact appears in to_addresses)
    all_contacts = fetchall(conn, "SELECT id, email FROM contacts")
    updated = 0

    for contact in all_contacts:
        email_addr = contact["email"]
        like_pattern = f'%"{email_addr}"%'

        # Count emails where this contact is in to_addresses or cc_addresses
        sent = fetchone(
            conn,
            """SELECT COUNT(*) as cnt FROM emails
               WHERE to_addresses LIKE ? OR cc_addresses LIKE ?""",
            (like_pattern, like_pattern),
        )
        sent_count = sent["cnt"] if sent else 0

        # Get accurate first/last seen across all roles (sender + recipient)
        dates = fetchone(
            conn,
            """SELECT MIN(date) as first_seen, MAX(date) as last_seen FROM emails
               WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?""",
            (email_addr, like_pattern, like_pattern),
        )

        total = (contact.get("received_count") or 0) + sent_count

        # Infer company from email domain
        domain = email_addr.split("@")[-1] if "@" in email_addr else None
        company = _domain_to_company(domain) if domain else None

        conn.execute(
            """UPDATE contacts SET sent_count = ?, email_count = ?,
               first_seen = ?, last_seen = ?, company = COALESCE(company, ?)
               WHERE id = ?""",
            (sent_count, total, dates["first_seen"], dates["last_seen"],
             company, contact["id"]),
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
    # Two-level TLDs where the second-level part is generic (e.g. .com.au, .co.uk)
    _SECOND_LEVEL = {
        "com", "co", "org", "net", "edu", "gov", "ac", "mil",
        "gen", "biz", "info", "nom", "sch", "nhs",
    }
    # Use domain as company name (strip TLD for readability)
    parts = domain.split(".")
    # For domains like fourhats.com.au: parts = [fourhats, com, au]
    # If second-to-last part is a generic SLD, take the part before it
    if len(parts) >= 3 and parts[-2].lower() in _SECOND_LEVEL:
        return parts[-3].capitalize()
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return domain
