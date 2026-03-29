from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from imapclient import IMAPClient
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.config import Config
from email_manager.db import fetchone
from email_manager.ingestion.parser import parse_raw_email, email_to_db_row


def sync_emails(conn: sqlite3.Connection, config: Config) -> int:
    total_new = 0

    with _connect(config) as client:
        folders = config.imap_folders

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
        ) as progress:
            for folder_name in folders:
                try:
                    count = _sync_folder(client, conn, folder_name, progress)
                    total_new += count
                except Exception as e:
                    progress.console.print(
                        f"[red]Error syncing {folder_name}: {e}[/red]"
                    )

    return total_new


def _connect(config: Config) -> IMAPClient:
    client = IMAPClient(config.imap_host, port=config.imap_port, ssl=config.imap_use_ssl)
    client.login(config.imap_user, config.imap_password)
    return client


def _sync_folder(
    client: IMAPClient,
    conn: sqlite3.Connection,
    folder_name: str,
    progress: Progress,
) -> int:
    client.select_folder(folder_name, readonly=True)

    # Get current UIDVALIDITY
    folder_status = client.folder_status(folder_name, ["UIDVALIDITY", "MESSAGES"])
    uidvalidity = folder_status[b"UIDVALIDITY"]
    message_count = folder_status[b"MESSAGES"]

    # Check sync state
    state = fetchone(
        conn, "SELECT uidvalidity, last_uid FROM sync_state WHERE folder = ?", (folder_name,)
    )

    last_uid = 0
    if state is not None:
        if state["uidvalidity"] != uidvalidity:
            # UIDVALIDITY changed — must re-sync from scratch
            conn.execute("DELETE FROM emails WHERE folder = ?", (folder_name,))
            conn.execute("DELETE FROM sync_state WHERE folder = ?", (folder_name,))
            conn.commit()
        else:
            last_uid = state["last_uid"]

    # Fetch UIDs > last_uid
    search_criteria = f"UID {last_uid + 1}:*"
    uids = client.search(search_criteria)

    # Filter out UIDs we already have (the range is inclusive of last_uid+1)
    if last_uid > 0:
        uids = [uid for uid in uids if uid > last_uid]

    if not uids:
        return 0

    task = progress.add_task(f"Syncing {folder_name}", total=len(uids))
    new_count = 0

    # Fetch in batches of 100
    batch_size = 100
    for i in range(0, len(uids), batch_size):
        batch_uids = uids[i : i + batch_size]
        messages = client.fetch(batch_uids, ["RFC822"])

        for uid, data in messages.items():
            raw = data[b"RFC822"]
            try:
                em = parse_raw_email(raw, folder=folder_name)
                row = email_to_db_row(em)
                conn.execute(
                    """INSERT OR IGNORE INTO emails
                    (message_id, thread_id, subject, from_address, from_name,
                     to_addresses, cc_addresses, date, body_text, body_html,
                     raw_headers, folder, size_bytes, has_attachments, fetched_at)
                    VALUES
                    (:message_id, :thread_id, :subject, :from_address, :from_name,
                     :to_addresses, :cc_addresses, :date, :body_text, :body_html,
                     :raw_headers, :folder, :size_bytes, :has_attachments, :fetched_at)""",
                    row,
                )
                new_count += 1
            except Exception as e:
                progress.console.print(
                    f"[yellow]Skipping UID {uid}: {e}[/yellow]"
                )

            progress.advance(task)

    # Update sync state
    now = datetime.now(timezone.utc).isoformat()
    max_uid = max(uids)
    conn.execute(
        """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync)
        VALUES (?, ?, ?, ?)""",
        (folder_name, uidvalidity, max_uid, now),
    )
    conn.commit()

    return new_count
