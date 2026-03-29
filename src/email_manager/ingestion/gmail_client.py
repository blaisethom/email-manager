from __future__ import annotations

import base64
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.console import Console

from email_manager.config import Config
from email_manager.db import fetchone
from email_manager.ingestion.parser import parse_raw_email, email_to_db_row

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def _get_gmail_service(config: Config):
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    token_path = config.gmail_token_path
    credentials_path = config.gmail_credentials_path

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Gmail credentials file not found at {credentials_path}. "
                    "Download it from Google Cloud Console > APIs & Credentials > OAuth 2.0 Client IDs."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path), SCOPES
            )
            creds = flow.run_local_server(port=0)

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def sync_emails(conn: sqlite3.Connection, config: Config) -> int:
    service = _get_gmail_service(config)
    console = Console()

    # Check if we have a stored historyId for incremental sync
    state = fetchone(
        conn,
        "SELECT last_uid, uidvalidity FROM sync_state WHERE folder = ?",
        ("gmail",),
    )

    if state and state["last_uid"]:
        # Incremental sync via history API
        try:
            return _sync_incremental(service, conn, state["last_uid"], config, console)
        except Exception as e:
            # historyId may have expired, fall back to full sync
            console.print(f"[yellow]Incremental sync failed ({e}), doing full sync[/yellow]")

    return _sync_full(service, conn, config, console)


def _sync_full(service, conn: sqlite3.Connection, config: Config, console: Console) -> int:
    console.print("Performing full Gmail sync...")

    # List all message IDs
    message_ids = []
    page_token = None
    labels = config.gmail_labels

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        task = progress.add_task("Listing messages...")

        while True:
            kwargs = {"userId": "me", "maxResults": 500}
            if labels:
                kwargs["labelIds"] = labels
            if page_token:
                kwargs["pageToken"] = page_token

            result = service.users().messages().list(**kwargs).execute()
            messages = result.get("messages", [])
            message_ids.extend(m["id"] for m in messages)
            progress.update(task, description=f"Listed {len(message_ids)} messages...")

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    if not message_ids:
        console.print("[dim]No messages found.[/dim]")
        return 0

    # Fetch each message in raw format
    new_count = 0
    latest_history_id = None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
    ) as progress:
        task = progress.add_task("Fetching messages", total=len(message_ids))

        for msg_id in message_ids:
            try:
                msg = (
                    service.users()
                    .messages()
                    .get(userId="me", id=msg_id, format="raw")
                    .execute()
                )
                raw_bytes = base64.urlsafe_b64decode(msg["raw"])
                label_folder = _labels_to_folder(msg.get("labelIds", []))

                em = parse_raw_email(raw_bytes, folder=label_folder)
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

                # Track the latest historyId for incremental sync
                h = msg.get("historyId")
                if h and (latest_history_id is None or int(h) > int(latest_history_id)):
                    latest_history_id = h

            except Exception as e:
                progress.console.print(f"[yellow]Skipping {msg_id}: {e}[/yellow]")

            progress.advance(task)

    # Store sync state (reuse sync_state table: folder="gmail", last_uid=historyId)
    if latest_history_id:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync)
            VALUES (?, ?, ?, ?)""",
            ("gmail", 0, int(latest_history_id), now),
        )

    conn.commit()
    return new_count


def _sync_incremental(
    service, conn: sqlite3.Connection, start_history_id: int, config: Config, console: Console
) -> int:
    console.print(f"Incremental Gmail sync from historyId {start_history_id}...")

    # Get message IDs added since last sync
    new_message_ids = []
    page_token = None

    while True:
        kwargs = {
            "userId": "me",
            "startHistoryId": str(start_history_id),
            "historyTypes": ["messageAdded"],
        }
        if page_token:
            kwargs["pageToken"] = page_token

        result = service.users().history().list(**kwargs).execute()
        for record in result.get("history", []):
            for added in record.get("messagesAdded", []):
                new_message_ids.append(added["message"]["id"])

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    if not new_message_ids:
        console.print("[dim]No new messages.[/dim]")
        return 0

    # Deduplicate
    new_message_ids = list(dict.fromkeys(new_message_ids))

    new_count = 0
    latest_history_id = start_history_id

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
    ) as progress:
        task = progress.add_task("Fetching new messages", total=len(new_message_ids))

        for msg_id in new_message_ids:
            try:
                msg = (
                    service.users()
                    .messages()
                    .get(userId="me", id=msg_id, format="raw")
                    .execute()
                )
                raw_bytes = base64.urlsafe_b64decode(msg["raw"])
                label_folder = _labels_to_folder(msg.get("labelIds", []))

                em = parse_raw_email(raw_bytes, folder=label_folder)
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

                h = msg.get("historyId")
                if h and int(h) > int(latest_history_id):
                    latest_history_id = int(h)

            except Exception as e:
                progress.console.print(f"[yellow]Skipping {msg_id}: {e}[/yellow]")

            progress.advance(task)

    # Update sync state
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync)
        VALUES (?, ?, ?, ?)""",
        ("gmail", 0, int(latest_history_id), now),
    )
    conn.commit()

    return new_count


def _labels_to_folder(label_ids: list[str]) -> str:
    priority = ["INBOX", "SENT", "DRAFT", "SPAM", "TRASH"]
    for label in priority:
        if label in label_ids:
            return label
    if label_ids:
        return label_ids[0]
    return "UNKNOWN"
