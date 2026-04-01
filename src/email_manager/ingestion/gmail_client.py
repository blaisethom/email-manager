from __future__ import annotations

import base64
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.console import Console

from email_manager.config import EmailAccount
from email_manager.db import fetchone
from email_manager.ingestion.parser import parse_raw_email, email_to_db_row

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def _get_gmail_service(config: EmailAccount, *, remote: bool = False):
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
            if remote:
                creds = _run_remote_auth(flow)
            else:
                creds = flow.run_local_server(port=0)

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json())

    return build("gmail", "v1", credentials=creds)


AUTH_PORT = 8085


def _run_remote_auth(flow: object):
    """OAuth flow for headless/remote machines with no browser.

    Starts a local server on a fixed port and prints the auth URL for the
    user to open on another machine.  Works in two ways:

    1. SSH tunnel (recommended):
       ssh -L 8085:localhost:8085 user@remote-host
       Then open the printed URL in your local browser.

    2. Direct access (if the remote host is reachable):
       Open the printed URL, replacing 'localhost' with the remote host's
       IP/hostname.
    """
    console = Console()
    console.print(
        f"\n[bold yellow]Remote authentication mode[/bold yellow]\n\n"
        f"The OAuth server will listen on port [bold]{AUTH_PORT}[/bold].\n\n"
        f"[bold]Option 1 — SSH tunnel (recommended):[/bold]\n"
        f"  From your local machine, run:\n"
        f"  [cyan]ssh -L {AUTH_PORT}:localhost:{AUTH_PORT} <user>@<this-host>[/cyan]\n"
        f"  Then open the URL below in your local browser.\n\n"
        f"[bold]Option 2 — Direct access:[/bold]\n"
        f"  Open the URL below, replacing 'localhost' with this machine's hostname/IP.\n"
    )
    creds = flow.run_local_server(
        host="localhost",
        bind_addr="0.0.0.0",
        port=AUTH_PORT,
        open_browser=False,
    )
    return creds


def _sync_state_key(config: EmailAccount) -> str:
    """Per-account key for the sync_state table."""
    return f"gmail:{config.name}" if config.name else "gmail"


def authenticate(config: EmailAccount, *, remote: bool = False) -> None:
    """Run the OAuth flow and persist the token, without syncing."""
    _get_gmail_service(config, remote=remote)


def sync_emails(conn: sqlite3.Connection, config: EmailAccount, *, remote: bool = False) -> int:
    service = _get_gmail_service(config, remote=remote)
    console = Console()
    state_key = _sync_state_key(config)

    # Check if we have a stored historyId for incremental sync
    state = fetchone(
        conn,
        "SELECT last_uid, uidvalidity FROM sync_state WHERE folder = ?",
        (state_key,),
    )

    if state and state["last_uid"]:
        # Incremental sync via history API
        try:
            return _sync_incremental(service, conn, state["last_uid"], config, console)
        except Exception as e:
            # historyId may have expired, fall back to full sync
            console.print(f"[yellow]Incremental sync failed ({e}), doing full sync[/yellow]")

    return _sync_full(service, conn, config, console)


def _sync_full(service, conn: sqlite3.Connection, config: EmailAccount, console: Console) -> int:
    console.print("Performing full Gmail sync...")
    state_key = _sync_state_key(config)

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

    # Filter out messages already fetched (by gmail_id)
    existing = {
        r[0]
        for r in conn.execute(
            "SELECT gmail_id FROM emails WHERE gmail_id IS NOT NULL"
        ).fetchall()
    }
    to_fetch = [mid for mid in message_ids if mid not in existing]
    skipped = len(message_ids) - len(to_fetch)
    if skipped:
        console.print(f"Skipping {skipped} already-fetched messages...")
    if not to_fetch:
        console.print("[dim]All messages already fetched.[/dim]")
        return 0

    # Fetch each message in raw format, committing in batches
    BATCH_SIZE = 100
    new_count = 0
    latest_history_id = None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
    ) as progress:
        task = progress.add_task("Fetching messages", total=len(to_fetch))

        for msg_id in to_fetch:
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
                row["gmail_id"] = msg_id
                conn.execute(
                    """INSERT OR IGNORE INTO emails
                    (message_id, thread_id, subject, from_address, from_name,
                     to_addresses, cc_addresses, date, body_text, body_html,
                     raw_headers, folder, size_bytes, has_attachments, fetched_at,
                     gmail_id)
                    VALUES
                    (:message_id, :thread_id, :subject, :from_address, :from_name,
                     :to_addresses, :cc_addresses, :date, :body_text, :body_html,
                     :raw_headers, :folder, :size_bytes, :has_attachments, :fetched_at,
                     :gmail_id)""",
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

            # Commit in batches so progress survives interruption
            if new_count % BATCH_SIZE == 0 and new_count > 0:
                conn.commit()

    # Final commit
    if latest_history_id:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync)
            VALUES (?, ?, ?, ?)""",
            (state_key, 0, int(latest_history_id), now),
        )

    conn.commit()
    return new_count


def _sync_incremental(
    service, conn: sqlite3.Connection, start_history_id: int, config: EmailAccount, console: Console
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
                row["gmail_id"] = msg_id
                conn.execute(
                    """INSERT OR IGNORE INTO emails
                    (message_id, thread_id, subject, from_address, from_name,
                     to_addresses, cc_addresses, date, body_text, body_html,
                     raw_headers, folder, size_bytes, has_attachments, fetched_at,
                     gmail_id)
                    VALUES
                    (:message_id, :thread_id, :subject, :from_address, :from_name,
                     :to_addresses, :cc_addresses, :date, :body_text, :body_html,
                     :raw_headers, :folder, :size_bytes, :has_attachments, :fetched_at,
                     :gmail_id)""",
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
    state_key = _sync_state_key(config)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync)
        VALUES (?, ?, ?, ?)""",
        (state_key, 0, int(latest_history_id), now),
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
