"""Sync Google Calendar events into the local database."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone, timedelta

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.config import EmailAccount
from email_manager.db import fetchone

# Calendar needs read-only access; Gmail token may not include this scope.
CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
# Combined scopes so re-auth covers both Gmail and Calendar.
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
ALL_SCOPES = GMAIL_SCOPES + CALENDAR_SCOPES


def _get_calendar_service(config: EmailAccount, *, remote: bool = False):
    """Build a Google Calendar API service, re-authing if the calendar scope is missing."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    token_path = config.gmail_token_path
    credentials_path = config.gmail_credentials_path

    creds = None
    stored_email = None
    needs_reauth = False

    if token_path.exists():
        token_data = json.loads(token_path.read_text())
        stored_email = token_data.get("authenticated_email")
        # Check the STORED scopes in the token file, not what Credentials reports
        stored_scopes = token_data.get("scopes", [])
        has_calendar = any("calendar" in s for s in stored_scopes)

        if has_calendar:
            creds = Credentials.from_authorized_user_file(str(token_path), ALL_SCOPES)
        else:
            needs_reauth = True

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token and not needs_reauth:
            try:
                creds.refresh(Request())
            except Exception:
                # Refresh failed (e.g. scope mismatch) — need re-auth
                needs_reauth = True
                creds = None

        if needs_reauth or not creds:
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Gmail credentials file not found at {credentials_path}. "
                    "Download it from Google Cloud Console."
                )
            console = Console()
            console.print(
                "[yellow]Calendar scope not found in existing token. "
                "Re-authenticating with calendar access...[/yellow]"
            )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path), ALL_SCOPES
            )
            if remote:
                from email_manager.ingestion.gmail_client import _run_remote_auth
                creds = _run_remote_auth(flow)
            else:
                creds = flow.run_local_server(port=0)

            # Persist updated token with all scopes
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_data = json.loads(creds.to_json())
            if stored_email:
                token_data["authenticated_email"] = stored_email
            token_path.write_text(json.dumps(token_data, indent=2))

    return build("calendar", "v3", credentials=creds)


def needs_calendar_auth(config: EmailAccount) -> bool:
    """Check whether this account needs re-auth to get calendar scope."""
    token_path = config.gmail_token_path
    if not token_path.exists():
        return True
    try:
        token_data = json.loads(token_path.read_text())
        stored_scopes = token_data.get("scopes", [])
        return not any("calendar" in s for s in stored_scopes)
    except (json.JSONDecodeError, OSError):
        return True


def _sync_state_key(config: EmailAccount) -> str:
    return f"calendar:{config.name}" if config.name else "calendar"


def sync_calendar_events(
    conn: sqlite3.Connection,
    config: EmailAccount,
    *,
    console: Console | None = None,
    remote: bool = False,
    months_back: int = 6,
) -> int:
    """Sync Google Calendar events for an account. Returns count of new/updated events."""
    if console is None:
        console = Console()

    service = _get_calendar_service(config, remote=remote)
    state_key = _sync_state_key(config)

    # Check for existing sync token
    state = fetchone(
        conn,
        "SELECT sync_token FROM sync_state WHERE folder = ?",
        (state_key,),
    )
    sync_token = state["sync_token"] if state and state["sync_token"] else None

    now_str = datetime.now(timezone.utc).isoformat()
    count = 0

    try:
        if sync_token:
            count = _sync_incremental(service, conn, config, sync_token, state_key, now_str, console)
        else:
            count = _sync_full(service, conn, config, state_key, now_str, months_back, console)
    except Exception as e:
        if "410" in str(e) or "Gone" in str(e):
            console.print("[yellow]Sync token expired, doing full sync...[/yellow]")
            count = _sync_full(service, conn, config, state_key, now_str, months_back, console)
        else:
            raise

    return count


def _sync_full(
    service, conn: sqlite3.Connection, config: EmailAccount,
    state_key: str, now_str: str, months_back: int, console: Console,
) -> int:
    time_min = (datetime.now(timezone.utc) - timedelta(days=months_back * 30)).isoformat()

    all_events = []
    page_token = None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        fetch_task = progress.add_task("Fetching calendar events", total=None)
        while True:
            kwargs = {
                "calendarId": "primary",
                "timeMin": time_min,
                "singleEvents": True,  # expand recurring events
                "maxResults": 250,
                # Note: orderBy is intentionally omitted — the API does not
                # return nextSyncToken when orderBy is specified.
            }
            if page_token:
                kwargs["pageToken"] = page_token

            result = service.events().list(**kwargs).execute()
            items = result.get("items", [])
            all_events.extend(items)
            progress.update(fetch_task, description="Fetching calendar events", completed=len(all_events))
            page_token = result.get("nextPageToken")
            if not page_token:
                break

        progress.update(fetch_task, description="Fetched calendar events", total=len(all_events), completed=len(all_events))

        next_sync_token = result.get("nextSyncToken")

        count = 0
        save_task = progress.add_task("Saving calendar events", total=len(all_events))
        for event in all_events:
            if _save_event(conn, config, event, now_str):
                count += 1
            progress.advance(save_task)

    # Store sync token
    conn.execute(
        """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync, sync_token)
           VALUES (?, 0, 0, ?, ?)""",
        (state_key, now_str, next_sync_token),
    )
    conn.commit()
    return count


def _sync_incremental(
    service, conn: sqlite3.Connection, config: EmailAccount,
    sync_token: str, state_key: str, now_str: str, console: Console,
) -> int:
    all_events = []
    page_token = None
    count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        fetch_task = progress.add_task("Fetching calendar changes", total=None)
        while True:
            kwargs = {"calendarId": "primary", "syncToken": sync_token}
            if page_token:
                kwargs["pageToken"] = page_token

            result = service.events().list(**kwargs).execute()
            items = result.get("items", [])
            all_events.extend(items)
            progress.update(fetch_task, description="Fetching calendar changes", completed=len(all_events))
            page_token = result.get("nextPageToken")
            if not page_token:
                break

        if not all_events:
            progress.update(fetch_task, description="No calendar changes", total=0, completed=0)
        else:
            progress.update(fetch_task, description="Fetched calendar changes", total=len(all_events), completed=len(all_events))

            count = 0
            save_task = progress.add_task("Saving calendar changes", total=len(all_events))
            for event in all_events:
                if event.get("status") == "cancelled":
                    conn.execute("DELETE FROM calendar_events WHERE event_id = ?", (event["id"],))
                elif _save_event(conn, config, event, now_str):
                    count += 1
                progress.advance(save_task)

    next_sync_token = result.get("nextSyncToken")

    conn.execute(
        """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync, sync_token)
           VALUES (?, 0, 0, ?, ?)""",
        (state_key, now_str, next_sync_token),
    )
    conn.commit()
    return count


def _save_event(conn: sqlite3.Connection, config: EmailAccount, event: dict, now_str: str) -> bool:
    """Save a single calendar event. Returns True if inserted/updated."""
    event_id = event.get("id")
    if not event_id:
        return False

    start = event.get("start", {})
    end = event.get("end", {})

    # All-day events use 'date', timed events use 'dateTime'
    all_day = "date" in start and "dateTime" not in start
    start_time = start.get("dateTime") or start.get("date", "")
    end_time = end.get("dateTime") or end.get("date", "")

    if not start_time or not end_time:
        return False

    attendees = []
    for a in event.get("attendees", []):
        entry = {"email": a.get("email", ""), "response_status": a.get("responseStatus", "")}
        if a.get("displayName"):
            entry["name"] = a["displayName"]
        attendees.append(entry)

    organizer = event.get("organizer", {})

    conn.execute(
        """INSERT OR REPLACE INTO calendar_events
           (event_id, calendar_id, account_name, title, description, location,
            start_time, end_time, all_day, status, organizer_email, attendees,
            html_link, recurring_event_id, created_at, updated_at, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            event_id,
            "primary",
            config.name,
            event.get("summary", ""),
            event.get("description", ""),
            event.get("location", ""),
            start_time,
            end_time,
            1 if all_day else 0,
            event.get("status", ""),
            organizer.get("email", ""),
            json.dumps(attendees) if attendees else None,
            event.get("htmlLink", ""),
            event.get("recurringEventId", ""),
            event.get("created", ""),
            event.get("updated", ""),
            now_str,
        ),
    )
    return True
