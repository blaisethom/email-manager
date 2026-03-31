from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

from imapclient import IMAPClient
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.config import EmailAccount
from email_manager.db import fetchone
from email_manager.ingestion.parser import parse_raw_email, email_to_db_row

# Defaults — Yahoo needs smaller values
DEFAULT_BATCH_SIZE = 100
YAHOO_BATCH_SIZE = 50
MAX_RETRIES = 5
RETRY_BASE_DELAY = 5  # seconds, doubles each retry
DB_RETRY_DELAY = 2


def sync_emails(conn: sqlite3.Connection, config: EmailAccount) -> int:
    total_new = 0
    is_yahoo = _is_yahoo(config.imap_host)
    batch_size = YAHOO_BATCH_SIZE if is_yahoo else DEFAULT_BATCH_SIZE

    is_yahoo = _is_yahoo(config.imap_host)

    folders = config.imap_folders
    if not folders or folders == ["*"]:
        client = _connect_with_retry(config)
        try:
            folders = _list_folders(client)
        finally:
            try:
                client.logout()
            except Exception:
                pass

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
    ) as progress:
        for folder_name in folders:
            try:
                count = _sync_folder_with_reconnect(
                    config, conn, folder_name, progress,
                    batch_size=batch_size, is_yahoo=is_yahoo,
                    use_export=is_yahoo,
                )
                total_new += count
            except Exception as e:
                progress.console.print(
                    f"[red]Error syncing {folder_name}: {e}[/red]"
                )

    return total_new


def _is_yahoo(host: str) -> bool:
    host = host.lower()
    return "yahoo" in host or "aol" in host or "ymail" in host


def _list_folders(client: IMAPClient) -> list[str]:
    """List all available folders on the IMAP server, skipping non-selectable ones."""
    raw_folders = client.list_folders()
    folders = []
    for flags, delimiter, name in raw_folders:
        flag_strs = [f.decode() if isinstance(f, bytes) else str(f) for f in flags]
        if "\\Noselect" in flag_strs or "\\NonExistent" in flag_strs:
            continue
        folders.append(name)
    return folders


def _connect_with_retry(config: EmailAccount, use_export: bool = False) -> IMAPClient:
    host = config.imap_host
    # Yahoo export endpoint supports up to 100k messages per folder (vs 10k on standard)
    if use_export and _is_yahoo(host):
        host = "export.imap.mail.yahoo.com"

    for attempt in range(MAX_RETRIES):
        try:
            client = IMAPClient(
                host, port=config.imap_port, ssl=config.imap_use_ssl,
                timeout=30,
            )
            client.login(config.imap_user, config.imap_password)
            return client
        except Exception as e:
            error_str = str(e).lower()
            is_retryable = any(kw in error_str for kw in (
                "limit", "rate", "try again", "too many", "refused",
                "temporary", "timeout", "timed out",
            ))
            if attempt < MAX_RETRIES - 1 and is_retryable:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                time.sleep(delay)
            else:
                raise
    raise RuntimeError(f"Failed to connect to {config.imap_host} after {MAX_RETRIES} attempts")


def _sync_folder_with_reconnect(
    config: EmailAccount,
    conn: sqlite3.Connection,
    folder_name: str,
    progress: Progress,
    batch_size: int = DEFAULT_BATCH_SIZE,
    is_yahoo: bool = False,
    use_export: bool = False,
) -> int:
    """Sync a folder, reconnecting if the connection drops mid-sync."""
    for attempt in range(MAX_RETRIES):
        try:
            client = _connect_with_retry(config, use_export=use_export)
            try:
                return _sync_folder(
                    client, conn, folder_name, progress,
                    batch_size=batch_size, is_yahoo=is_yahoo,
                )
            finally:
                try:
                    client.logout()
                except Exception:
                    pass
        except Exception as e:
            error_str = str(e).lower()
            is_connection_error = any(kw in error_str for kw in (
                "bye", "connection", "broken pipe", "eof", "reset",
                "timeout", "timed out", "refused", "limit", "rate",
                "socket", "abort", "ssl",
            ))
            if attempt < MAX_RETRIES - 1 and is_connection_error:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                progress.console.print(
                    f"[yellow]Connection lost on {folder_name} (attempt {attempt + 1}/{MAX_RETRIES}): "
                    f"{str(e)[:80]}. Reconnecting in {delay}s...[/yellow]"
                )
                time.sleep(delay)
                # Loop continues — will reconnect and _sync_folder picks up from last_uid
            else:
                raise

    return 0


def _sync_folder(
    client: IMAPClient,
    conn: sqlite3.Connection,
    folder_name: str,
    progress: Progress,
    batch_size: int = DEFAULT_BATCH_SIZE,
    is_yahoo: bool = False,
) -> int:
    task = progress.add_task(f"Checking {folder_name}", total=None)

    client.select_folder(folder_name, readonly=True)

    # Get current UIDVALIDITY
    folder_status = client.folder_status(folder_name, ["UIDVALIDITY", "MESSAGES"])
    uidvalidity = folder_status[b"UIDVALIDITY"]

    # Check sync state
    state = fetchone(
        conn, "SELECT uidvalidity, last_uid FROM sync_state WHERE folder = ?", (folder_name,)
    )

    last_uid = 0
    if state is not None:
        if state["uidvalidity"] != uidvalidity:
            conn.execute("DELETE FROM emails WHERE folder = ?", (folder_name,))
            conn.execute("DELETE FROM sync_state WHERE folder = ?", (folder_name,))
            _db_commit(conn)
        else:
            last_uid = state["last_uid"]

    # Fetch UIDs > last_uid
    search_criteria = f"UID {last_uid + 1}:*"
    uids = client.search(search_criteria)

    if last_uid > 0:
        uids = [uid for uid in uids if uid > last_uid]

    if not uids:
        progress.update(task, description=f"{folder_name} ✓", total=0, completed=0)
        return 0

    progress.update(task, description=f"Syncing {folder_name}", total=len(uids), completed=0)
    new_count = 0

    for i in range(0, len(uids), batch_size):
        batch_uids = uids[i : i + batch_size]

        messages = _fetch_batch_with_retry(
            client, batch_uids, progress, is_yahoo=is_yahoo
        )

        max_processed_uid = last_uid
        for uid, data in messages.items():
            raw = data.get(b"RFC822")
            if not raw:
                max_processed_uid = max(max_processed_uid, uid)
                progress.advance(task)
                continue
            try:
                em = parse_raw_email(raw, folder=folder_name)
                row = email_to_db_row(em)
                _db_insert_email(conn, row)
                new_count += 1
                max_processed_uid = max(max_processed_uid, uid)
            except Exception as e:
                # Parse errors are non-recoverable, advance past them
                max_processed_uid = max(max_processed_uid, uid)
                progress.console.print(
                    f"[yellow]Skipping UID {uid}: {e}[/yellow]"
                )

            progress.advance(task)

        # Advance progress for UIDs that weren't in the response (fetched 0 messages)
        fetched_count = len(messages)
        unfetched = len(batch_uids) - fetched_count
        for _ in range(unfetched):
            progress.advance(task)

        # Only advance last_uid if we actually processed messages
        if max_processed_uid > last_uid:
            last_uid = max_processed_uid
            now = datetime.now(timezone.utc).isoformat()
            _db_execute_with_retry(
                conn,
                """INSERT OR REPLACE INTO sync_state (folder, uidvalidity, last_uid, last_sync)
                VALUES (?, ?, ?, ?)""",
                (folder_name, uidvalidity, last_uid, now),
            )
            _db_commit(conn)

        # Rate limit pause for Yahoo
        if is_yahoo and i + batch_size < len(uids):
            time.sleep(1)

    return new_count


def _db_insert_email(conn: sqlite3.Connection, row: dict) -> None:
    """Insert an email with retry on database lock."""
    _db_execute_with_retry(
        conn,
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


def _db_execute_with_retry(conn: sqlite3.Connection, sql: str, params=None, retries: int = 5) -> None:
    """Execute SQL with retry on database lock."""
    for attempt in range(retries):
        try:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < retries - 1:
                time.sleep(DB_RETRY_DELAY * (attempt + 1))
            else:
                raise


def _db_commit(conn: sqlite3.Connection, retries: int = 5) -> None:
    """Commit with retry on database lock."""
    for attempt in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < retries - 1:
                time.sleep(DB_RETRY_DELAY * (attempt + 1))
            else:
                raise


def _fetch_batch_with_retry(
    client: IMAPClient,
    uids: list[int],
    progress: Progress,
    is_yahoo: bool = False,
) -> dict:
    """Fetch a batch of messages with retry logic for server errors."""
    for attempt in range(MAX_RETRIES):
        try:
            return client.fetch(uids, ["RFC822"])
        except Exception as e:
            error_str = str(e).lower()
            is_connection_error = any(kw in error_str for kw in (
                "bye", "connection", "broken pipe", "eof", "reset",
                "timeout", "timed out", "socket", "abort", "ssl",
            ))
            is_retryable = is_connection_error or any(kw in error_str for kw in (
                "serverbug", "try again", "too many", "rate",
            ))
            if not is_retryable or attempt >= MAX_RETRIES - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            progress.console.print(
                f"[yellow]Fetch error (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)[:100]}. "
                f"Retrying in {delay}s...[/yellow]"
            )
            time.sleep(delay)
            # Connection is dead — don't try individual fetches, let the
            # caller reconnect and retry the whole batch.
            if is_connection_error:
                raise
            # For non-connection server errors, fall back to one-at-a-time
            if len(uids) > 1 and attempt >= 1:
                progress.console.print(
                    f"[yellow]Falling back to individual fetches...[/yellow]"
                )
                return _fetch_individually(client, uids, progress)

    return {}


def _fetch_individually(
    client: IMAPClient, uids: list[int], progress: Progress
) -> dict:
    """Fetch messages one at a time as a fallback for problematic servers."""
    results = {}
    consecutive_errors = 0
    for uid in uids:
        try:
            batch = client.fetch([uid], ["RFC822"])
            results.update(batch)
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            progress.console.print(f"[yellow]Skipping UID {uid}: {e}[/yellow]")
            # If we get many consecutive errors the connection is likely dead —
            # stop and let the caller reconnect instead of skipping everything.
            if consecutive_errors >= 3:
                raise
        time.sleep(0.2)
    return results
