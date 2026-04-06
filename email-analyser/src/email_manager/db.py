from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from email_manager.config import Config

SCHEMA_VERSION = 12

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS emails (
    id              INTEGER PRIMARY KEY,
    message_id      TEXT UNIQUE NOT NULL,
    thread_id       TEXT,
    subject         TEXT,
    normalised_subject TEXT,
    from_address    TEXT NOT NULL,
    from_name       TEXT,
    to_addresses    TEXT,
    cc_addresses    TEXT,
    date            TEXT NOT NULL,
    body_text       TEXT,
    body_html       TEXT,
    raw_headers     TEXT,
    folder          TEXT,
    size_bytes      INTEGER,
    has_attachments INTEGER DEFAULT 0,
    fetched_at      TEXT NOT NULL,
    gmail_id        TEXT,
    account_name    TEXT
);

CREATE INDEX IF NOT EXISTS idx_emails_thread ON emails(thread_id);
CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date);
CREATE INDEX IF NOT EXISTS idx_emails_from ON emails(from_address);
CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(message_id);

CREATE TABLE IF NOT EXISTS sync_state (
    folder          TEXT PRIMARY KEY,
    uidvalidity     INTEGER NOT NULL,
    last_uid        INTEGER NOT NULL DEFAULT 0,
    last_sync       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contacts (
    id              INTEGER PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    name            TEXT,
    company         TEXT,
    first_seen      TEXT,
    last_seen       TEXT,
    email_count     INTEGER DEFAULT 0,
    sent_count      INTEGER DEFAULT 0,
    received_count  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS threads (
    id              INTEGER PRIMARY KEY,
    thread_id       TEXT UNIQUE NOT NULL,
    subject         TEXT,
    email_count     INTEGER DEFAULT 0,
    first_date      TEXT,
    last_date       TEXT,
    participants    TEXT,
    summary         TEXT,
    summary_model   TEXT
);

CREATE TABLE IF NOT EXISTS projects (
    id              INTEGER PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    description     TEXT,
    department      TEXT,
    workstream      TEXT,
    created_at      TEXT,
    is_auto         INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS email_projects (
    email_id        INTEGER REFERENCES emails(id),
    project_id      INTEGER REFERENCES projects(id),
    confidence      REAL,
    assigned_by     TEXT,
    PRIMARY KEY (email_id, project_id)
);

CREATE TABLE IF NOT EXISTS companies (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    domain          TEXT UNIQUE NOT NULL,
    email_count     INTEGER DEFAULT 0,
    first_seen      TEXT,
    last_seen       TEXT,
    homepage_fetched_at TEXT,
    description     TEXT
);

CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);

CREATE TABLE IF NOT EXISTS company_contacts (
    company_id      INTEGER REFERENCES companies(id),
    contact_email   TEXT NOT NULL,
    PRIMARY KEY (company_id, contact_email)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              INTEGER PRIMARY KEY,
    stage           TEXT NOT NULL,
    email_id        INTEGER REFERENCES emails(id),
    status          TEXT NOT NULL,
    model_used      TEXT,
    started_at      TEXT,
    completed_at    TEXT,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_status ON pipeline_runs(stage, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_email_stage ON pipeline_runs(email_id, stage);

CREATE TABLE IF NOT EXISTS co_email_stats (
    email_a         TEXT NOT NULL,
    email_b         TEXT NOT NULL,
    co_email_count  INTEGER DEFAULT 0,
    first_co_email  TEXT,
    last_co_email   TEXT,
    PRIMARY KEY (email_a, email_b)
);

CREATE INDEX IF NOT EXISTS idx_co_email_a ON co_email_stats(email_a);
CREATE INDEX IF NOT EXISTS idx_co_email_b ON co_email_stats(email_b);

CREATE TABLE IF NOT EXISTS contact_memories (
    email           TEXT PRIMARY KEY,
    name            TEXT,
    relationship    TEXT,
    summary         TEXT,
    discussions     TEXT,
    key_facts       TEXT,
    model_used      TEXT,
    strategy_used   TEXT,
    version         INTEGER DEFAULT 1,
    generated_at    TEXT,
    emails_hash     TEXT
);

CREATE TABLE IF NOT EXISTS company_labels (
    company_id      INTEGER REFERENCES companies(id),
    label           TEXT NOT NULL,
    confidence      REAL,
    reasoning       TEXT,
    model_used      TEXT,
    assigned_at     TEXT,
    PRIMARY KEY (company_id, label)
);

CREATE INDEX IF NOT EXISTS idx_company_labels_label ON company_labels(label);

CREATE TABLE IF NOT EXISTS discussions (
    id              INTEGER PRIMARY KEY,
    title           TEXT NOT NULL,
    category        TEXT NOT NULL,
    current_state   TEXT,
    company_id      INTEGER REFERENCES companies(id),
    summary         TEXT,
    participants    TEXT,
    first_seen      TEXT,
    last_seen       TEXT,
    model_used      TEXT,
    updated_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_discussions_company ON discussions(company_id);
CREATE INDEX IF NOT EXISTS idx_discussions_category ON discussions(category);
CREATE INDEX IF NOT EXISTS idx_discussions_state ON discussions(current_state);

CREATE TABLE IF NOT EXISTS discussion_threads (
    discussion_id   INTEGER REFERENCES discussions(id),
    thread_id       TEXT NOT NULL,
    PRIMARY KEY (discussion_id, thread_id)
);

CREATE INDEX IF NOT EXISTS idx_discussion_threads_thread ON discussion_threads(thread_id);

CREATE TABLE IF NOT EXISTS discussion_state_history (
    id              INTEGER PRIMARY KEY,
    discussion_id   INTEGER REFERENCES discussions(id),
    state           TEXT NOT NULL,
    entered_at      TEXT,
    reasoning       TEXT,
    model_used      TEXT,
    detected_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_dsh_discussion ON discussion_state_history(discussion_id);
CREATE INDEX IF NOT EXISTS idx_dsh_state ON discussion_state_history(state);

CREATE TABLE IF NOT EXISTS actions (
    id              INTEGER PRIMARY KEY,
    discussion_id   INTEGER REFERENCES discussions(id),
    description     TEXT NOT NULL,
    assignee_emails TEXT,
    target_date     TEXT,
    status          TEXT DEFAULT 'open',
    source_date     TEXT,
    completed_date  TEXT,
    model_used      TEXT,
    detected_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_actions_discussion ON actions(discussion_id);
CREATE INDEX IF NOT EXISTS idx_actions_status ON actions(status);

CREATE TABLE IF NOT EXISTS calendar_events (
    id              INTEGER PRIMARY KEY,
    event_id        TEXT UNIQUE NOT NULL,
    calendar_id     TEXT NOT NULL DEFAULT 'primary',
    account_name    TEXT,
    title           TEXT,
    description     TEXT,
    location        TEXT,
    start_time      TEXT NOT NULL,
    end_time        TEXT NOT NULL,
    all_day         INTEGER DEFAULT 0,
    status          TEXT,
    organizer_email TEXT,
    attendees       TEXT,
    html_link       TEXT,
    recurring_event_id TEXT,
    created_at      TEXT,
    updated_at      TEXT,
    fetched_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_calendar_events_start ON calendar_events(start_time);
CREATE INDEX IF NOT EXISTS idx_calendar_events_end ON calendar_events(end_time);
CREATE INDEX IF NOT EXISTS idx_calendar_events_account ON calendar_events(account_name);

CREATE TABLE IF NOT EXISTS discussion_events (
    discussion_id   INTEGER REFERENCES discussions(id),
    event_id        INTEGER REFERENCES calendar_events(id),
    match_score     REAL,
    match_reason    TEXT,
    PRIMARY KEY (discussion_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_discussion_events_event ON discussion_events(event_id);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""


def get_db(config: Config) -> sqlite3.Connection:
    db_path = config.db_abs_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    _init_schema(conn)
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    # Track schema version
    row = conn.execute(
        "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
    ).fetchone()
    current_version = row[0] if row else 0
    if current_version == 0:
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        conn.commit()
    if current_version < 2:
        # Add gmail_id column if missing (migration v1 -> v2)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(emails)").fetchall()}
        if "gmail_id" not in cols:
            conn.execute("ALTER TABLE emails ADD COLUMN gmail_id TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_gmail_id ON emails(gmail_id)")
        conn.execute(
            "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        conn.commit()
    if current_version < 3:
        _migrate_to_v3(conn)
    if current_version < 4:
        _migrate_to_v4(conn)
    if current_version < 5:
        _migrate_to_v5(conn)
    if current_version < 6:
        _migrate_to_v6(conn)
    if current_version < 7:
        _migrate_to_v7(conn)
    if current_version < 8:
        _migrate_to_v8(conn)
    if current_version < 9:
        _migrate_to_v9(conn)
    if current_version < 10:
        _migrate_to_v10(conn)
    if current_version < 11:
        _migrate_to_v11(conn)
    if current_version < 12:
        _migrate_to_v12(conn)


def _migrate_to_v4(conn: sqlite3.Connection) -> None:
    """Migration v3 -> v4: add account_name column to emails."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(emails)").fetchall()}
    if "account_name" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN account_name TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_account ON emails(account_name)")

    # Backfill: emails with gmail_id came from a gmail account.
    # Match them to accounts via sync_state keys (gmail:<name>).
    gmail_states = conn.execute(
        "SELECT folder FROM sync_state WHERE folder LIKE 'gmail:%'"
    ).fetchall()
    if gmail_states:
        # If there's only one gmail account, assign all gmail emails to it
        gmail_names = [r[0].split(":", 1)[1] for r in gmail_states]
        if len(gmail_names) == 1:
            conn.execute(
                "UPDATE emails SET account_name = ? WHERE gmail_id IS NOT NULL AND account_name IS NULL",
                (gmail_names[0],),
            )
        # Multiple gmail accounts — can't reliably guess, leave NULL

    # Backfill IMAP: match by folder name to sync_state entries that aren't gmail
    imap_folders = conn.execute(
        "SELECT DISTINCT folder FROM sync_state WHERE folder NOT LIKE 'gmail:%'"
    ).fetchall()
    if imap_folders:
        # IMAP sync_state folder names are just the folder name, shared across accounts.
        # If there's only one non-gmail account configured, assign all non-gmail emails.
        # We can't do better without config access in a migration, so keep it simple.
        pass

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v4] account_name column added")


def _migrate_to_v3(conn: sqlite3.Connection) -> None:
    """Migration v2 -> v3: add normalised_subject, email_references table."""
    import json
    import re

    SUBJECT_PREFIX_RE = re.compile(
        r"^(\s*(Re|Fwd?|Fw)\s*(\[\d+\])?\s*:\s*)+", re.IGNORECASE
    )

    def _norm(subject: str | None) -> str:
        if not subject:
            return ""
        return SUBJECT_PREFIX_RE.sub("", subject).strip().lower()

    def _extract_ids(header_value: str) -> list[str]:
        if not header_value:
            return []
        return re.findall(r"<([^>]+)>", header_value)

    cols = {r[1] for r in conn.execute("PRAGMA table_info(emails)").fetchall()}
    if "normalised_subject" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN normalised_subject TEXT")

    conn.execute("""CREATE TABLE IF NOT EXISTS email_references (
        email_id        INTEGER NOT NULL REFERENCES emails(id),
        referenced_id   TEXT NOT NULL,
        PRIMARY KEY (email_id, referenced_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email_refs_referenced ON email_references(referenced_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_norm_subject ON emails(normalised_subject)")

    # Backfill normalised_subject in batches
    BATCH = 5000
    total = conn.execute("SELECT COUNT(*) FROM emails WHERE normalised_subject IS NULL").fetchone()[0]
    processed = 0
    while True:
        rows = conn.execute(
            "SELECT id, subject FROM emails WHERE normalised_subject IS NULL LIMIT ?",
            (BATCH,),
        ).fetchall()
        if not rows:
            break
        conn.executemany(
            "UPDATE emails SET normalised_subject = ? WHERE id = ?",
            [(_norm(r[1]), r[0]) for r in rows],
        )
        conn.commit()
        processed += len(rows)
        if total > 0:
            print(f"  [migration v3] normalised_subject: {processed}/{total}")

    # Backfill email_references
    total = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    existing_refs = conn.execute("SELECT COUNT(*) FROM email_references").fetchone()[0]
    if existing_refs == 0 and total > 0:
        processed = 0
        offset = 0
        while True:
            rows = conn.execute(
                "SELECT id, raw_headers FROM emails ORDER BY id LIMIT ? OFFSET ?",
                (BATCH, offset),
            ).fetchall()
            if not rows:
                break
            ref_rows = []
            for r in rows:
                headers = json.loads(r[1]) if r[1] else {}
                refs = _extract_ids(headers.get("references", ""))
                in_reply = _extract_ids(headers.get("in_reply_to", ""))
                seen = set()
                for ref_id in refs + in_reply:
                    if ref_id not in seen:
                        ref_rows.append((r[0], ref_id))
                        seen.add(ref_id)
            if ref_rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO email_references (email_id, referenced_id) VALUES (?, ?)",
                    ref_rows,
                )
            conn.commit()
            processed += len(rows)
            offset += BATCH
            print(f"  [migration v3] email_references: {processed}/{total}")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v3] complete")


def _migrate_to_v5(conn: sqlite3.Connection) -> None:
    """Migration v4 -> v5: replace entities table with companies + company_contacts."""
    conn.execute("DROP TABLE IF EXISTS entities")
    conn.execute("""CREATE TABLE IF NOT EXISTS companies (
        id              INTEGER PRIMARY KEY,
        name            TEXT NOT NULL,
        domain          TEXT UNIQUE NOT NULL,
        email_count     INTEGER DEFAULT 0,
        first_seen      TEXT,
        last_seen       TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name)")
    conn.execute("""CREATE TABLE IF NOT EXISTS company_contacts (
        company_id      INTEGER REFERENCES companies(id),
        contact_email   TEXT NOT NULL,
        PRIMARY KEY (company_id, contact_email)
    )""")
    # Clear extract_base pipeline runs so companies get rebuilt
    conn.execute("DELETE FROM pipeline_runs WHERE stage = 'extract_base'")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v5] entities table replaced with companies + company_contacts")


def _migrate_to_v6(conn: sqlite3.Connection) -> None:
    """Migration v5 -> v6: add homepage_fetched_at to companies, add company_labels table."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(companies)").fetchall()}
    if "homepage_fetched_at" not in cols:
        conn.execute("ALTER TABLE companies ADD COLUMN homepage_fetched_at TEXT")

    conn.execute("""CREATE TABLE IF NOT EXISTS company_labels (
        company_id      INTEGER REFERENCES companies(id),
        label           TEXT NOT NULL,
        confidence      REAL,
        reasoning       TEXT,
        model_used      TEXT,
        assigned_at     TEXT,
        PRIMARY KEY (company_id, label)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_company_labels_label ON company_labels(label)")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v6] homepage columns and company_labels table added")


def _migrate_to_v7(conn: sqlite3.Connection) -> None:
    """Migration v6 -> v7: add description column to companies."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(companies)").fetchall()}
    if "description" not in cols:
        conn.execute("ALTER TABLE companies ADD COLUMN description TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v7] description column added to companies")


def _migrate_to_v8(conn: sqlite3.Connection) -> None:
    """Migration v7 -> v8: add discussions, discussion_threads, discussion_state_history tables."""
    conn.execute("""CREATE TABLE IF NOT EXISTS discussions (
        id              INTEGER PRIMARY KEY,
        title           TEXT NOT NULL,
        category        TEXT NOT NULL,
        current_state   TEXT,
        company_id      INTEGER REFERENCES companies(id),
        summary         TEXT,
        participants    TEXT,
        first_seen      TEXT,
        last_seen       TEXT,
        model_used      TEXT,
        updated_at      TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discussions_company ON discussions(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discussions_category ON discussions(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discussions_state ON discussions(current_state)")

    conn.execute("""CREATE TABLE IF NOT EXISTS discussion_threads (
        discussion_id   INTEGER REFERENCES discussions(id),
        thread_id       TEXT NOT NULL,
        PRIMARY KEY (discussion_id, thread_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discussion_threads_thread ON discussion_threads(thread_id)")

    conn.execute("""CREATE TABLE IF NOT EXISTS discussion_state_history (
        id              INTEGER PRIMARY KEY,
        discussion_id   INTEGER REFERENCES discussions(id),
        state           TEXT NOT NULL,
        entered_at      TEXT,
        reasoning       TEXT,
        model_used      TEXT,
        detected_at     TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dsh_discussion ON discussion_state_history(discussion_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dsh_state ON discussion_state_history(state)")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v8] discussions tables added")


def _migrate_to_v9(conn: sqlite3.Connection) -> None:
    """Migration v8 -> v9: add actions table."""
    conn.execute("""CREATE TABLE IF NOT EXISTS actions (
        id              INTEGER PRIMARY KEY,
        discussion_id   INTEGER REFERENCES discussions(id),
        description     TEXT NOT NULL,
        assignee_emails TEXT,
        target_date     TEXT,
        status          TEXT DEFAULT 'open',
        source_date     TEXT,
        completed_date  TEXT,
        model_used      TEXT,
        detected_at     TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_actions_discussion ON actions(discussion_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_actions_status ON actions(status)")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v9] actions table added")


def _migrate_to_v10(conn: sqlite3.Connection) -> None:
    """Migration v9 -> v10: add completed_date column to actions."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(actions)").fetchall()}
    if "completed_date" not in cols:
        conn.execute("ALTER TABLE actions ADD COLUMN completed_date TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v10] completed_date column added to actions")


def _migrate_to_v11(conn: sqlite3.Connection) -> None:
    """Migration v10 -> v11: rename assignee_email to assignee_emails (JSON array)."""
    import json as _json

    cols = {r[1] for r in conn.execute("PRAGMA table_info(actions)").fetchall()}
    if "assignee_email" in cols and "assignee_emails" not in cols:
        conn.execute("ALTER TABLE actions ADD COLUMN assignee_emails TEXT")
        # Migrate existing data: wrap single email in a JSON array
        rows = conn.execute("SELECT id, assignee_email FROM actions WHERE assignee_email IS NOT NULL AND assignee_email != ''").fetchall()
        for r in rows:
            conn.execute(
                "UPDATE actions SET assignee_emails = ? WHERE id = ?",
                (_json.dumps([r[1]]), r[0]),
            )
    conn.execute("DROP INDEX IF EXISTS idx_actions_assignee")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v11] assignee_email migrated to assignee_emails (JSON array)")


def _migrate_to_v12(conn: sqlite3.Connection) -> None:
    """Migration v11 -> v12: add calendar_events and discussion_events tables."""
    conn.execute("""CREATE TABLE IF NOT EXISTS calendar_events (
        id              INTEGER PRIMARY KEY,
        event_id        TEXT UNIQUE NOT NULL,
        calendar_id     TEXT NOT NULL DEFAULT 'primary',
        account_name    TEXT,
        title           TEXT,
        description     TEXT,
        location        TEXT,
        start_time      TEXT NOT NULL,
        end_time        TEXT NOT NULL,
        all_day         INTEGER DEFAULT 0,
        status          TEXT,
        organizer_email TEXT,
        attendees       TEXT,
        html_link       TEXT,
        recurring_event_id TEXT,
        created_at      TEXT,
        updated_at      TEXT,
        fetched_at      TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_calendar_events_start ON calendar_events(start_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_calendar_events_end ON calendar_events(end_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_calendar_events_account ON calendar_events(account_name)")

    conn.execute("""CREATE TABLE IF NOT EXISTS discussion_events (
        discussion_id   INTEGER REFERENCES discussions(id),
        event_id        INTEGER REFERENCES calendar_events(id),
        match_score     REAL,
        match_reason    TEXT,
        PRIMARY KEY (discussion_id, event_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discussion_events_event ON discussion_events(event_id)")

    # Add sync_token column to sync_state for calendar sync tokens (strings)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sync_state)").fetchall()}
    if "sync_token" not in cols:
        conn.execute("ALTER TABLE sync_state ADD COLUMN sync_token TEXT")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    print("  [migration v12] calendar_events and discussion_events tables added")


def execute(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Cursor:
    return conn.execute(sql, params)


def fetchall(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[Any]:
    return conn.execute(sql, params).fetchall()


def fetchone(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any | None:
    return conn.execute(sql, params).fetchone()
