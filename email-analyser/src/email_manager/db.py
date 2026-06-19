from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

from email_manager.config import Config


def _log(msg: str) -> None:
    """Print migration/schema messages to stderr so they don't pollute stdout (e.g. --csv)."""
    print(msg, file=sys.stderr)

SCHEMA_VERSION = 39

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
    account_name    TEXT,
    from_domain     TEXT
);

CREATE INDEX IF NOT EXISTS idx_emails_thread ON emails(thread_id);
CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date);
CREATE INDEX IF NOT EXISTS idx_emails_from ON emails(from_address);
CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(message_id);
CREATE INDEX IF NOT EXISTS idx_emails_from_domain ON emails(from_domain);

CREATE TABLE IF NOT EXISTS sync_state (
    folder          TEXT PRIMARY KEY,
    uidvalidity     INTEGER NOT NULL,
    last_uid        INTEGER NOT NULL DEFAULT 0,
    last_sync       TEXT NOT NULL,
    sync_token      TEXT
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

CREATE TABLE IF NOT EXISTS email_references (
    email_id        TEXT NOT NULL,
    referenced_id   TEXT NOT NULL,
    UNIQUE(email_id, referenced_id)
);

CREATE INDEX IF NOT EXISTS idx_email_refs_referenced ON email_references(referenced_id);

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

CREATE TABLE IF NOT EXISTS processing_runs (
    id              INTEGER PRIMARY KEY,
    company_domain  TEXT NOT NULL,
    mode            TEXT NOT NULL,
    model           TEXT,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    events_created  INTEGER DEFAULT 0,
    discussions_created INTEGER DEFAULT 0,
    discussions_updated INTEGER DEFAULT 0,
    actions_proposed INTEGER DEFAULT 0,
    input_tokens    INTEGER DEFAULT 0,
    output_tokens   INTEGER DEFAULT 0,
    llm_calls       INTEGER DEFAULT 0,
    proposed_changes_json TEXT,
    parent_run_id   INTEGER REFERENCES processing_runs(id),
    email_cutoff_date TEXT,
    prompt_hash     TEXT,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_processing_runs_company ON processing_runs(company_domain);
CREATE INDEX IF NOT EXISTS idx_processing_runs_mode ON processing_runs(mode);

CREATE TABLE IF NOT EXISTS llm_calls (
    id              INTEGER PRIMARY KEY,
    run_id          INTEGER REFERENCES processing_runs(id),
    stage           TEXT NOT NULL,
    model           TEXT,
    input_tokens    INTEGER DEFAULT 0,
    output_tokens   INTEGER DEFAULT 0,
    duration_ms     INTEGER,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON llm_calls(run_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_stage ON llm_calls(stage);

CREATE TABLE IF NOT EXISTS discussions (
    id              INTEGER PRIMARY KEY,
    title           TEXT NOT NULL,
    category        TEXT NOT NULL,
    current_state   TEXT,
    company_id      INTEGER REFERENCES companies(id),
    parent_id       INTEGER REFERENCES discussions(id),
    run_id          INTEGER REFERENCES processing_runs(id),
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
-- idx_discussions_parent created by _migrate_to_v15

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

CREATE TABLE IF NOT EXISTS event_ledger (
    id              TEXT PRIMARY KEY,
    thread_id       TEXT,
    source_email_id TEXT,
    source_calendar_event_id TEXT,
    source_type     TEXT NOT NULL DEFAULT 'email',
    source_id       TEXT,
    run_id          INTEGER REFERENCES processing_runs(id),
    discussion_id   INTEGER REFERENCES discussions(id),
    domain          TEXT NOT NULL,
    type            TEXT NOT NULL,
    actor           TEXT,
    target          TEXT,
    event_date      TEXT,
    detail          TEXT,
    confidence      REAL,
    model_version   TEXT,
    prompt_version  TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_ledger_thread ON event_ledger(thread_id);
CREATE INDEX IF NOT EXISTS idx_event_ledger_discussion ON event_ledger(discussion_id);
CREATE INDEX IF NOT EXISTS idx_event_ledger_domain ON event_ledger(domain);
CREATE INDEX IF NOT EXISTS idx_event_ledger_type ON event_ledger(type);
CREATE INDEX IF NOT EXISTS idx_event_ledger_date ON event_ledger(event_date);

CREATE TABLE IF NOT EXISTS milestones (
    id              INTEGER PRIMARY KEY,
    discussion_id   INTEGER REFERENCES discussions(id),
    run_id          INTEGER REFERENCES processing_runs(id),
    name            TEXT NOT NULL,
    achieved        INTEGER DEFAULT 0,
    achieved_date   TEXT,
    evidence_event_ids TEXT,
    confidence      REAL,
    last_evaluated_at TEXT,
    UNIQUE(discussion_id, name)
);

CREATE INDEX IF NOT EXISTS idx_milestones_discussion ON milestones(discussion_id);

CREATE TABLE IF NOT EXISTS feedback (
    id              INTEGER PRIMARY KEY,
    layer           TEXT NOT NULL,
    target_type     TEXT NOT NULL,
    target_id       TEXT NOT NULL,
    action          TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    reason          TEXT,
    applied         INTEGER DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_feedback_layer ON feedback(layer);
CREATE INDEX IF NOT EXISTS idx_feedback_target ON feedback(target_type, target_id);

CREATE TABLE IF NOT EXISTS few_shot_examples (
    id              INTEGER PRIMARY KEY,
    layer           TEXT NOT NULL,
    category        TEXT,
    input_text      TEXT NOT NULL,
    wrong_output    TEXT,
    correct_output  TEXT NOT NULL,
    source_feedback_ids TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS learned_rules (
    id              INTEGER PRIMARY KEY,
    layer           TEXT NOT NULL,
    category        TEXT,
    rule_text       TEXT NOT NULL,
    source_feedback_ids TEXT,
    active          INTEGER DEFAULT 1,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS proposed_actions (
    id              INTEGER PRIMARY KEY,
    discussion_id   INTEGER REFERENCES discussions(id),
    run_id          INTEGER REFERENCES processing_runs(id),
    action          TEXT NOT NULL,
    reasoning       TEXT,
    priority        TEXT,
    wait_until      TEXT,
    assignee        TEXT,
    model_used      TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_proposed_actions_discussion ON proposed_actions(discussion_id);

CREATE TABLE IF NOT EXISTS change_journal (
    id              INTEGER PRIMARY KEY,
    entity_type     TEXT NOT NULL,
    entity_id       TEXT NOT NULL,
    change_type     TEXT NOT NULL,
    source_stage    TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    processed_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_change_journal_entity ON change_journal(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_change_journal_unprocessed ON change_journal(processed_at) WHERE processed_at IS NULL;

CREATE TABLE IF NOT EXISTS hubspot_companies (
    id              TEXT PRIMARY KEY,
    name            TEXT,
    domain          TEXT,
    website         TEXT,
    industry        TEXT,
    description     TEXT,
    about_us        TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    phone           TEXT,
    num_employees   INTEGER,
    annual_revenue  REAL,
    lifecycle_stage TEXT,
    type            TEXT,
    owner_id        TEXT,
    founded_year    TEXT,
    linkedin_url    TEXT,
    twitter_handle  TEXT,
    hs_created_at   TEXT,
    hs_updated_at   TEXT,
    hs_url          TEXT,
    properties_json TEXT,
    fetched_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hubspot_companies_domain ON hubspot_companies(domain);
CREATE INDEX IF NOT EXISTS idx_hubspot_companies_updated ON hubspot_companies(hs_updated_at);

CREATE TABLE IF NOT EXISTS hubspot_contacts (
    id              TEXT PRIMARY KEY,
    email           TEXT,
    firstname       TEXT,
    lastname        TEXT,
    company_name    TEXT,
    job_title       TEXT,
    phone           TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    address         TEXT,
    lifecycle_stage TEXT,
    lead_status     TEXT,
    owner_id        TEXT,
    twitter_handle  TEXT,
    linkedin_url    TEXT,
    website         TEXT,
    industry        TEXT,
    salutation      TEXT,
    hs_created_at   TEXT,
    hs_updated_at   TEXT,
    hs_url          TEXT,
    properties_json TEXT,
    fetched_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_email ON hubspot_contacts(email);
CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_updated ON hubspot_contacts(hs_updated_at);

CREATE TABLE IF NOT EXISTS hubspot_company_contacts (
    company_id      TEXT NOT NULL,
    contact_id      TEXT NOT NULL,
    PRIMARY KEY (company_id, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_hubspot_cc_contact ON hubspot_company_contacts(contact_id);

CREATE TABLE IF NOT EXISTS hubspot_sync_state (
    object_type     TEXT PRIMARY KEY,
    last_sync_at    TEXT NOT NULL,
    last_full_sync_at TEXT,
    record_count    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS hubspot_tasks (
    id                      TEXT PRIMARY KEY,
    subject                 TEXT,
    body                    TEXT,
    status                  TEXT,
    type                    TEXT,
    priority                TEXT,
    due_date                TEXT,
    completed_at            TEXT,
    owner_id                TEXT,
    associated_contact_ids  TEXT,
    associated_company_ids  TEXT,
    hs_url                  TEXT,
    properties_json         TEXT,
    fetched_at              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_status ON hubspot_tasks(status);
CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_owner ON hubspot_tasks(owner_id);
CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_due ON hubspot_tasks(due_date);

CREATE TABLE IF NOT EXISTS organizations (
    id              INTEGER PRIMARY KEY,
    canonical_name  TEXT,
    canonical_domain TEXT,
    notes           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_organizations_domain ON organizations(canonical_domain);

CREATE TABLE IF NOT EXISTS people (
    id              INTEGER PRIMARY KEY,
    canonical_name  TEXT,
    canonical_email TEXT,
    notes           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_people_email ON people(canonical_email);

CREATE TABLE IF NOT EXISTS organization_identities (
    organization_id INTEGER NOT NULL REFERENCES organizations(id),
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    match_key       TEXT,
    confidence      REAL DEFAULT 1.0,
    is_manual       INTEGER DEFAULT 0,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_org_identities_org ON organization_identities(organization_id);
CREATE INDEX IF NOT EXISTS idx_org_identities_match ON organization_identities(match_key);

CREATE TABLE IF NOT EXISTS person_identities (
    person_id       INTEGER NOT NULL REFERENCES people(id),
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    match_key       TEXT,
    confidence      REAL DEFAULT 1.0,
    is_manual       INTEGER DEFAULT 0,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_person_identities_person ON person_identities(person_id);
CREATE INDEX IF NOT EXISTS idx_person_identities_match ON person_identities(match_key);

CREATE TABLE IF NOT EXISTS field_overrides (
    entity_type     TEXT NOT NULL,
    entity_id       INTEGER NOT NULL,
    field           TEXT NOT NULL,
    value           TEXT,
    set_by          TEXT,
    set_at          TEXT NOT NULL,
    PRIMARY KEY (entity_type, entity_id, field)
);

CREATE TABLE IF NOT EXISTS entity_merges (
    id              INTEGER PRIMARY KEY,
    entity_type     TEXT NOT NULL,
    source_id       INTEGER NOT NULL,
    target_id       INTEGER NOT NULL,
    performed_at    TEXT NOT NULL,
    performed_by    TEXT,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_entity_merges_target ON entity_merges(entity_type, target_id);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""


def _get_column_names(conn: Any, table: str) -> set[str]:
    """Get column names for a table, works with both SQLite and PostgreSQL."""
    try:
        # SQLite
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r[1] for r in rows}
    except Exception:
        # PostgreSQL
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table,),
        ).fetchall()
        return {r[0] for r in rows}


def get_db(config: Config) -> sqlite3.Connection:
    """Get a database connection based on config.

    Returns either a sqlite3.Connection or a PostgresConnection wrapper
    (which has the same interface).

    Auto-detects postgres if DB_URL is set to a postgresql:// URL.
    """
    pg_url = config.db_url or config.postgres_url
    use_postgres = config.db_backend == "postgres"

    if use_postgres:
        if not pg_url:
            raise ValueError("DB_URL must be set when using postgres")
        from email_manager.db_postgres import get_postgres_connection
        conn = get_postgres_connection(pg_url)
        _init_schema(conn)
        return conn  # type: ignore[return-value]

    # Default: SQLite
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
        cols = _get_column_names(conn, "emails")
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
    if current_version < 13:
        _migrate_to_v13(conn)
    if current_version < 14:
        _migrate_to_v14(conn)
    if current_version < 15:
        _migrate_to_v15(conn)
    if current_version < 16:
        _migrate_to_v16(conn)
    if current_version < 17:
        _migrate_to_v17(conn)
    if current_version < 18:
        _migrate_to_v18(conn)
    if current_version < 19:
        _migrate_to_v19(conn)
    if current_version < 20:
        _migrate_to_v20(conn)
    if current_version < 21:
        _migrate_to_v21(conn)
    if current_version < 22:
        _migrate_to_v22(conn)
    if current_version < 23:
        _migrate_to_v23(conn)
    if current_version < 24:
        _migrate_to_v24(conn)
    if current_version < 25:
        _migrate_to_v25(conn)
    if current_version < 26:
        _migrate_to_v26(conn)
    if current_version < 27:
        _migrate_to_v27(conn)
    if current_version < 28:
        _migrate_to_v28(conn)
    if current_version < 29:
        _migrate_to_v29(conn)
    if current_version < 30:
        _migrate_to_v30(conn)
    if current_version < 31:
        _migrate_to_v31(conn)
    if current_version < 32:
        _migrate_to_v32(conn)
    if current_version < 33:
        _migrate_to_v33(conn)
    if current_version < 34:
        _migrate_to_v34(conn)
    if current_version < 35:
        _migrate_to_v35(conn)
    if current_version < 36:
        _migrate_to_v36(conn)
    if current_version < 37:
        _migrate_to_v37(conn)
    if current_version < 38:
        _migrate_to_v38(conn)
    if current_version < 39:
        _migrate_to_v39(conn)


def _migrate_to_v39(conn: sqlite3.Connection) -> None:
    """Migration v38 -> v39: add human_state_override to discussions."""
    try:
        conn.execute("ALTER TABLE discussions ADD COLUMN human_state_override TEXT")
    except Exception:
        pass  # column may already exist
    conn.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    conn.commit()


def _migrate_to_v38(conn: sqlite3.Connection) -> None:
    """Migration v37 -> v38: add hubspot_notes and discussion_notes tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hubspot_notes (
            id                      TEXT PRIMARY KEY,
            body                    TEXT,
            created_at              TEXT,
            updated_at              TEXT,
            owner_id                TEXT,
            associated_contact_ids  TEXT,
            associated_company_ids  TEXT,
            associated_deal_ids     TEXT,
            hs_url                  TEXT,
            properties_json         TEXT,
            fetched_at              TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_notes_created ON hubspot_notes(created_at);
        CREATE INDEX IF NOT EXISTS idx_hubspot_notes_owner   ON hubspot_notes(owner_id);

        CREATE TABLE IF NOT EXISTS discussion_notes (
            discussion_id  INTEGER NOT NULL REFERENCES discussions(id),
            note_id        TEXT NOT NULL,
            PRIMARY KEY (discussion_id, note_id)
        );
        CREATE INDEX IF NOT EXISTS idx_discussion_notes_note ON discussion_notes(note_id);
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v38] hubspot_notes and discussion_notes tables added")


def _migrate_to_v37(conn: sqlite3.Connection) -> None:
    """Migration v36 -> v37: add HubSpot email-deal association tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hubspot_email_id_map (
            hs_id      TEXT PRIMARY KEY,
            message_id TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_email_id_map_msg ON hubspot_email_id_map(message_id);

        CREATE TABLE IF NOT EXISTS hubspot_email_deal_links (
            hs_id   TEXT NOT NULL,
            deal_id TEXT NOT NULL,
            PRIMARY KEY (hs_id, deal_id)
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_edl_deal ON hubspot_email_deal_links(deal_id);
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v37] hubspot_email_id_map and hubspot_email_deal_links tables added")


def _migrate_to_v36(conn: sqlite3.Connection) -> None:
    """Migration v35 -> v36: add hubspot_deals table and source_type/source_id to discussions."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hubspot_deals (
            id              TEXT PRIMARY KEY,
            name            TEXT,
            stage           TEXT,
            pipeline        TEXT,
            amount          REAL,
            close_date      TEXT,
            hs_created_at   TEXT,
            hs_updated_at   TEXT,
            hs_url          TEXT,
            properties_json TEXT,
            fetched_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_deals_stage ON hubspot_deals(stage);
        CREATE INDEX IF NOT EXISTS idx_hubspot_deals_updated ON hubspot_deals(hs_updated_at);

        CREATE TABLE IF NOT EXISTS hubspot_deal_companies (
            deal_id         TEXT NOT NULL REFERENCES hubspot_deals(id),
            company_id      TEXT NOT NULL,
            PRIMARY KEY (deal_id, company_id)
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_dc_company ON hubspot_deal_companies(company_id);
    """)
    # Add source_type / source_id to discussions for tracking deal-sourced rows
    cols = _get_column_names(conn, "discussions")
    if "source_type" not in cols:
        conn.execute("ALTER TABLE discussions ADD COLUMN source_type TEXT")
    if "source_id" not in cols:
        conn.execute("ALTER TABLE discussions ADD COLUMN source_id TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v36] hubspot_deals table added; source_type/source_id added to discussions")


def _migrate_to_v35(conn: sqlite3.Connection) -> None:
    """Migration v34 -> v35: add hubspot_task_threads table for task-email enrichment."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hubspot_task_threads (
            task_id         TEXT NOT NULL,
            thread_id       TEXT NOT NULL,
            contact_email   TEXT NOT NULL,
            PRIMARY KEY (task_id, thread_id)
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_task_threads_task   ON hubspot_task_threads(task_id);
        CREATE INDEX IF NOT EXISTS idx_hubspot_task_threads_thread ON hubspot_task_threads(thread_id);
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v35] hubspot_task_threads table added")


def _migrate_to_v34(conn: sqlite3.Connection) -> None:
    """Migration v33 -> v34: add hubspot_tasks table for CRM task sync."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hubspot_tasks (
            id                      TEXT PRIMARY KEY,
            subject                 TEXT,
            body                    TEXT,
            status                  TEXT,
            type                    TEXT,
            priority                TEXT,
            due_date                TEXT,
            completed_at            TEXT,
            owner_id                TEXT,
            associated_contact_ids  TEXT,
            associated_company_ids  TEXT,
            hs_url                  TEXT,
            properties_json         TEXT,
            fetched_at              TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_status ON hubspot_tasks(status);
        CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_owner ON hubspot_tasks(owner_id);
        CREATE INDEX IF NOT EXISTS idx_hubspot_tasks_due ON hubspot_tasks(due_date);
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v34] hubspot_tasks table added")


def _migrate_to_v33(conn: sqlite3.Connection) -> None:
    """Migration v32 -> v33: add from_domain column + index.

    from_domain stores the part of from_address after '@', enabling fast equality
    lookups by sender domain instead of a full-table LIKE '%@domain%' scan.
    to_addresses/cc_addresses are not indexed: they are multi-value text fields
    that exceed Postgres's B-tree size limit and cannot benefit from LIKE anyway.
    """
    cols = _get_column_names(conn, "emails")
    if "from_domain" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN from_domain TEXT")
        conn.commit()
        conn.execute("""
            UPDATE emails
            SET from_domain = LOWER(SUBSTR(from_address, INSTR(from_address, '@') + 1))
            WHERE from_address LIKE '%@%'
        """)
        conn.commit()
    conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_from_domain ON emails(from_domain)")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v33] from_domain column + email address indexes added")


def _migrate_to_v32(conn: sqlite3.Connection) -> None:
    """Migration v31 -> v32: add hs_url to hubspot_companies and hubspot_contacts.

    HubSpot returns a per-record `url` field on each object. We now store it
    so the UI can deep-link to the right portal. New columns only — sync
    populates them on next run; existing rows are backfilled lazily by
    `email_manager.integrations.hubspot.backfill_urls`, called automatically
    from `hubspot sync`.
    """
    # NB: each iteration of `_get_column_names` does a `PRAGMA table_info`
    # which raises on Postgres; PostgresConnection.execute rolls back on
    # exception, killing any pending DDL. Commit between iterations so the
    # first ALTER is durable before the second PRAGMA probes.
    for table in ("hubspot_companies", "hubspot_contacts"):
        cols = _get_column_names(conn, table)
        if "hs_url" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN hs_url TEXT")
            conn.commit()
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v32] hs_url columns added to hubspot_companies and hubspot_contacts")


def _migrate_to_v31(conn: sqlite3.Connection) -> None:
    """Migration v30 -> v31: add unified entity model.

    Introduces `organizations` and `people` as abstract entity tables, plus
    `organization_identities` and `person_identities` linking each entity to
    one or more source rows (email, homepage, hubspot, future linkedin, ...).
    `field_overrides` lets users hand-edit any field on the unified view;
    `entity_merges` records manual merges for audit/undo.

    Source tables (`companies`, `contacts`, `hubspot_companies`, etc.) are
    left untouched — the unified view is computed from identities + overrides.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS organizations (
            id              INTEGER PRIMARY KEY,
            canonical_name  TEXT,
            canonical_domain TEXT,
            notes           TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_organizations_domain ON organizations(canonical_domain);

        CREATE TABLE IF NOT EXISTS people (
            id              INTEGER PRIMARY KEY,
            canonical_name  TEXT,
            canonical_email TEXT,
            notes           TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_people_email ON people(canonical_email);

        CREATE TABLE IF NOT EXISTS organization_identities (
            organization_id INTEGER NOT NULL REFERENCES organizations(id),
            source          TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            match_key       TEXT,
            confidence      REAL DEFAULT 1.0,
            is_manual       INTEGER DEFAULT 0,
            created_at      TEXT NOT NULL,
            PRIMARY KEY (source, source_id)
        );
        CREATE INDEX IF NOT EXISTS idx_org_identities_org ON organization_identities(organization_id);
        CREATE INDEX IF NOT EXISTS idx_org_identities_match ON organization_identities(match_key);

        CREATE TABLE IF NOT EXISTS person_identities (
            person_id       INTEGER NOT NULL REFERENCES people(id),
            source          TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            match_key       TEXT,
            confidence      REAL DEFAULT 1.0,
            is_manual       INTEGER DEFAULT 0,
            created_at      TEXT NOT NULL,
            PRIMARY KEY (source, source_id)
        );
        CREATE INDEX IF NOT EXISTS idx_person_identities_person ON person_identities(person_id);
        CREATE INDEX IF NOT EXISTS idx_person_identities_match ON person_identities(match_key);

        CREATE TABLE IF NOT EXISTS field_overrides (
            entity_type     TEXT NOT NULL,
            entity_id       INTEGER NOT NULL,
            field           TEXT NOT NULL,
            value           TEXT,
            set_by          TEXT,
            set_at          TEXT NOT NULL,
            PRIMARY KEY (entity_type, entity_id, field)
        );

        CREATE TABLE IF NOT EXISTS entity_merges (
            id              INTEGER PRIMARY KEY,
            entity_type     TEXT NOT NULL,
            source_id       INTEGER NOT NULL,
            target_id       INTEGER NOT NULL,
            performed_at    TEXT NOT NULL,
            performed_by    TEXT,
            notes           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_entity_merges_target ON entity_merges(entity_type, target_id);
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v31] entity model: organizations, people, *_identities, field_overrides, entity_merges")


def _migrate_to_v30(conn: sqlite3.Connection) -> None:
    """Migration v29 -> v30: add hubspot_* tables for CRM ingestion.

    Companies and contacts from HubSpot land in dedicated tables (rather than
    merging into the email-derived companies/contacts) so each source keeps
    its own provenance and field semantics. Domain/email columns indexed for
    later cross-source joins.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hubspot_companies (
            id              TEXT PRIMARY KEY,
            name            TEXT,
            domain          TEXT,
            website         TEXT,
            industry        TEXT,
            description     TEXT,
            about_us        TEXT,
            city            TEXT,
            state           TEXT,
            country         TEXT,
            phone           TEXT,
            num_employees   INTEGER,
            annual_revenue  REAL,
            lifecycle_stage TEXT,
            type            TEXT,
            owner_id        TEXT,
            founded_year    TEXT,
            linkedin_url    TEXT,
            twitter_handle  TEXT,
            hs_created_at   TEXT,
            hs_updated_at   TEXT,
            properties_json TEXT,
            fetched_at      TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_companies_domain ON hubspot_companies(domain);
        CREATE INDEX IF NOT EXISTS idx_hubspot_companies_updated ON hubspot_companies(hs_updated_at);

        CREATE TABLE IF NOT EXISTS hubspot_contacts (
            id              TEXT PRIMARY KEY,
            email           TEXT,
            firstname       TEXT,
            lastname        TEXT,
            company_name    TEXT,
            job_title       TEXT,
            phone           TEXT,
            city            TEXT,
            state           TEXT,
            country         TEXT,
            address         TEXT,
            lifecycle_stage TEXT,
            lead_status     TEXT,
            owner_id        TEXT,
            twitter_handle  TEXT,
            linkedin_url    TEXT,
            website         TEXT,
            industry        TEXT,
            salutation      TEXT,
            hs_created_at   TEXT,
            hs_updated_at   TEXT,
            properties_json TEXT,
            fetched_at      TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_email ON hubspot_contacts(email);
        CREATE INDEX IF NOT EXISTS idx_hubspot_contacts_updated ON hubspot_contacts(hs_updated_at);

        CREATE TABLE IF NOT EXISTS hubspot_company_contacts (
            company_id      TEXT NOT NULL,
            contact_id      TEXT NOT NULL,
            PRIMARY KEY (company_id, contact_id)
        );
        CREATE INDEX IF NOT EXISTS idx_hubspot_cc_contact ON hubspot_company_contacts(contact_id);

        CREATE TABLE IF NOT EXISTS hubspot_sync_state (
            object_type     TEXT PRIMARY KEY,
            last_sync_at    TEXT NOT NULL,
            last_full_sync_at TEXT,
            record_count    INTEGER DEFAULT 0
        );
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v30] hubspot_companies, hubspot_contacts, hubspot_company_contacts, hubspot_sync_state tables created")


def _migrate_to_v29(conn: sqlite3.Connection) -> None:
    """Migration v28 -> v29: add discussion_search_docs and discussion_embeddings.

    Discussions become a first-class search primitive, separate from threads. This
    means (a) thread embeddings no longer invalidate when a discussion summary
    evolves, (b) discussions have their own retrieval path, (c) discussion
    title/summary are removed from thread doc_text (invalidating existing thread
    hashes — re-embed required on next build_search_index).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS discussion_search_docs (
            discussion_id   INTEGER PRIMARY KEY,
            doc_text        TEXT NOT NULL,
            doc_hash        TEXT,
            company_domain  TEXT,
            category        TEXT,
            current_state   TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dsd_company ON discussion_search_docs(company_domain)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dsd_category ON discussion_search_docs(category)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS discussion_embeddings (
            discussion_id   INTEGER NOT NULL,
            model_name      TEXT NOT NULL,
            doc_hash        TEXT,
            created_at      TEXT NOT NULL,
            PRIMARY KEY (discussion_id, model_name)
        )
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v29] discussion_search_docs and discussion_embeddings tables created")


def _migrate_to_v28(conn: sqlite3.Connection) -> None:
    """Migration v27 -> v28: add doc_hash to thread_embeddings for freshness tracking.

    Backfills existing rows from thread_search_docs so the existing embeddings
    are treated as up to date. Rows added after migration (e.g. by an in-flight
    embedding run using pre-migration code) will have NULL doc_hash and must be
    re-backfilled with the same UPDATE afterwards.
    """
    cols = _get_column_names(conn, "thread_embeddings")
    if "doc_hash" not in cols:
        conn.execute("ALTER TABLE thread_embeddings ADD COLUMN doc_hash TEXT")
    conn.execute(
        """UPDATE thread_embeddings SET doc_hash = (
               SELECT tsd.doc_hash FROM thread_search_docs tsd
               WHERE tsd.thread_id = thread_embeddings.thread_id
           )
           WHERE doc_hash IS NULL"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v28] doc_hash column added to thread_embeddings; backfilled from thread_search_docs")


def _migrate_to_v27(conn: sqlite3.Connection) -> None:
    """Migration v26 -> v27: add outreach_score to thread_search_docs for personal-frequency ranking."""
    cols = _get_column_names(conn, "thread_search_docs")
    if "outreach_score" not in cols:
        conn.execute("ALTER TABLE thread_search_docs ADD COLUMN outreach_score REAL DEFAULT 0")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v27] outreach_score column added to thread_search_docs")


def _migrate_to_v26(conn: sqlite3.Connection) -> None:
    """Migration v25 -> v26: add search index and embedding tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS thread_search_docs (
            thread_id       TEXT PRIMARY KEY,
            doc_text        TEXT NOT NULL,
            company_domain  TEXT,
            is_important    INTEGER DEFAULT 0,
            outreach_score  REAL DEFAULT 0,
            doc_hash        TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tsd_company ON thread_search_docs(company_domain)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS thread_embeddings (
            thread_id       TEXT NOT NULL,
            model_name      TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            PRIMARY KEY (thread_id, model_name)
        )
    """)
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v26] thread_search_docs and thread_embeddings tables created")


def _migrate_to_v25(conn: sqlite3.Connection) -> None:
    """Migration v24 -> v25: add pipeline_jobs table for web-based job management."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id              INTEGER PRIMARY KEY,
            job_type        TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'queued',
            config_json     TEXT NOT NULL,
            pid             INTEGER,
            started_at      TEXT,
            completed_at    TEXT,
            created_at      TEXT NOT NULL,
            exit_code       INTEGER,
            error_message   TEXT,
            current_stage   TEXT,
            progress_done   INTEGER DEFAULT 0,
            progress_total  INTEGER DEFAULT 0,
            current_company TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status ON pipeline_jobs(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_created ON pipeline_jobs(created_at)")
    # Add staleness_status column to companies for pre-computed staleness
    cols = _get_column_names(conn, "companies")
    if "staleness_status" not in cols:
        conn.execute("ALTER TABLE companies ADD COLUMN staleness_status TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v25] pipeline_jobs table created, staleness_status column added")


def _migrate_to_v24(conn: sqlite3.Connection) -> None:
    """Migration v23 -> v24: add error column to processing_runs."""
    cols = _get_column_names(conn, "processing_runs")
    if "error" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN error TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v24] error column added to processing_runs")


def _migrate_to_v23(conn: sqlite3.Connection) -> None:
    """Migration v22 -> v23: add prompt_hash to processing_runs for prompt versioning."""
    cols = _get_column_names(conn, "processing_runs")
    if "prompt_hash" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN prompt_hash TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v23] prompt_hash column added to processing_runs")


def _migrate_to_v22(conn: sqlite3.Connection) -> None:
    """Migration v21 -> v22: add chain tracking to processing_runs (parent_run_id, email_cutoff_date)."""
    cols = _get_column_names(conn, "processing_runs")
    if "parent_run_id" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN parent_run_id INTEGER REFERENCES processing_runs(id)")
    if "email_cutoff_date" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN email_cutoff_date TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v22] chain tracking added to processing_runs (parent_run_id, email_cutoff_date)")


def _migrate_to_v21(conn: sqlite3.Connection) -> None:
    """Migration v20 -> v21: add proposed_changes_json to processing_runs for evaluation snapshots."""
    cols = _get_column_names(conn, "processing_runs")
    if "proposed_changes_json" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN proposed_changes_json TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v21] proposed_changes_json column added to processing_runs")


def _migrate_to_v4(conn: sqlite3.Connection) -> None:
    """Migration v3 -> v4: add account_name column to emails."""
    cols = _get_column_names(conn, "emails")
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
    _log("  [migration v4] account_name column added")


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

    cols = _get_column_names(conn, "emails")
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
            _log(f"  [migration v3] normalised_subject: {processed}/{total}")

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
            _log(f"  [migration v3] email_references: {processed}/{total}")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v3] complete")


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
    _log("  [migration v5] entities table replaced with companies + company_contacts")


def _migrate_to_v6(conn: sqlite3.Connection) -> None:
    """Migration v5 -> v6: add homepage_fetched_at to companies, add company_labels table."""
    cols = _get_column_names(conn, "companies")
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
    _log("  [migration v6] homepage columns and company_labels table added")


def _migrate_to_v7(conn: sqlite3.Connection) -> None:
    """Migration v6 -> v7: add description column to companies."""
    cols = _get_column_names(conn, "companies")
    if "description" not in cols:
        conn.execute("ALTER TABLE companies ADD COLUMN description TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v7] description column added to companies")


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
    _log("  [migration v8] discussions tables added")


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
    _log("  [migration v9] actions table added")


def _migrate_to_v10(conn: sqlite3.Connection) -> None:
    """Migration v9 -> v10: add completed_date column to actions."""
    cols = _get_column_names(conn, "actions")
    if "completed_date" not in cols:
        conn.execute("ALTER TABLE actions ADD COLUMN completed_date TEXT")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v10] completed_date column added to actions")


def _migrate_to_v11(conn: sqlite3.Connection) -> None:
    """Migration v10 -> v11: rename assignee_email to assignee_emails (JSON array)."""
    import json as _json

    cols = _get_column_names(conn, "actions")
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
    _log("  [migration v11] assignee_email migrated to assignee_emails (JSON array)")


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
    cols = _get_column_names(conn, "sync_state")
    if "sync_token" not in cols:
        conn.execute("ALTER TABLE sync_state ADD COLUMN sync_token TEXT")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v12] calendar_events and discussion_events tables added")


def _migrate_to_v13(conn: sqlite3.Connection) -> None:
    """Migration v12 -> v13: add event ledger, milestones, feedback, few-shot, learned rules tables."""
    conn.execute("""CREATE TABLE IF NOT EXISTS event_ledger (
        id              TEXT PRIMARY KEY,
        thread_id       TEXT,
        source_email_id TEXT,
        source_calendar_event_id TEXT,
        discussion_id   INTEGER REFERENCES discussions(id),
        domain          TEXT NOT NULL,
        type            TEXT NOT NULL,
        actor           TEXT,
        target          TEXT,
        event_date      TEXT,
        detail          TEXT,
        confidence      REAL,
        model_version   TEXT,
        prompt_version  TEXT,
        created_at      TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_ledger_thread ON event_ledger(thread_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_ledger_discussion ON event_ledger(discussion_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_ledger_domain ON event_ledger(domain)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_ledger_type ON event_ledger(type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_ledger_date ON event_ledger(event_date)")

    conn.execute("""CREATE TABLE IF NOT EXISTS milestones (
        id              INTEGER PRIMARY KEY,
        discussion_id   INTEGER REFERENCES discussions(id),
        name            TEXT NOT NULL,
        achieved        INTEGER DEFAULT 0,
        achieved_date   TEXT,
        evidence_event_ids TEXT,
        confidence      REAL,
        last_evaluated_at TEXT,
        UNIQUE(discussion_id, name)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_milestones_discussion ON milestones(discussion_id)")

    conn.execute("""CREATE TABLE IF NOT EXISTS feedback (
        id              INTEGER PRIMARY KEY,
        layer           TEXT NOT NULL,
        target_type     TEXT NOT NULL,
        target_id       TEXT NOT NULL,
        action          TEXT NOT NULL,
        old_value       TEXT,
        new_value       TEXT,
        reason          TEXT,
        applied         INTEGER DEFAULT 0,
        created_at      TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_layer ON feedback(layer)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feedback_target ON feedback(target_type, target_id)")

    conn.execute("""CREATE TABLE IF NOT EXISTS few_shot_examples (
        id              INTEGER PRIMARY KEY,
        layer           TEXT NOT NULL,
        category        TEXT,
        input_text      TEXT NOT NULL,
        wrong_output    TEXT,
        correct_output  TEXT NOT NULL,
        source_feedback_ids TEXT,
        created_at      TEXT NOT NULL
    )""")

    conn.execute("""CREATE TABLE IF NOT EXISTS learned_rules (
        id              INTEGER PRIMARY KEY,
        layer           TEXT NOT NULL,
        category        TEXT,
        rule_text       TEXT NOT NULL,
        source_feedback_ids TEXT,
        active          INTEGER DEFAULT 1,
        created_at      TEXT NOT NULL
    )""")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v13] event ledger, milestones, feedback tables added")


def _migrate_to_v14(conn: sqlite3.Connection) -> None:
    """Migration v13 -> v14: add proposed_actions table."""
    conn.execute("""CREATE TABLE IF NOT EXISTS proposed_actions (
        id              INTEGER PRIMARY KEY,
        discussion_id   INTEGER REFERENCES discussions(id),
        action          TEXT NOT NULL,
        reasoning       TEXT,
        priority        TEXT,
        wait_until      TEXT,
        assignee        TEXT,
        model_used      TEXT,
        created_at      TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_proposed_actions_discussion ON proposed_actions(discussion_id)")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v14] proposed_actions table added")


def _migrate_to_v15(conn: sqlite3.Connection) -> None:
    """Migration v14 -> v15: add parent_id to discussions for sub-discussions."""
    cols = _get_column_names(conn, "discussions")
    if "parent_id" not in cols:
        conn.execute("ALTER TABLE discussions ADD COLUMN parent_id INTEGER REFERENCES discussions(id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discussions_parent ON discussions(parent_id)")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v15] parent_id column added to discussions")


def _migrate_to_v20(conn: sqlite3.Connection) -> None:
    """Migration v19 -> v20: add token tracking to processing_runs + llm_calls table."""
    cols = _get_column_names(conn, "processing_runs")
    if "input_tokens" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN input_tokens INTEGER DEFAULT 0")
    if "output_tokens" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN output_tokens INTEGER DEFAULT 0")
    if "llm_calls" not in cols:
        conn.execute("ALTER TABLE processing_runs ADD COLUMN llm_calls INTEGER DEFAULT 0")

    conn.execute("""CREATE TABLE IF NOT EXISTS llm_calls (
        id              INTEGER PRIMARY KEY,
        run_id          INTEGER REFERENCES processing_runs(id),
        stage           TEXT NOT NULL,
        model           TEXT,
        input_tokens    INTEGER DEFAULT 0,
        output_tokens   INTEGER DEFAULT 0,
        duration_ms     INTEGER,
        created_at      TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON llm_calls(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_stage ON llm_calls(stage)")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v20] token tracking added to processing_runs + llm_calls table")


def _migrate_to_v19(conn: sqlite3.Connection) -> None:
    """Migration v18 -> v19: add run_id to discussions, milestones, proposed_actions."""
    for table in ("discussions", "milestones", "proposed_actions"):
        cols = _get_column_names(conn, table)
        if "run_id" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN run_id INTEGER REFERENCES processing_runs(id)")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v19] run_id column added to discussions, milestones, proposed_actions")


def _migrate_to_v18(conn: sqlite3.Connection) -> None:
    """Migration v17 -> v18: add processing_runs table and run_id to event_ledger."""
    conn.execute("""CREATE TABLE IF NOT EXISTS processing_runs (
        id              INTEGER PRIMARY KEY,
        company_domain  TEXT NOT NULL,
        mode            TEXT NOT NULL,
        model           TEXT,
        started_at      TEXT NOT NULL,
        completed_at    TEXT,
        events_created  INTEGER DEFAULT 0,
        discussions_created INTEGER DEFAULT 0,
        discussions_updated INTEGER DEFAULT 0,
        actions_proposed INTEGER DEFAULT 0
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_runs_company ON processing_runs(company_domain)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_runs_mode ON processing_runs(mode)")

    cols = _get_column_names(conn, "event_ledger")
    if "run_id" not in cols:
        conn.execute("ALTER TABLE event_ledger ADD COLUMN run_id INTEGER REFERENCES processing_runs(id)")

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v18] processing_runs table and run_id column added")


def _migrate_to_v17(conn: sqlite3.Connection) -> None:
    """Migration v16 -> v17: add source_type and source_id to event_ledger."""
    cols = _get_column_names(conn, "event_ledger")
    if "source_type" not in cols:
        conn.execute("ALTER TABLE event_ledger ADD COLUMN source_type TEXT NOT NULL DEFAULT 'email'")
    if "source_id" not in cols:
        conn.execute("ALTER TABLE event_ledger ADD COLUMN source_id TEXT")

    # Backfill source_id from existing columns
    conn.execute("""UPDATE event_ledger SET source_id = source_email_id
                    WHERE source_email_id IS NOT NULL AND source_id IS NULL""")
    conn.execute("""UPDATE event_ledger SET source_type = 'calendar', source_id = source_calendar_event_id
                    WHERE source_calendar_event_id IS NOT NULL AND source_email_id IS NULL AND source_id IS NULL""")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v17] source_type and source_id columns added to event_ledger")


def _migrate_to_v16(conn: sqlite3.Connection) -> None:
    """Migration v15 -> v16: add change_journal table."""
    conn.execute("""CREATE TABLE IF NOT EXISTS change_journal (
        id              INTEGER PRIMARY KEY,
        entity_type     TEXT NOT NULL,
        entity_id       TEXT NOT NULL,
        change_type     TEXT NOT NULL,
        source_stage    TEXT,
        created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        processed_at    TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_change_journal_entity ON change_journal(entity_type, entity_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_change_journal_unprocessed ON change_journal(processed_at) WHERE processed_at IS NULL")
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
    )
    conn.commit()
    _log("  [migration v16] change_journal table added")


def execute(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Cursor:
    return conn.execute(sql, params)


def fetchall(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[Any]:
    return conn.execute(sql, params).fetchall()


def fetchone(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any | None:
    return conn.execute(sql, params).fetchone()
