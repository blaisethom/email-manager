# Pipeline Architecture

The email-analyser is an AI-powered CRM/PRM that transforms raw email and calendar data into structured business intelligence. It extracts business events from email threads, clusters them into discussions, tracks relationship state through workflow milestones, and proposes next actions. The system is organised as a multi-stage pipeline where each stage reads from upstream tables and writes to downstream ones, with a change journal coordinating incremental processing.

## High-Level Data Flow

```
  Email accounts                   Calendar accounts
  (Gmail / IMAP)                   (Google Calendar)
        |                                |
        v                                v
  +-----------+                   +-------------+
  |   sync    |                   | sync_calendar|
  +-----------+                   +-------------+
        |                                |
        v                                v
  +-----------+      +-------------------------------------------+
  |  emails   |      |          calendar_events                  |
  +-----------+      +-------------------------------------------+
        |
        v
  +-------------------+
  | compute_threads   |   (Union-Find on Message-ID / References / subject)
  +-------------------+
        |
        v
  +-------------------+        +-------------------+
  | extract_base      |  --->  | contacts          |
  | (no AI)           |  --->  | companies         |
  +-------------------+  --->  | company_contacts  |
        |                --->  | co_email_stats    |
        v
  +-------------------+
  | fetch_homepages   |  --->  data/homepages/{domain}.html
  +-------------------+
        |
        v
  +-------------------+
  | label_companies   |  --->  company_labels (customer, investor, vendor, ...)
  | (AI)              |
  +-------------------+
        |
        v
  +-------------------+
  | extract_events    |  --->  event_ledger
  | (AI)              |
  +-------------------+
        |
        v
  +-------------------+
  | discover_discuss. |  --->  discussions, discussion_threads
  | (AI)              |
  +-------------------+
        |
        v
  +-------------------+
  | analyse_discuss.  |  --->  milestones, discussion_state_history
  | (AI)              |
  +-------------------+
        |
        v
  +-------------------+
  | propose_actions   |  --->  proposed_actions
  | (AI)              |
  +-------------------+
        |
        v
  +-------------------+
  | contact_memory    |  --->  contact_memories (SQLite + Markdown)
  | (AI)              |
  +-------------------+
```

## Stage Details

### 1. Email Ingestion (`sync`)

Emails are fetched via two backends, configured per-account in `accounts.json`:

**Gmail** (`ingestion/gmail_client.py`): Uses the Gmail API with OAuth2. Full sync lists all message IDs from configured labels and downloads any not yet in the database. Incremental sync uses Gmail's History API, keyed on a `historyId` stored in the `sync_state` table. If the stored `historyId` has expired (Google retains ~30 days), the client falls back to a full sync. Emails are fetched in raw RFC 2822 format and parsed locally.

**IMAP** (`ingestion/imap_client.py`): Connects via standard IMAP4 (with special-case handling for Yahoo/AOL rate limits). Incremental sync uses IMAP UIDs stored in `sync_state`, with `UIDVALIDITY` to detect mailbox resets. Batch size is 100 messages (10 for Yahoo). Retry logic: up to 5 attempts with exponential backoff.

**Parsing** (`ingestion/parser.py`): Raw RFC 2822 bytes are parsed into an `Email` model. Headers (Message-ID, From, To, Cc, Date, References, In-Reply-To) are extracted and normalised. Body text is extracted from plain-text parts or converted from HTML via `html2text`. Addresses are lowercased.

**Thread computation** (`ingestion/threading.py`): A Union-Find algorithm groups emails into threads by matching on (a) Message-ID + References/In-Reply-To chains (RFC 2822 standard), and (b) normalised subject + shared participants within a 90-day window. Incremental mode only processes emails that lack a `thread_id`. A full rebuild clears all assignments and recomputes from scratch.

**Calendar sync** (`ingestion/calendar_client.py`): For Gmail accounts, syncs Google Calendar events with a configurable lookback window (default 6 months). Calendar events are later linked to discussions by attendee overlap and time proximity scoring.

On ingestion, the system records entries in the **change journal** for each affected thread and company domain so that downstream stages can detect what needs processing.

### 2. Base Extraction (`extract_base`)

No AI calls. Pure SQL-based extraction from email headers:

- **Contacts**: Upserts from From/To/Cc fields, tracking first/last seen, email counts, sent vs received counts.
- **Companies**: Inferred from contact email domains (e.g. `jane@acme.com` -> `acme.com`). Stores email count and first/last seen.
- **Company-contact links**: `company_contacts` maps companies to their associated email addresses.
- **Co-email stats**: `co_email_stats` records how often pairs of email addresses appear together in threads, for later relationship analysis.

### 3. Homepage Fetching (`fetch_homepages`)

Downloads company homepages concurrently (10 workers by default) and caches them on disk at `data/homepages/{domain}.html`. The cached HTML is fed into the company labelling prompt to give the LLM richer context about the organisation.

### 4. Company Labelling (`label_companies`)

Each company is classified into a relationship category using an LLM call. The prompt includes the company name, a sample of emails exchanged, and homepage content (if available). Labels are defined in `company_labels.yaml` and typically include: pharma, CRO, academic, hospital, vendor, investor, recruiter, partner, service-provider, internal, other.

Output: `company_labels` table with label, confidence, reasoning, and model used.

### 5. Event Extraction (`extract_events`)

The core of the pipeline. For each email thread, the LLM extracts discrete, factual business events using a domain-specific vocabulary defined in `discussion_categories.yaml`. A "domain" is a business context like `pharma-deal`, `investment`, `hiring`, `scheduling`, each with its own event types (e.g. `lead_identified`, `deck_shared`, `term_sheet_sent`, `meeting_proposed`).

**Prompt structure**: The system prompt defines what an "event" is (something observable that happened, not an interpretation), instructs the model to use the provided vocabulary, and emphasises deduplication rules. The user prompt provides the thread's emails chronologically, the available domain vocabularies, and account ownership context.

**Batching**: Small threads (<=3 emails, <=2K characters) are grouped into batches of ~8K characters and processed in a single LLM call using a batch prompt variant. Large threads get individual calls. Threads over 25 emails are chunked with 1-email overlap for context continuity. This batching typically reduces extraction calls by 50-60% (e.g. 30 threads -> ~12-15 calls).

**Deduplication**: After extraction, `_dedup_events()` removes duplicates within a thread by keying on `domain|type|date|actor|target`, keeping the highest-confidence instance. A separate `_dedup_against_previous()` check prevents re-extraction of events that already exist in the event ledger.

**Incremental detection**: Only processes threads that either have no events yet, or where `MAX(emails.date) > MAX(event_ledger.created_at)` for that thread (i.e. new emails arrived since last extraction).

**Model override**: The `extract_events_model` config option allows using a cheaper model for this high-volume stage while keeping a more capable model for downstream stages.

Output: `event_ledger` table. Each event records thread_id, source_email_id, source_type, run_id, domain, type, actor, target, event_date, detail, confidence, model_version, and prompt_version.

### 6. Discussion Discovery (`discover_discussions`)

Clusters events into coherent business discussions. A "discussion" represents an ongoing interaction with an external party around a specific topic (e.g. "Series A fundraise with Acme VC", "Hiring - Senior Engineer", "Pharma deal - BigPharma trial").

The LLM receives a company's events grouped by time clusters and decides which events belong to the same discussion. It can assign events to existing discussions or create new ones. It also handles sub-discussions (e.g. a scheduling thread that supports a larger investment discussion gets `parent_id` pointing to the parent).

Merging logic detects and consolidates overlapping discussions via similarity matching.

Output: `discussions` and `discussion_threads` tables.

### 7. Discussion Analysis (`analyse_discussions`)

For each discussion, a single LLM call evaluates:

- **Milestones**: Which checkpoints have been achieved (e.g. `qualified_lead`, `demo_delivered`, `contract_sent`, `closed_won`), with evidence linking back to specific event IDs and confidence scores.
- **Workflow state**: The current position in the domain's state machine (e.g. `discovery -> qualification -> demo -> proposal -> negotiating -> signed`). Terminal states like `signed`, `lost`, `stale` lock the discussion from further processing.
- **Summary**: A 2-4 sentence narrative incorporating the latest developments.

Output: `milestones` table, `discussion_state_history` audit trail, and updated `discussions.summary`.

### 8. Action Proposal (`propose_actions`)

For each non-terminal discussion, proposes 1-3 specific next actions with:

- **Priority**: `high` (this week), `medium` (soon), `low` (can wait)
- **Reasoning**: Why this action matters now
- **Assignee**: Who should do it, if identifiable
- **Wait-until date**: For follow-ups that should wait

Output: `proposed_actions` table.

### 9. Contact Memory (`contact_memory`)

Generates AI-enhanced contact profiles by synthesising a contact's email history and discussion involvement. Two strategies: "default" (fast, 1-2 sentence summaries) and "detailed" (richer context with full discussion summaries). Two storage backends: SQLite (`contact_memories` table) and Markdown files (`data/memories/`).

## Pipeline Execution Modes

### Staged Pipeline (default)

Run via `email-analyser analyse`. Executes stages sequentially. Two ordering modes:

- **Stage-first** (default): Run each stage across all target companies, then the next stage. Natural when you want consistent state per stage.
- **Per-company** (`--per-company`): Run all stages for one company, then the next. More memory-efficient for large runs; used by `run_pipeline.sh`.

Global stages (`extract_base`, `fetch_homepages`, `label_companies`) always run once, not per-company.

### Quick Update

Run via `email-analyser update`. The incremental path for day-to-day use:

1. The change journal identifies companies with unprocessed changes.
2. For each company, counts new threads since last processing.
3. If below a threshold (typically <10 new threads): uses a **single merged LLM call** that does extraction + discovery + analysis + actions in one pass (`quick_update.py`). Much faster for the common case of a few new emails arriving.
4. If above the threshold: falls back to the full staged pipeline for that company.

The quick update prompt receives new emails plus full existing discussion context, so it can correctly assign events to existing discussions and update their state.

### Agent Mode

Run via `email-analyser update --agent`. Uses the Claude Code SDK to run an autonomous agent per company. The agent gets read-only database tools (`get_new_emails`, `get_discussions`, `get_category_config`) and a single write tool (`propose_changes`). It explores the data, reasons about it, and proposes a structured changeset (`ProposedChanges`) that the system reviews and applies. This separates AI reasoning from database writes for safety.

### Batch Script (`run_pipeline.sh`)

A bash wrapper for large batch runs across many companies. Processes one company at a time, with:

- Resume capability via `.pipeline_progress` tracking
- Memory monitoring (waits for >1000MB available before starting each company)
- 15-minute timeout per company
- Per-company log files (`pipeline_{domain}.log`)

## Managing Updates in Source Data

### Change Journal

The `change_journal` table is the central mechanism for tracking what needs processing. It records entries with:

| Field | Purpose |
|-------|---------|
| `entity_type` | `'thread'`, `'company'`, or `'discussion'` |
| `entity_id` | The thread_id, company domain, or discussion ID |
| `change_type` | `'new_email'`, `'new_event'`, `'state_change'`, `'manual_event'` |
| `source_stage` | Which stage or action produced this entry |
| `processed_at` | `NULL` until consumed by a downstream stage |

**Write path**: Email ingestion records journal entries when new emails arrive. Event extraction records entries when new events are created. The agent backend records company-level changes after processing.

**Read path**: `get_dirty_company_domains()` resolves thread-level changes to company domains by joining through `company_contacts`, so that `email-analyser update` (no args) automatically finds which companies need work.

**Consumption**: Each downstream stage marks journal entries as `processed_at` when it completes, preventing double-processing.

### Incremental Detection Beyond the Journal

The event extraction stage has its own timestamp-based freshness check: it compares `MAX(emails.date)` against `MAX(event_ledger.created_at)` per thread. This catches threads that received new emails even if the change journal missed them (e.g. after a manual database repair).

### Force and Clean Modes

- `--force`: Reprocesses entities even if already done, ignoring `pipeline_runs` records and freshness checks. Used after model or prompt changes when you want to regenerate all analysis.
- `--clean`: Deletes previous output for the target stages before reprocessing. Used when you want a fresh start rather than incremental updates layered on top of old results.

### Filtering and Scoping

The pipeline accepts fine-grained filters to control what gets processed:

- `--company domain`: Single company
- `--label category`: All companies with a specific label
- `--company-file path`: Batch from a file of domains
- `--stale-before date`: Companies whose latest milestone evaluation is before the cutoff
- `--last-seen-after / --last-seen-before`: Companies by email activity window
- `--dry-run`: Shows what would be processed without doing it

## Managing User Input

### Manual Event Injection (schema-ready, UI partially implemented)

The event ledger supports a polymorphic `source_type` field: `'email'`, `'calendar'`, `'manual'`, `'debrief'`. The `add-event` CLI command (planned/partial) writes directly to the event ledger with `source_type = 'manual'` and inserts a change journal entry so the affected discussion gets re-analysed on the next update.

### Discussion Management

`update-discussion` allows manual corrections: changing workflow state, renaming, reassigning to a different company. Changes are recorded in `discussion_state_history` for audit and in the `feedback` table for future AI learning.

### Feedback Loop (schema exists, not yet wired into prompts)

Three tables support a feedback-driven learning loop:

- **`feedback`**: Records corrections (layer, target, old value, new value, reason). Written when a user overrides an AI decision.
- **`few_shot_examples`**: Input/output pairs derived from feedback, intended to be injected into prompts for in-context learning.
- **`learned_rules`**: Natural-language rules distilled from feedback patterns (e.g. "emails from @acme.com about 'trial' should use the pharma-deal domain, not investment").

These tables are populated by manual corrections but not yet consumed by the prompt construction code.

## Provenance and Lineage

Every piece of derived data can be traced back to its source:

| Entity | Lineage Fields |
|--------|---------------|
| Event | `run_id` -> processing_runs, `model_version`, `prompt_version`, `source_type`, `source_id`, `source_email_id` |
| Discussion | `run_id` -> processing_runs, `model_used` |
| Milestone | `run_id` -> processing_runs, `confidence`, `evidence_event_ids`, `last_evaluated_at` |
| State change | `discussion_state_history.reasoning`, `model_used`, `detected_at` |
| Action | `run_id` -> processing_runs, `model_used` |
| Contact memory | `model_used`, `strategy_used`, `version`, `emails_hash` |

The `processing_runs` table records each execution: company_domain, mode (`staged`/`quick`/`agent`), model, timestamps, and counts (events_created, discussions_created, discussions_updated, actions_proposed).

## AI Backend Architecture

The system abstracts LLM access behind an `LLMBackend` protocol:

```python
class LLMBackend(Protocol):
    def complete(system: str, user: str, temperature=0.3) -> str
    def complete_json(system: str, user: str, temperature=0.0) -> dict
    @property
    def model_name(self) -> str
```

Three implementations:

- **Claude API** (`claude_backend.py`): Direct Anthropic SDK calls. Default model configurable; max_tokens=4096, timeout=120s.
- **Claude CLI** (`claude_cli_backend.py`): Falls back to the `claude` command-line tool as a subprocess. No prompt caching (each subprocess is independent). This was the original backend before API key access.
- **Ollama** (`ollama_backend.py`): Local inference for development/testing.

The factory selects the backend based on `config.ai_backend` and API key availability. Per-stage model overrides are supported (e.g. a cheaper model for high-volume event extraction).

## Database

SQLite with WAL mode, foreign keys enabled, 30-second busy timeout. Schema version 19 with migration support. Key pragmas:

```sql
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=30000;
```

All analysis tables use integer foreign keys back to core tables. The schema is defined in a single `SCHEMA_SQL` string in `db.py` and applied idempotently on every connection.

## Configuration

| Source | Purpose |
|--------|---------|
| `.env` / environment | API keys, backend selection, model names |
| `accounts.json` | Email account credentials and settings |
| `company_labels.yaml` | Company relationship taxonomy |
| `discussion_categories.yaml` | Domain vocabularies, event types, milestones, workflow states |
| `Config` (pydantic-settings) | Central config class merging all sources |

The `discussion_categories.yaml` file is particularly important: it defines the entire domain model. Each category specifies event types with descriptions, milestones to track, workflow states with an ordering, and terminal states. This is the vocabulary the LLM uses when extracting events and analysing discussions. Changing this file changes what the system can detect.

---

## Shortcomings

### Duplicated Incremental Codepaths

The quick update prompt (`quick_update.py`) and the staged pipeline (`extract_events` -> `discover_discussions` -> `analyse_discussions` -> `propose_actions`) are two separate codepaths that produce the same outputs. The quick update is a single merged LLM call; the staged pipeline is multiple focused calls. They share some utility functions (email formatting, dedup, domain config loading) but have independent prompt templates, JSON parsing, and save logic. This means:

- Bug fixes must be applied in both places.
- Behavioural drift is likely: a prompt improvement in one path may not be reflected in the other.
- The agent mode (`agent_backend.py`) is a third codepath with its own `ProposedChanges` structure and `apply_changes` function, adding to the maintenance burden.

The system should converge on a single save-path that all execution modes feed into, with the LLM call strategy (single merged call vs. staged calls) being the only variable.

### No Prompt Caching When Using the CLI Backend

When running via `claude-cli` (subprocess per call), there is no shared context between calls. Each subprocess pays full input token costs. This makes the system significantly more expensive per-token than it needs to be, and rules out prompt caching strategies that the Anthropic API supports. The batch prompt optimisation (grouping small threads) is a workaround for this, but it's a blunt instrument compared to proper prompt caching.

### Coarse Company-Level Scoping

All analysis is scoped to companies (identified by email domain). This breaks down for:

- **Multi-domain organisations**: A company that emails from both `acme.com` and `acme.co.uk` appears as two separate companies with independent discussion histories.
- **Personal email addresses**: Contacts using `gmail.com` or `outlook.com` addresses don't map cleanly to a company. All `gmail.com` contacts would be grouped under one "company".
- **Intermediaries**: Introductions by a third party (e.g. a recruiter introducing a candidate) may be attributed to the wrong company.

The system needs a company-merging or aliasing mechanism and better handling of freemail domains.

### Feedback Loop Not Wired

The `feedback`, `few_shot_examples`, and `learned_rules` tables exist in the schema but are not consumed by any prompt construction code. User corrections via `update-discussion` are recorded but never fed back into the LLM. This means the system makes the same mistakes repeatedly and doesn't improve from corrections.

### No Rollback Mechanism

While every analysis result is stamped with a `run_id`, there is no tooling to roll back a specific run's outputs. If a bad model or prompt produces incorrect results, the only recovery paths are `--clean` (nuke and redo) or manual SQL. A `rollback-run <run_id>` command that deletes events, discussion updates, milestones, and actions produced by a specific run would be valuable.

### Sequential LLM Calls

All LLM calls are synchronous and sequential. Companies are independent, threads within a company are independent, discussions are independent, but the system processes them one at a time. The `BetterUpdating.md` plan identifies this and proposes async backends with a concurrency semaphore, but it's not implemented. The impact depends on API rate limits, but even modest parallelism (3-5 concurrent calls) would meaningfully reduce wall-clock time for batch runs.

### SQLite Scaling Limits

SQLite is single-writer. WAL mode helps with concurrent reads, but the pipeline's write patterns (many small inserts across stages) and the 30-second busy timeout can become a bottleneck if multiple processes try to write simultaneously. For a single-user tool this is fine, but it limits future multi-user or continuous-processing scenarios.

### Event Extraction Quality is Prompt-Sensitive

The event extraction stage is the foundation everything else builds on. If the LLM misclassifies a domain, misses an event, or hallucinates one, the error propagates through discovery, analysis, milestones, and actions. The system has no automated quality checks on extracted events (e.g. validating that event types match the domain vocabulary, that dates are plausible, that actors are known contacts). Post-extraction validation could catch many classes of error before they compound.

### Calendar Linking is Heuristic-Only

Calendar events are linked to discussions via attendee overlap and time proximity scoring, with no LLM involvement. This produces false matches when meetings have overlapping attendees across multiple discussions, and misses matches when the calendar invite uses different email addresses than the discussion participants.

### No Automated Testing of AI Outputs

There are no regression tests that verify the quality of LLM outputs against known-good examples. Prompt changes, model upgrades, or vocabulary changes can silently degrade output quality. An evaluation suite with representative threads and expected events/discussions would catch regressions before they hit production data.

---

## Future: Learning and Versioning

### How the System Should Learn

The feedback infrastructure is partially built. Three mechanisms should be completed:

**1. Few-shot injection**: When a user corrects an event extraction (wrong domain, missed event, hallucinated event), the system should generate a few-shot example from the correction: "given this thread, you said X but the correct answer is Y." These examples should be injected into the extraction prompt for similar threads. The `few_shot_examples` table already exists for this purpose.

**2. Learned rules**: Pattern-level corrections (e.g. "emails from @acme.com about 'trial' are always pharma-deal, not investment") should be distilled into rules and prepended to relevant prompts. The `learned_rules` table exists but isn't read during prompt construction.

**3. Confidence calibration**: The system tracks confidence scores on events and milestones. Over time, comparing confidence predictions to user corrections would allow calibrating thresholds: "events with confidence < 0.6 from this model are wrong 40% of the time" could trigger automatic review queues.

### Versioning Strategy

The system already tracks `model_version` and `prompt_version` on every event, and `model_used` on discussions, milestones, and actions. This is the foundation for a proper versioning strategy, but several pieces are missing:

**Prompt versioning**: Currently a static string (`PROMPT_VERSION = "v2"`). This should be computed from a hash of the actual prompt template content, so that any prompt edit automatically creates a new version. Alternatively, prompts should live in versioned files rather than Python constants.

**Category config versioning**: The `discussion_categories.yaml` file defines the domain model. Changing event types, milestones, or workflow states changes what the system can detect. But the YAML file has no version tracking. Adding or removing event types from a domain should be a versioned change, and re-analysis should be triggered for affected companies when the vocabulary changes.

**Model version management**: When upgrading models (e.g. Claude Sonnet 4 -> Claude Sonnet 4.6), all existing analysis was produced by the old model. The system should be able to:

1. Identify which companies/discussions were last analysed by which model version.
2. Prioritise re-analysis for entities where the model change is most likely to matter (e.g. low-confidence events, discussions in ambiguous states).
3. Compare outputs between model versions to detect regressions before committing to the new model.

**Run-based rollback**: Each `processing_runs` entry should support rollback: delete all events, discussion updates, milestones, and actions created by that run, restoring the previous state. This turns model upgrades into safe, reversible operations.

**Schema versioning**: The database uses `SCHEMA_VERSION = 19` with sequential migrations. This works but doesn't capture the relationship between schema changes and analysis compatibility. A schema migration that adds a column is fine, but one that changes how events are stored may invalidate existing analysis. The migration system should distinguish structural changes from semantic changes.

### Evaluation and Quality Gates

Before deploying a prompt change or model upgrade:

1. Run the new configuration against a held-out set of representative threads.
2. Compare extracted events against a gold-standard annotation set.
3. Measure precision/recall per domain and event type.
4. Only promote the new configuration if metrics are at or above the previous version.

This evaluation pipeline doesn't exist yet but is essential for safe iteration. Without it, every change to prompts, models, or category vocabularies is a leap of faith.
