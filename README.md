# Email Manager

A personal email data pipeline that syncs your emails into a local SQLite database, uses AI to extract business events, track discussions with milestones and workflow states, label company relationships, build contact memories, and provides an interactive chat agent and web dashboard for exploring your email data.

All data stays local. You choose the AI backend.

## Quick Start

```bash
# Install
uv sync

# Configure accounts
cp accounts.json.example accounts.json
# Edit accounts.json with your email accounts

# Configure AI
cp .env.example .env
# Edit .env with your AI backend settings

# Sync emails
email-manager sync

# Run base analysis (no AI needed)
email-manager analyse --stage extract_base

# Run AI analysis
email-manager analyse

# Generate contact memories
email-manager memory --all --limit 10

# Explore interactively
email-manager chat
```

## Architecture

```
┌──────────────────┐     ┌──────────────┐     ┌──────────────────────────┐
│  Email Accounts  │     │   SQLite     │     │  AI Backend              │
│  Gmail / IMAP    │────>│   Database   │<───>│  (Claude/CLI/Ollama)     │
│  Calendar        │     │              │     └──────────────────────────┘
└──────────────────┘     │  emails      │              │
                         │  event_ledger│     ┌────────┴─────────────────┐
                         │  discussions │     │  Pipeline Stages         │
                         │  milestones  │     │  1. Sync Calendar        │
                         │  contacts    │     │  2. Extract Base (no AI) │
                         │  companies   │     │  3. Fetch Homepages      │
                         │  contact_    │     │  4. Label Companies      │
                         │    memories  │     │  5. Extract Events       │
                         │              │     │  6. Discover Discussions  │
                         └──────────────┘     │  7. Analyse Discussions  │
                               │              │  8. Contact Memory       │
                    ┌──────────┴──────────┐   └──────────────────────────┘
                    │  CLI / Web UI       │
                    │  Chat Agent         │
                    └─────────────────────┘
```

### Data Flow

1. **Sync** — Emails are fetched from all configured accounts (Gmail API, IMAP) and stored in SQLite. Calendar events are also synced. Incremental sync means only new data is fetched on subsequent runs.
2. **Extract Base** — Contacts, companies, and co-email statistics are extracted from email headers. No AI needed.
3. **Fetch Homepages** — Company homepage content is downloaded and converted to markdown for use by later stages. No AI needed.
4. **Label Companies** — AI classifies each company's relationship to you (investor, customer, vendor, partner, etc.).
5. **Extract Events** — AI reads each email thread (with calendar context) and extracts fine-grained business events (e.g. `deck_shared`, `nda_signed`, `meeting_held`) into an append-only event ledger.
6. **Discover Discussions** — AI clusters events into discussions by company, topic, and participants. A thread can contribute events to multiple discussions.
7. **Analyse Discussions** — For each discussion, AI evaluates milestones (~10 per category), infers the current workflow state, and generates a narrative summary — all in a single call.
8. **Contact Memory** — AI generates relationship profiles for contacts (summary, key facts, discussion topics).
9. **Explore** — CLI commands, web dashboard, and the interactive chat agent let you query, explore, and refine the data.

## Multi-Account Setup

Configure multiple email accounts in `accounts.json`:

```json
[
  {
    "name": "personal-gmail",
    "backend": "gmail",
    "gmail_credentials_path": "data/gmail_credentials.json",
    "gmail_token_path": "data/gmail_token.json",
    "gmail_labels": []
  },
  {
    "name": "work-imap",
    "backend": "imap",
    "imap_host": "imap.example.com",
    "imap_user": "user@example.com",
    "imap_password": "your-app-password",
    "imap_port": 993,
    "imap_use_ssl": true,
    "imap_folders": ["*"]
  }
]
```

Set `imap_folders` to `["*"]` to auto-discover and sync all folders. All accounts feed into the same database, so analysis works across everything.

**Backwards compatible:** If no `accounts.json` exists, falls back to the single-account `.env` configuration.

### Gmail

Uses the Gmail API with OAuth2. Supports incremental sync via Gmail's `historyId` mechanism.

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project, enable the Gmail API
3. Create OAuth 2.0 credentials (Desktop application type)
4. Download the JSON file to `data/gmail_credentials.json`
5. Run `email-manager sync` — a browser window will open for OAuth consent on first run. The token is saved locally for future use.

### IMAP

Works with any email provider that supports IMAP (Fastmail, Yahoo, ProtonMail Bridge, self-hosted, etc.). Uses UID-based incremental sync with UIDVALIDITY tracking.

### Yahoo Mail

Yahoo is fully supported with automatic handling of its quirks:

- **Export endpoint** — Automatically uses `export.imap.mail.yahoo.com` (100k messages/folder) instead of the standard endpoint (10k limit)
- **Rate limiting** — Exponential backoff on rate limit errors, 1-second pause between batches
- **Connection drops** — Auto-reconnect with resume from last saved position
- **Batch size** — Smaller batches (50 vs 100) to stay under Yahoo's limits
- **Server errors** — `[SERVERBUG]` errors trigger retry, falling back to individual message fetches
- **App passwords** — Required since May 2024. Generate one at Yahoo Account > Security > App Passwords

## AI Backends

Three backends, all behind a common `LLMBackend` protocol. Switch between them by changing `AI_BACKEND` in `.env`.

| Backend | Setting | Auth | Best for |
|---|---|---|---|
| Claude API | `AI_BACKEND=claude` | `ANTHROPIC_API_KEY` | Highest quality, structured JSON output |
| Claude CLI | `AI_BACKEND=claude-cli` | Your existing `claude` CLI auth | No API key needed, uses your CLI subscription |
| Ollama | `AI_BACKEND=ollama` | None (local) | Full privacy, no data leaves your machine |

### Claude CLI

If you have [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated, this is the simplest option — no API key needed:

```
AI_BACKEND=claude-cli
```

### Ollama

Run any local model. Install [Ollama](https://ollama.com), pull a model, and configure:

```
AI_BACKEND=ollama
OLLAMA_MODEL=llama3.1:8b
OLLAMA_URL=http://localhost:11434
```

## Pipeline

The analysis pipeline has 8 stages. Each is independently resumable — if interrupted, re-running skips already-processed items.

| # | Stage | AI? | What it does |
|---|---|---|---|
| 1 | `sync_calendar` | No | Syncs Google Calendar events for calendar-email correlation |
| 2 | `extract_base` | No | Extracts contacts, companies, and co-email pair statistics from email headers |
| 3 | `fetch_homepages` | No | Downloads company homepages and converts to markdown (concurrent, 10 workers) |
| 4 | `label_companies` | Yes | Assigns relationship labels (investor, customer, vendor, etc.) to companies |
| 5 | `extract_events` | Yes | Reads each email thread + calendar context, extracts fine-grained business events into an append-only event ledger |
| 6 | `discover_discussions` | Yes | Clusters events into discussions by company, topic, and participants |
| 7 | `analyse_discussions` | Yes | Evaluates milestones, infers workflow state, and generates narrative summary per discussion |
| 8 | `contact_memory` | Yes | Generates AI memory profiles for contacts (relationship, discussions, key facts) |

```bash
email-analyser analyse                           # run all stages
email-analyser analyse --stage extract_base      # run one stage (no AI needed)
email-analyser analyse --stage label_companies   # classify company relationships
email-analyser run                               # sync + analyse in one command

# Rebuild a specific company from scratch
email-analyser analyse -s extract_events -s discover_discussions \
  -s analyse_discussions --company acme.com --clean

# Process all investor companies
email-analyser analyse -s extract_events --label investor --force
```

### Event-Driven Discussion Pipeline

The core analysis pipeline (stages 5-7) follows an event-driven architecture:

1. **Extract Events** — Each email thread is read once. The AI classifies the business domain(s) present (investment, pharma-deal, hiring, etc.) and extracts fine-grained events using a domain-specific vocabulary (~15 event types per category). Events are stored in an append-only ledger with source email links.

2. **Discover Discussions** — Events are clustered into discussions by company, topic, and participants. One thread can contribute events to multiple discussions. Overlapping discussions are automatically detected and merged.

3. **Analyse Discussions** — For each discussion, the AI evaluates ~10 category-specific milestones (e.g. `intro_complete`, `materials_shared`, `nda_executed`, `deep_diligence`), infers the current workflow state, and generates a narrative summary — all in a single call.

### Discussion Categories

Categories and their event types, milestones, and workflow states are defined in `discussion_categories.yaml`. Includes: investment, investor-relations, pharma-deal, scheduling, contract-negotiation, vendor-selection, internal-decision, hiring, partnership, support-issue, board-discussion, newsletter, and other.

### Flags

| Flag | Effect |
|---|---|
| `--force` / `-f` | Reprocess items even if already done (keeps old data) |
| `--clean` | Delete previous output for the scoped stages before reprocessing |
| `--company` / `-c` | Scope to a specific company domain |
| `--label` / `-l` | Scope to all companies with this label (e.g. `investor`) |
| `--limit` / `-n` | Only process the N most recent items |

## Contact Memory System

The memory system generates AI-powered profiles for each contact, including:
- **Relationship type** — colleague, vendor, client, friend, manager, etc.
- **Summary** — 2-4 sentence overview of all interactions
- **Discussions** — Each topic/project with status (active, resolved, waiting)
- **Key facts** — Extracted from email content ("Based in London", "Prefers async communication")

### Swappable backends and strategies

Two independent abstractions:

**Storage backends** (`MEMORY_BACKEND` in `.env`):
- `sqlite` — Stored in `contact_memories` table, queryable by the chat agent
- `markdown` — One `.md` file per contact in `data/memories/`, human-readable
- `both` (default) — Writes to both

**Generation strategies** (`MEMORY_STRATEGY` in `.env`):
- `default` — Single AI call with 30 recent emails, co-email network, projects, threads
- `detailed` — Two AI calls: first identifies all discussions in depth, then builds the profile with 50 emails

### Usage

```bash
email-manager memory                              # list all existing memories
email-manager memory alice@example.com            # show or generate for one contact
email-manager memory alice@example.com --force    # regenerate
email-manager memory --all --limit 20             # top 20 contacts by email count
email-manager memory --strategy detailed          # use detailed strategy
```

Memories are incremental — they detect when a contact's emails have changed and only regenerate when needed.

## Company Labelling

The `label_companies` stage classifies each company's relationship to you (customer, vendor, partner, etc.) using AI analysis of email exchanges and homepage content.

### Setup

1. **Run prerequisite stages** — company labelling works best when homepages have been fetched:

```bash
email-manager analyse --stage extract_base
email-manager analyse --stage fetch_homepages
```

2. **Configure labels** (optional) — copy the example config and customise:

```bash
cp company_labels.yaml.example company_labels.yaml
```

Edit `company_labels.yaml` to define labels relevant to your use case. Each label needs a name and description that guides the AI:

```yaml
labels:
  - name: customer
    description: A company that pays us for products or services.
  - name: prospect
    description: A company we are trying to sell to but is not yet a customer.
  - name: vendor
    description: A company that provides products or services to us.
  - name: partner
    description: A company we collaborate with on joint initiatives.
```

If no config file exists, a sensible set of defaults is used (customer, prospect, vendor, partner, investor, recruiter, service-provider, internal, other).

The config is loaded from the first file found at: `company_labels.yaml`, `company_labels.yml`, `company_labels.json`, or the equivalent in `data/`. You can also set `COMPANY_LABELS_PATH` in `.env` to point to a specific file.

3. **Run the stage:**

```bash
email-manager analyse --stage label_companies
email-manager analyse --stage label_companies -n 50  # label top 50 companies by email count
```

### How it works

For each unlabelled company, the AI receives:
- The company's homepage content (markdown excerpt, up to 3000 chars)
- Up to 20 recent email exchanges involving that company's domain
- The account owner (auto-detected from the most frequent sender)

It assigns 1-3 labels with confidence scores and reasoning. Labels are stored in the `company_labels` table.

### Viewing labels

```bash
email-manager companies                         # shows companies with their labels
```

## Database

SQLite with WAL mode and 30-second busy timeout. Stored at `data/email_manager.db`.

### Key Tables

| Table | Description |
|---|---|
| `emails` | Raw email data — message ID, headers, body, folder, timestamps. Immutable after insert. |
| `sync_state` | Per-folder sync cursor (UIDVALIDITY + last UID for IMAP, historyId for Gmail). |
| `contacts` | Aggregated contact info — name, company, email counts, first/last seen. |
| `companies` | Companies extracted from email domains, with email counts and homepage fetch status. |
| `company_labels` | AI-assigned relationship labels (investor, customer, vendor, etc.) with confidence and reasoning. |
| `event_ledger` | Append-only business events extracted from emails — type, domain, actor, date, detail, confidence. |
| `discussions` | Business discussions with category, workflow state, summary, participants. |
| `milestones` | Per-discussion milestone achievements with dates, evidence event IDs, and confidence. |
| `discussion_threads` | Maps discussions to email threads (many-to-many). |
| `discussion_state_history` | State transition history for discussions. |
| `calendar_events` | Synced Google Calendar events. |
| `contact_memories` | AI-generated memory profiles — relationship, discussions, key facts. |
| `feedback` | User corrections to events, milestones, and discussion classifications. |
| `threads` | Thread groupings computed from References/In-Reply-To headers. |
| `pipeline_runs` | Tracks which emails have been processed by which pipeline stage. |

### Email Threading

Threads are computed using a union-find algorithm:

1. Emails linked via `References` and `In-Reply-To` headers are grouped together.
2. Fallback: emails with the same normalised subject (stripped of Re:/Fwd: prefixes) within a 90-day window are grouped.

## CLI Commands

```
email-analyser accounts                          List configured email accounts
email-analyser sync                              Sync all accounts
email-analyser sync --account personal-gmail     Sync one account
email-analyser sync --list-folders               List available IMAP folders
email-analyser analyse                           Run all analysis stages
email-analyser analyse --stage extract_events    Run one stage
email-analyser analyse -s extract_events -s discover_discussions -s analyse_discussions
                                                 Run the discussion pipeline
email-analyser analyse --company acme.com --clean
                                                 Rebuild analysis for one company
email-analyser analyse --label investor --force  Reprocess all investor companies
email-analyser run                               Sync + analyse
email-analyser list                              Show recent emails
email-analyser search "quarterly report"         Full-text search
email-analyser companies                         List companies with labels
email-analyser company acme.com                  Detail view for one company
email-analyser discussions                       List discussions
email-analyser discussion 123                    Detail view for one discussion
email-analyser contacts                          List contacts by frequency
email-analyser contact alice@example.com         Detail view for one contact
email-analyser memory                            List contact memories
email-analyser memory alice@example.com          View/generate one contact's memory
email-analyser memory --all --limit 10           Generate for top 10 contacts
email-analyser status                            Sync state, pipeline progress, stats
email-analyser chat                              Interactive AI agent
```

## Interactive Chat Agent

`email-manager chat` starts a conversational session where you can:

- **Query your data** — "Show me all emails from Sarah in the last month", "What are my biggest projects?"
- **Refine project structure** — "Merge 'Q4 Planning' and 'Q4 Budget Review' into one project", "Rename project X to Y"
- **Explore contacts** — "Tell me about my interactions with alice@example.com"
- **View contact memories** — "What's my relationship with bob@company.com?"
- **Discuss the data model** — "What departments should I organise these projects into?"
- **Run SQL** — "How many emails did I get per month this year?" (read-only queries only)

The agent has access to 11 tools for querying and modifying your email data. For Claude API and Claude CLI backends, it uses native tool calling. For Ollama, it uses ReAct-style prompting.

## Project Structure

```
email-analyser/src/email_manager/
├── cli.py                          CLI entrypoint (Click)
├── config.py                       Settings, multi-account config (Pydantic)
├── db.py                           SQLite schema, migrations, helpers
├── models.py                       Data models (Email, Contact, Thread)
├── ingestion/
│   ├── imap_client.py              IMAP sync with Yahoo resilience
│   ├── gmail_client.py             Gmail API sync with historyId tracking
│   ├── calendar_client.py          Google Calendar sync
│   ├── parser.py                   Raw email bytes → structured Email objects
│   └── threading.py                Thread detection (union-find algorithm)
├── ai/
│   ├── base.py                     LLMBackend protocol
│   ├── claude_backend.py           Claude API implementation
│   ├── claude_cli_backend.py       Claude CLI subprocess implementation
│   ├── ollama_backend.py           Ollama HTTP implementation
│   ├── prompts.py                  Prompt templates
│   └── factory.py                  Backend selection
├── memory/
│   ├── base.py                     ContactMemory dataclass, MemoryBackend + MemoryStrategy protocols
│   ├── factory.py                  Backend and strategy selection
│   ├── sqlite_backend.py           SQLite memory storage
│   ├── markdown_backend.py         Markdown file memory storage
│   └── strategies/
│       ├── default.py              Default strategy (single AI call)
│       └── detailed.py             Detailed strategy (two AI calls)
├── pipeline/
│   ├── runner.py                   Pipeline orchestrator
│   └── stages.py                   Stage registry (8 stages)
├── analysis/
│   ├── base_extract.py             No-AI extraction (contacts, companies, co-email stats)
│   ├── homepage.py                 Concurrent homepage fetcher (no AI)
│   ├── company_labels.py           AI company relationship labelling
│   ├── events.py                   Event extraction from email threads
│   ├── discover_discussions.py     Discussion discovery from event clusters
│   ├── analyse_discussions.py      Milestone evaluation, state inference, summary
│   ├── contact_memory.py           Contact memory generation pipeline
│   └── discussions.py              (legacy) Direct discussion extraction
└── agent/
    ├── repl.py                     Interactive chat (Claude API / CLI / generic)
    ├── tools.py                    Agent tool definitions and handlers
    └── context.py                  Conversation memory management

web/
├── src/                            React + TypeScript frontend
│   ├── pages/                      Companies, Contacts, Discussions, Actions, Calendar
│   └── components/                 Badge, Pagination, SearchBar, Markdown, etc.
├── server/                         Express API server (SQLite → JSON)
└── discussion_categories.yaml      Category config (event types, milestones, states)
```

## Development

```bash
# Install with dev dependencies
uv sync --group dev

# Run tests
pytest tests/ -v
```
