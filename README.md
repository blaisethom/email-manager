# Email Manager

A personal email data pipeline that syncs your emails into a local SQLite database, uses AI to categorise them into projects, extracts entities, summarises threads, builds a contact CRM, and provides an interactive chat agent for exploring and refining your email data.

All data stays local. You choose the AI backend.

## Quick Start

```bash
# Install
uv sync

# Configure
cp .env.example .env
# Edit .env with your email and AI settings

# Sync emails
email-manager sync

# Run AI analysis
email-manager analyse

# Explore interactively
email-manager chat
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────────────┐
│  Email       │     │   SQLite     │     │  AI Backend              │
│  Sources     │────>│   Database   │<───>│  (Claude/CLI/Ollama)     │
│  IMAP/Gmail  │     │              │     └──────────────────────────┘
└─────────────┘     │  emails      │              │
                    │  threads     │     ┌────────┴─────────────────┐
                    │  contacts    │     │  Pipeline Stages         │
                    │  projects    │     │  1. Categorise           │
                    │  entities    │     │  2. Extract Entities     │
                    │  pipeline_   │     │  3. Summarise Threads    │
                    │    runs      │     │  4. Build CRM            │
                    └──────────────┘     └──────────────────────────┘
                          │
                    ┌─────┴──────┐
                    │  CLI / Chat │
                    │  Agent      │
                    └────────────┘
```

### Data Flow

1. **Sync** — Emails are fetched from your mail server (IMAP or Gmail API) and stored as-is in SQLite. Incremental sync means only new emails are fetched on subsequent runs.
2. **Analyse** — The AI pipeline processes emails in stages. Each stage is independently resumable — if interrupted, it picks up where it left off.
3. **Explore** — CLI commands and the interactive chat agent let you query, reorganise, and refine the data.

## Email Sources

### Gmail (recommended)

Uses the Gmail API with OAuth2. Supports incremental sync via Gmail's `historyId` mechanism.

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project, enable the Gmail API
3. Create OAuth 2.0 credentials (Desktop application type)
4. Download the JSON file to `data/gmail_credentials.json`
5. Set in `.env`:
   ```
   EMAIL_BACKEND=gmail
   GMAIL_CREDENTIALS_PATH=data/gmail_credentials.json
   ```
6. Run `email-manager sync` — a browser window will open for OAuth consent on first run. The token is saved locally for future use.

### IMAP

Works with any email provider that supports IMAP (Fastmail, ProtonMail Bridge, self-hosted, etc.).

```
EMAIL_BACKEND=imap
IMAP_HOST=imap.example.com
IMAP_USER=user@example.com
IMAP_PASSWORD=your-password
IMAP_PORT=993
IMAP_USE_SSL=true
IMAP_FOLDERS=INBOX,Sent
```

Uses UID-based incremental sync with UIDVALIDITY tracking.

## AI Backends

Three backends, all behind a common interface. Switch between them by changing `AI_BACKEND` in `.env`.

| Backend | Setting | Auth | Best for |
|---|---|---|---|
| Claude API | `AI_BACKEND=claude` | `ANTHROPIC_API_KEY` | Highest quality, structured JSON output |
| Claude CLI | `AI_BACKEND=claude-cli` | Your existing `claude` CLI auth | No API key needed, uses your CLI subscription |
| Ollama | `AI_BACKEND=ollama` | None (local) | Full privacy, no data leaves your machine |

### Claude CLI

If you have the [Claude CLI](https://docs.anthropic.com/en/docs/claude-cli) installed and authenticated, this is the simplest option — no API key needed:

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

The analysis pipeline has four stages, each independently resumable:

| Stage | What it does |
|---|---|
| `categorise` | Assigns each email to 1-3 projects using AI. Creates projects automatically. |
| `extract_entities` | Extracts people, companies, topics, and action items from each email. |
| `summarise_threads` | Generates summaries for email threads. |
| `build_crm` | Aggregates contact statistics (email counts, companies, first/last seen). SQL-based, no AI needed. |

Run all stages:
```bash
email-manager analyse
```

Run a specific stage:
```bash
email-manager analyse --stage categorise
email-manager analyse --stage build_crm
```

### Resumability

Each stage tracks its progress in the `pipeline_runs` table. If interrupted, re-running the same stage skips already-processed emails. To reprocess everything:

```sql
-- Via the chat agent or sqlite3 CLI
DELETE FROM pipeline_runs WHERE stage = 'categorise';
```

### Batching

Emails are sent to the AI in configurable batches (default 10) to reduce API calls. If a batch fails, individual emails are retried. Set batch size in `.env`:

```
AI_BATCH_SIZE=10
```

## Database

SQLite with WAL mode. Stored at `data/email_manager.db` by default.

### Key Tables

| Table | Description |
|---|---|
| `emails` | Raw email data — message ID, headers, body, folder, timestamps. Immutable after insert. |
| `sync_state` | Per-folder sync cursor (UIDVALIDITY + last UID for IMAP, historyId for Gmail). |
| `threads` | Thread groupings computed from References/In-Reply-To headers, with AI-generated summaries. |
| `projects` | AI-discovered or user-created project categories. |
| `email_projects` | Many-to-many mapping of emails to projects, with confidence scores. |
| `contacts` | Aggregated contact info — name, company, email counts, first/last seen. |
| `entities` | Extracted entities (person, company, topic, action_item) per email. |
| `pipeline_runs` | Tracks which emails have been processed by which pipeline stage. |

### Email Threading

Threads are computed using a union-find algorithm:

1. Emails linked via `References` and `In-Reply-To` headers are grouped together.
2. Fallback: emails with the same normalised subject (stripped of Re:/Fwd: prefixes) within a 90-day window are grouped.

## CLI Commands

```
email-manager sync                              Fetch new emails from IMAP or Gmail
email-manager sync --backend gmail              Override backend for this run
email-manager analyse                           Run all AI analysis stages
email-manager analyse --stage categorise        Run a specific stage
email-manager run                               Sync + analyse in one command
email-manager list                              Show recent emails
email-manager list --limit 50                   Show more emails
email-manager search "quarterly report"         Full-text search
email-manager projects                          List projects with email counts
email-manager threads                           List threads with summaries
email-manager contacts                          List contacts by frequency
email-manager contact alice@example.com         Detail view for one contact
email-manager status                            Sync state, pipeline progress, stats
email-manager chat                              Interactive AI agent
```

## Interactive Chat Agent

`email-manager chat` starts a conversational session where you can:

- **Query your data** — "Show me all emails from Sarah in the last month", "What are my biggest projects?"
- **Refine project structure** — "Merge 'Q4 Planning' and 'Q4 Budget Review' into one project", "Rename project X to Y"
- **Explore contacts** — "Tell me about my interactions with alice@example.com"
- **Discuss the data model** — "What departments should I organise these projects into?", "Help me structure my projects into workstreams"
- **Run SQL** — "How many emails did I get per month this year?" (read-only queries only)

The agent has access to tools for querying and modifying your email data. For Claude API and Claude CLI backends, it uses native tool calling. For Ollama, it uses ReAct-style prompting.

## Project Structure

```
src/email_manager/
├── cli.py                  CLI entrypoint (Click)
├── config.py               Settings from .env (Pydantic)
├── db.py                   SQLite schema and helpers
├── models.py               Data models (Email, Contact, Thread, Project)
├── ingestion/
│   ├── imap_client.py      IMAP sync with incremental UID tracking
│   ├── gmail_client.py     Gmail API sync with historyId tracking
│   ├── parser.py           Raw email bytes → structured Email objects
│   └── threading.py        Thread detection (union-find algorithm)
├── ai/
│   ├── base.py             LLMBackend protocol
│   ├── claude_backend.py   Claude API implementation
│   ├── claude_cli_backend.py  Claude CLI subprocess implementation
│   ├── ollama_backend.py   Ollama HTTP implementation
│   ├── prompts.py          All prompt templates
│   └── factory.py          Backend selection
├── pipeline/
│   ├── runner.py           Pipeline orchestrator
│   ├── stages.py           Stage registry
│   └── batch.py            Batching utilities
├── analysis/
│   ├── categoriser.py      Email → project assignment
│   ├── entities.py         Entity extraction
│   ├── summariser.py       Thread summarisation
│   └── crm.py              Contact relationship aggregation
└── agent/
    ├── repl.py             Interactive chat (Claude API / CLI / generic)
    ├── tools.py            Agent tool definitions and handlers
    └── context.py          Conversation memory management
```

## Development

```bash
# Install with dev dependencies
uv sync --group dev

# Run tests
pytest tests/ -v
```
