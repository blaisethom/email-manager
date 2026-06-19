# Continuous Pipeline Scheduler

The email-manager pipeline can run as a continuous background process using
[Prefect](https://www.prefect.io/) as the workflow orchestrator. Three flows
run on staggered schedules, keeping the system's view of your contacts and
companies as current as your LLM budget allows.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        email / calendar / HubSpot                   │
└─────────────────────────┬───────────────────────────────────────────┘
                           │ (raw data)
                    every 10 min
                           │
                    ┌──────▼──────┐
                    │ ingest_flow │  Gmail, IMAP, Google Calendar, HubSpot
                    └──────┬──────┘
                           │ (emails, calendar events, CRM records)
                    every 30 min
                           │
                    ┌──────▼───────┐
                    │ enrich_flow  │  extract_base → homepages, labels, index
                    └──────┬───────┘
                           │ (threads, contacts, search index)
                     every 60 min
                           │
                    ┌──────▼──────┐
                    │  ai_flow    │  per-company: events → discussions → actions
                    └─────────────┘
```

### Why three flows?

Each layer has different cost and frequency characteristics:

| Flow | Cadence | LLM calls | Notes |
|------|---------|-----------|-------|
| `ingest_flow` | 10 min | None | Network I/O bound; cheap to run often |
| `enrich_flow` | 30 min | Label companies only | Mostly CPU/DB work |
| `ai_flow` | 60 min | Heavy (events → discussions → actions) | Rate-limited by `ai-llm` slot |

The company-scoped `build_search_index` stage is skipped in scoped runs (via
`skip_when_scoped=True`); `enrich_flow` handles the full-corpus rebuild once
threads are current.

### Work unit: the company

The AI flow uses the **`change_journal`** to discover which company domains have
received new data since they were last processed. Each company is a separate
Prefect task, so they run in parallel subject to the `ai-llm` concurrency limit.

---

## Setup

### 1. Install Prefect

```bash
cd email-analyser
pip install -e ".[postgres,scheduler]"
```

### 2. Start the Prefect server

For local development, SQLite works fine:

```bash
prefect server start
# Open http://localhost:4200 in your browser
```

For production, point it at a Postgres database:

```bash
export PREFECT_SERVER_DATABASE_CONNECTION_URL="postgresql+asyncpg://user:pass@host/prefect"
prefect server start --host 0.0.0.0
```

Or use Docker Compose (see [Docker section](#docker) below).

### 3. Create the work pool

A **work pool** is where the server sends flow runs. The worker picks them up
and spawns subprocesses.

```bash
prefect work-pool create email-manager-pool --type process
```

### 4. Set the AI concurrency limit

This is the most important safety valve. It caps how many companies can run
LLM calls simultaneously across all parallel tasks.

```bash
prefect concurrency-limit create ai-llm 3
```

Increase `3` if you have a high Claude rate limit tier; decrease it to stay
under quota. Changes take effect immediately — no restart needed.

### 5. Deploy the flows

From the `email-analyser` directory:

```bash
prefect deploy --all
```

This reads `prefect.yaml` and registers all four deployments (ingest, enrich,
ai-analysis, full-run) with their schedules.

### 6. Start the worker

```bash
prefect worker start --pool email-manager-pool
```

Leave this running. The worker polls the server for queued runs and executes
them as subprocesses in the current Python environment.

---

## Flows

### `ingest_flow` (every 10 min)

Pulls raw data from all configured sources in parallel:

- **Email sync** — one task per account (Gmail or IMAP), all concurrent
- **Calendar sync** — Google Calendar events for Gmail accounts
- **HubSpot sync** — companies → contacts → deals → notes → email engagements
  → deal discussions → repair thread links → link notes to discussions

Calendar failures are non-fatal (logged but don't fail the flow). HubSpot
failures retry twice with a 2-minute delay.

### `enrich_flow` (every 30 min)

Runs global enrichment stages in dependency order:

1. `extract_base` — parses raw emails into threads, contacts, and company records
2. (parallel) `hubspot_task_enrichment`, `fetch_homepages`, `label_companies`
3. `build_search_index` — rebuilds thread/discussion FTS + embeddings

The search index rebuild is incremental by default (only new threads); `force=True`
can be passed via a manual Prefect run parameter if you need a full rebuild.

### `ai_flow` (every hour, at :05)

Interprets company data using LLMs:

1. Reads `change_journal` to find dirty company domains
2. Takes the first N companies (default 50 per run — see `ai_company_batch_size`)
3. Fans out one task per company, each running:
   - `extract_events` — extract structured events from thread content
   - `discover_discussions` — cluster events into discussions
   - `analyse_discussions` — update discussion state and summaries
   - `propose_actions` — suggest next steps
   - `contact_memory` — build per-contact relationship summaries
4. Each task acquires one `ai-llm` concurrency slot before making LLM calls

Companies deferred to the next run are logged. If the backlog grows large,
increase `ai_company_batch_size` or run `full-run` manually.

### `full_run_flow` (manual only)

Runs ingest → enrich → AI sequentially in a single flow. Useful for:

- Initial bootstrapping a fresh database
- Ad-hoc full refreshes
- Testing the whole stack end-to-end

Trigger from the Prefect UI or CLI:

```bash
prefect deployment run 'email-manager-full-run/full-run'
```

---

## Configuration

### Scheduler settings (`scheduler/config.py`)

| Field | Default | Effect |
|-------|---------|--------|
| `ai_concurrency_limit_name` | `"ai-llm"` | Prefect concurrency limit name |
| `ai_company_batch_size` | `50` | Max companies per AI flow run |
| `work_pool` | `"email-manager-pool"` | Prefect work pool name |
| `ai_stages` | all 5 AI stages | Which stages to run per company |

Edit `src/email_manager/scheduler/config.py` to change defaults.

### Schedule overrides

Edit `prefect.yaml` and redeploy:

```bash
prefect deploy --all
```

Or change the schedule in the Prefect UI without touching the file.

---

## Docker

For production, run the full stack with Docker Compose:

```bash
# Bring up Prefect server + metadata DB + worker
docker compose -f docker-compose.prefect.yml up -d

# Check worker logs
docker compose -f docker-compose.prefect.yml logs -f worker
```

The worker container:
1. Waits for the Prefect server to become healthy
2. Creates the work pool and concurrency limit (idempotent)
3. Deploys all flows from `prefect.yaml`
4. Starts the worker loop

The app `.env` file is passed through so all secrets (DB_URL, ANTHROPIC_API_KEY,
etc.) are available to flow tasks.

---

## Monitoring

Open the Prefect UI at `http://localhost:4200` to see:

- Live flow run progress (task tree with status)
- Historical run logs and durations
- Concurrency limit usage
- Failed runs with full stack traces

To add alerting, configure a [Prefect notification](https://docs.prefect.io/v3/automate/events/automations-triggers)
in the UI (Automations → Create) to notify a Slack channel or email on flow failure.

---

## Manual triggers

Run any flow on demand:

```bash
# Process a specific company immediately
prefect deployment run 'email-manager-ai/ai-analysis' \
  --param batch_size=1

# Force a full ingest + enrich cycle
prefect deployment run 'email-manager-full-run/full-run'

# Watch the run live
prefect flow-run ls --limit 5
```

---

## Relation to existing `pipeline_jobs` table

The web UI's **Jobs** panel continues to work exactly as before. When you click
"Run pipeline" in the browser, a row is inserted into `pipeline_jobs` and the
existing job runner (`web/server/jobs.ts`) picks it up. The Prefect scheduler
and the web-triggered jobs are independent; they can run concurrently because
the AI stages use row-level locking in Postgres.

The Prefect-scheduled flows are for **background continuous processing**. The
web UI job queue is for **on-demand, user-triggered runs** (e.g. "process this
company now"). Both coexist without conflict.

---

## Adding new data sources

To add a new data source to the ingest flow:

1. Write a sync function (or wrap an existing CLI command) in a Prefect `@task`
   in `scheduler/tasks.py`
2. Submit the task in `ingest_flow()` in `scheduler/flows.py`
3. Redeploy: `prefect deploy --name ingest`

No server restart is needed — Prefect picks up the updated flow code on the
next scheduled run.
