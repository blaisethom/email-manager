# Better Updating: Design Plan

## Status

Items 1–6 from the original plan are **implemented**. Parallelism (async backends + `--concurrency` flag) is also **implemented**. The latest major addition is the **feedback and evaluation system** with ProposedChanges snapshots, per-company processing run chains, prompt versioning, and rollback.

### What's been built

| # | Feature | Status |
|---|---------|--------|
| 1 | Change journal + auto-scoped update | **Done.** `change_journal` table, `get_dirty_company_domains()`, `update` with no args auto-scopes. |
| 2 | Thread batching for event extraction | **Done.** Small threads (≤3 emails, ≤2K chars) batched into ~8K-char groups. Batched prompt with thread separators. |
| 3 | Unified incremental update | **Done.** `update` command adaptively routes: quick (≤10 threads, single LLM call) or staged (>10 threads, full pipeline). Threshold configurable via `--threshold`. |
| 4 | Source type generalization | **Done.** `source_type` + `source_id` columns on `event_ledger`. Values: email, calendar, manual, debrief. |
| 5 | Manual events + debrief | **Done.** `add-event` CLI for structured injection, `debrief` CLI for freeform LLM-assisted entry. |
| 6 | Discussion management | **Done.** `update-discussion` (state/title/company), `merge-discussions`, `reset --from-stage`. |

### Additional features built beyond original plan

| Feature | Description |
|---------|-------------|
| **Agent mode** | `--agent` flag on `update`. Uses Claude Agent SDK with read-only DB tools + incremental `add_event`/`add_discussion`/`update_discussion` tools. Agent proposes changes; we review and apply. |
| **Provenance tracking** | `processing_runs` table with `run_id` stamped on `event_ledger`, `discussions`, `milestones`, `proposed_actions`. Tracks mode, model, token usage per run. |
| **Token tracking** | `llm_calls` table records individual LLM calls per run. Backends track input/output tokens. Web UI shows usage breakdown by stage. |
| **Sub-discussions** | Scheduling/logistics are separated as sub-discussions with `parent_id` pointing to the main investment/deal discussion. |
| **PostgreSQL support** | `db_postgres.py` wrapper with SQL dialect translation. `migrate-db` command copies SQLite → PG. Web server supports both backends. |
| **Web insights tab** | Company detail page has "Insights & Provenance" tab showing discussion health, freshness, events by domain, processing history, next steps, and token usage. |
| **Parallelism** | Async LLM backends (`acomplete`/`acomplete_json`), `--concurrency` flag on `analyse` and `update`, semaphore-gated parallel extraction within stages. |
| **Feedback & evaluation** | All AI stages produce `ProposedChanges` objects snapshotted to `processing_runs.proposed_changes_json`. `review` CLI to inspect/annotate, `eval` for precision metrics, `learn` for rules injected into prompts. |
| **Per-company run chains** | `processing_runs` now always per-company with `parent_run_id` chain, `email_cutoff_date` input boundary, and `prompt_hash` for versioning. |
| **Rollback** | `rollback <run_id>` deletes all derived data from that run and later runs in the chain. |
| **Prompt versioning** | SHA-256 hash of the system prompt (including learned rules) stored on each run. Detects when prompts change and stages need re-running. |

---

## Current Architecture

### Processing modes

```
email-analyser update --company acme.com          # LLM mode: quick or staged
email-analyser update --agent --company acme.com  # Agent mode: Claude Agent SDK
```

**LLM mode** (default):
- `quick` path: single `complete_json` call per company. LLM receives all new emails + discussion context, returns events + discussion updates as JSON.
- `staged` path: sequential stages — `extract_events` → `discover_discussions` → `analyse_discussions` → `propose_actions`. Each stage makes one LLM call per thread/company/discussion.

**Agent mode** (`--agent`):
- Claude Agent SDK session with MCP tools for reading DB + incremental change tools.
- Agent processes thread by thread, calling `add_event`/`add_discussion`/`update_discussion` as it goes.
- Returns a `ProposedChanges` changeset applied via `apply_changes()`.

### Unified save path

All modes (staged, quick, agent) produce `ProposedChanges` objects and go through:
```
LLM output → ProposedChanges → apply_changes(conn, changes, company_id, domain, mode, model, token_tracker, prompt_hash)
```

`apply_changes`:
1. Creates a per-company `processing_runs` record with chain tracking (`parent_run_id`, `email_cutoff_date`, `prompt_hash`)
2. Snapshots the full `ProposedChanges` JSON for later review/evaluation
3. Applies events, discussions, milestones, actions, labels, and event assignments
4. Stamps `run_id` on all derived data
5. Records token usage and `llm_calls`
6. Records thread-level changes in the change journal

### LLM backends

| Backend | Token tracking | Call interface |
|---------|---------------|----------------|
| `ClaudeBackend` (API) | Exact (from response.usage) | Sync |
| `ClaudeCLIBackend` (CLI subprocess) | Estimated (~4 chars/token) | Sync (Popen with activity monitoring) |
| `OllamaBackend` | Exact (from eval_count) | Sync |

All backends implement `LLMBackend` protocol with `token_tracker: TokenTracker` property.

### Database

- SQLite (default) or PostgreSQL (`DB_BACKEND=postgres`)
- Schema version 23, 28+ tables
- All derived data has `run_id` FK to `processing_runs`
- Shared `.env` at repo root, symlinked from `email-analyser/` and `web/`

---

## Parallelism (Implemented)

Parallelism is implemented. Async LLM backends, within-stage concurrency, and `--concurrency` CLI flag are all working. The original design below is preserved for reference.

### Goal

Speed up batch processing by running independent LLM calls concurrently.

### What's parallelisable

| Level | Unit | Independent? | Current calls | Parallelism type |
|-------|------|-------------|---------------|-----------------|
| **Company** | Each company in an `update` batch | Yes | 1 quick call or N staged calls | Across companies |
| **Event extraction** | Each thread batch or large thread | Yes | 1 call per batch/thread | Within a company |
| **Discussion discovery** | Each company | Yes | 1 call per company | Across companies |
| **Discussion analysis** | Each discussion | Yes | 1 call per discussion | Within a company |
| **Action proposals** | Each discussion | Yes | 1 call per discussion | Within a company |

What's **not** parallelisable: the four stages within a single company's staged pipeline (extract → discover → analyse → propose) — each depends on the prior stage's output.

### Design

#### A. Async LLM backend

Add async methods to `LLMBackend`:

```python
class LLMBackend(Protocol):
    # Existing sync methods stay for backwards compatibility
    def complete(self, system: str, user: str, temperature: float = 0.3) -> str: ...
    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict: ...

    # New async methods
    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str: ...
    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict: ...
```

Default implementation wraps sync in a thread executor. For `ClaudeCLIBackend`, use `asyncio.create_subprocess_exec` natively — this is the main win since `Popen` subprocesses can genuinely run in parallel.

For `ClaudeBackend` (API), use `anthropic.AsyncAnthropic`.

#### B. Concurrency control

```python
# Global semaphore — configurable via CLI or env
MAX_CONCURRENT_LLM_CALLS = 5  # default, tune based on subscription

semaphore = asyncio.Semaphore(MAX_CONCURRENT_LLM_CALLS)

async def rate_limited_call(backend, system, user):
    async with semaphore:
        return await backend.acomplete_json(system, user)
```

The semaphore caps total concurrent subprocess calls system-wide, regardless of which stage or company is making them.

#### C. Parallelism within stages

**`extract_events`** — highest impact, most calls:
```python
async def extract_events_parallel(conn, backend, thread_ids, ...):
    tasks = []
    for batch in batches:
        tasks.append(process_batch_async(backend, batch, ...))
    for thread_id in large_thread_ids:
        tasks.append(process_thread_async(backend, thread_id, ...))

    results = await asyncio.gather(*tasks)
    # Save all events after all calls complete
    for events in results:
        _save_events(conn, events, run_id=run_id)
```

Key: LLM calls run in parallel, but DB writes remain sequential (SQLite doesn't support concurrent writes; PG does but we keep it simple).

**`analyse_discussions`** and **`propose_actions`** — same pattern:
```python
async def analyse_discussions_parallel(conn, backend, discussion_ids, ...):
    tasks = [analyse_one(backend, disc_id, context) for disc_id in discussion_ids]
    results = await asyncio.gather(*tasks)
    for disc_id, result in zip(discussion_ids, results):
        save_analysis(conn, disc_id, result)
```

**`discover_discussions`** — typically 1 call per company, limited benefit from parallelism within a company. Parallelise across companies instead.

#### D. Parallelism across companies

The `update` command's company loop is the coarsest parallelism level:

```python
async def update_companies_parallel(domains, backend, conn, ...):
    sem = asyncio.Semaphore(MAX_CONCURRENT_LLM_CALLS)

    async def process_one(domain):
        async with sem:
            # Quick path: single call
            # Staged path: sequential stages but calls within stages are parallel
            ...

    await asyncio.gather(*[process_one(d) for d in domains])
```

For quick mode (single call per company), this directly parallelises the LLM calls. For staged mode, the stages within a company run sequentially but the LLM calls within each stage run in parallel.

**DB connection handling**: SQLite requires a single writer. Options:
- Use a write queue: parallel LLM calls, sequential DB writes
- For PostgreSQL: use connection pool, write in parallel

#### E. CLI integration

```bash
# Default: 5 concurrent calls
email-analyser update --label investor

# Tune concurrency
email-analyser update --label investor --concurrency 3

# Disable parallelism
email-analyser update --label investor --concurrency 1

# Analyse stage only, parallel
email-analyser analyse --stage analyse_discussions --label investor --concurrency 10
```

### Implementation order

| Step | What | Effort | Impact |
|------|------|--------|--------|
| 1 | Add `acomplete`/`acomplete_json` to backends | Medium | Foundation |
| 2 | Parallelise `extract_events` (thread batches) | Medium | Highest — most calls per company |
| 3 | Parallelise `analyse_discussions` + `propose_actions` | Low | Good — many discussions per company |
| 4 | Parallelise company loop in `update` | Medium | Good for batch runs across many companies |
| 5 | Add `--concurrency` flag | Low | User control |

### How parallelism interacts with agent mode

Agent mode is fundamentally different from staged pipeline parallelism:

**Staged pipeline**: We control the workflow and can parallelise the LLM calls we make (thread batches, discussions, companies). We fire N concurrent subprocesses, collect results, write to DB.

**Agent mode**: The agent controls its own workflow. It reads data, reasons about it, and calls tools incrementally. We **cannot parallelise within a single agent session** — the agent's reasoning is inherently sequential. But we **can run multiple agent sessions in parallel** (one per company), each with its own tool set bound to that company's data.

```python
# Parallel agent sessions across companies
async def run_agents_parallel(domains, ...):
    sem = asyncio.Semaphore(concurrency)
    async def one(domain):
        async with sem:
            return await _run_agent_for_company(conn, domain, ...)
    results = await asyncio.gather(*[one(d) for d in domains])
```

The agent SDK's `query()` is already async, so this works naturally.

### Cross-company discussions

**Current model**: Each discussion is assigned to one company. Threads that involve multiple companies (e.g. "SCOR Ventures Intro Meeting via NordicNinja") are assigned to the "primary" company. This works well in practice — the discussion is *about* one relationship even if others facilitate it.

**Impact on parallelism**: Since discussions belong to one company and companies are processed independently, there are no cross-company data dependencies. Two agent sessions processing different companies won't conflict — they read/write different discussion sets.

**Edge case**: A thread involving companies A and B could be picked up by both when processing in parallel. The `INSERT OR IGNORE` on events handles this — if company A's session already extracted events from a shared thread, company B's session will see them (or skip duplicates). The same thread may produce different events depending on which company's perspective the LLM takes, but this is fine — the events are tagged with `discussion_id` which scopes them to the right company.

**Future consideration**: If we want first-class cross-company discussion linking (e.g. "this intro connects investor X to portfolio company Y"), that's a separate feature — a `discussion_links` table. Not needed for parallelism.

### Constraints and risks

- **Subscription rate limits**: The Claude CLI subscription may cap concurrent calls. Start with `--concurrency 3`, test empirically, tune up.
- **SQLite write locking**: SQLite allows only one writer at a time. Parallel LLM calls must queue their DB writes. PostgreSQL doesn't have this limitation — use connection pool.
- **Token tracker thread safety**: `TokenTracker.record()` is called from multiple coroutines/threads — add a `threading.Lock` or use `asyncio`-safe collection.
- **Progress reporting**: With parallel calls, the progress bar needs to show aggregate progress rather than per-thread updates. Use a shared counter.
- **Error handling**: One failed LLM call shouldn't kill the whole batch. Use `asyncio.gather(return_exceptions=True)` and log/skip failures.
- **Memory**: Each subprocess (CLI backend) uses ~50-100MB. With 10 concurrent calls, that's 0.5-1GB. Should be fine for most machines.
- **Agent mode concurrency**: Each agent session spawns its own `claude` subprocess via the Agent SDK. Multiple sessions = multiple subprocesses. The Agent SDK handles its own session lifecycle, but we need to ensure the DB connection (especially SQLite) isn't shared unsafely across concurrent async tasks.
