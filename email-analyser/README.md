# email-analyser

Personal email data pipeline with AI-powered analysis. Syncs emails from Gmail/IMAP, extracts business events, clusters them into discussions, tracks relationship state, and proposes next actions.

## Setup

Requires Python 3.11+.

```bash
cd email-analyser
pip install -e .
```

### Configuration

1. Copy and edit `.env` for AI backend settings (model, API keys).
2. Copy `accounts.json.example` to `accounts.json` and configure your email accounts.
3. For Gmail: download OAuth credentials from Google Cloud Console and run `email-analyser auth`.

## Usage

### Typical workflow

```bash
# 1. Sync new emails (and calendar events) from all accounts
email-analyser sync

# 2. Incrementally update — auto-detects which companies have new emails
email-analyser update

# Or run the full pipeline from scratch
email-analyser analyse
```

### Sync

```bash
email-analyser sync                        # All accounts
email-analyser sync --account work         # One account only
email-analyser sync --no-calendar          # Skip calendar sync
email-analyser sync --remote               # Headless/SSH OAuth flow
```

### Update (fast incremental)

Processes new emails in a single LLM call per company. Extracts events, assigns to discussions, updates state/milestones, and proposes actions.

```bash
email-analyser update                      # Auto-detect dirty companies from change journal
email-analyser update --company acme.com   # Scope to one company
email-analyser update --label investor     # Scope to a label
email-analyser update --company-file cos.txt  # Scope from a file
email-analyser update --threshold 5        # Lower threshold for staged pipeline
```

When run with no arguments, `update` queries the change journal to find companies with unprocessed new emails and only processes those. It adapts strategy per company: few new threads use a single merged LLM call (fast), many new threads use the staged pipeline (thorough). The threshold defaults to 10 and is configurable with `--threshold`.

With `--agent`, uses the Claude Code SDK to run an autonomous agent session per company. The agent has read-only database tools to examine emails, discussions, and category config. It proposes a structured changeset (events, new discussions, state updates) which is displayed for review before being applied. For a single company, you're prompted to confirm; in batch mode, changes are auto-applied.

### Analyse (full pipeline)

Runs the multi-stage pipeline: extract contacts/companies, fetch homepages, label relationships, extract events, discover discussions, analyse state, propose actions, build contact memory.

```bash
email-analyser analyse                                    # Full pipeline, all companies
email-analyser analyse --company acme.com                 # One company
email-analyser analyse --label customer                   # All companies with label
email-analyser analyse -s extract_events -s discover_discussions  # Specific stages
email-analyser analyse --company acme.com --clean         # Re-process from scratch
email-analyser analyse --per-company --label investor     # All stages per company before moving on
```

Pipeline stages (in order):
1. `extract_base` — Extract contacts, companies, domains (no AI)
2. `fetch_homepages` — Download company homepages (no AI)
3. `label_companies` — Classify company relationships (AI)
4. `extract_events` — Extract business events from threads (AI)
5. `discover_discussions` — Cluster events into discussions (AI)
6. `analyse_discussions` — Evaluate milestones, state, summary (AI)
7. `propose_actions` — Suggest next steps for active discussions (AI)
8. `contact_memory` — Generate contact relationship profiles (AI)

### Viewing data

```bash
email-analyser companies                   # List companies
email-analyser companies --label investor --csv  # Export filtered list as CSV
email-analyser company acme.com            # Company detail
email-analyser discussions                 # List discussions
email-analyser discussion 42              # Discussion detail
email-analyser actions                     # List actions
email-analyser contacts                    # List contacts
email-analyser discussion-stats            # Funnel / state analysis
email-analyser labels                      # Company labels
email-analyser status                      # Sync and pipeline status
```

### Manual events and debrief

Capture things that happen outside email — meetings, calls, decisions.

```bash
# Structured event injection
email-analyser add-event --company acme.com \
  --type meeting_held --domain fundraising \
  --detail "Met with Sarah, discussed term sheet" \
  --date 2026-04-10 --actor me@example.com

# Freeform debrief (LLM extracts events and updates discussion)
email-analyser debrief --company acme.com "Met with Sarah, they accepted our terms"
email-analyser debrief --discussion 42 "Call went well, pilot starts Monday"
echo "Had a call with Bob" | email-analyser debrief --company acme.com
```

### Discussion management

Correct AI mistakes or manually update discussion state.

```bash
email-analyser update-discussion 42 --state signed --reason "Signed at meeting"
email-analyser update-discussion 42 --title "Acme Series B Investment"
email-analyser update-discussion 42 --company newco.com
email-analyser merge-discussions 42 43 --reason "Same deal, duplicate"
```

### Review and evaluation

Every AI analysis step produces a `ProposedChanges` object that is snapshotted to the database. This enables reviewing what the LLM decided, annotating correctness, and feeding corrections back into prompts.

```bash
email-analyser review                      # List recent processing runs
email-analyser review 42                   # Show proposed changes for run #42
email-analyser review 42 --annotate        # Mark items correct/incorrect/missing

email-analyser eval                        # Show precision metrics from annotations
email-analyser eval --company acme.com     # Scoped metrics

email-analyser history acme.com            # Show processing run chain for a company
email-analyser rollback 42                 # Undo run #42 and all later runs in that chain
email-analyser rollback 42 --dry-run       # Preview what would be deleted
```

### Learned rules

Corrections from review annotations can be distilled into rules that are automatically injected into future LLM prompts.

```bash
email-analyser learn list                  # Show active rules
email-analyser learn add -l events -r "Scheduling emails should use the scheduling domain"
email-analyser learn add -l labels --category pharma -r "CROs are vendors, not partners"
email-analyser learn remove --rule-id 3    # Deactivate a rule
```

### Other commands

```bash
email-analyser auth                        # Authenticate Gmail accounts
email-analyser accounts                    # List configured accounts
email-analyser search "term sheet"         # Search emails
email-analyser memory                      # View/generate contact memory
email-analyser chat                        # Interactive agent REPL
email-analyser reset --company acme.com    # Delete all analysis for a company
email-analyser reset -c acme.com --from-stage discover_discussions  # Keep events, redo discussions+
email-analyser reset -c acme.com --from-stage propose_actions       # Just redo proposed actions
email-analyser delete-contact user@co.com  # Delete all emails for a contact
```

## Architecture

- **SQLite database** (default) — all data stored locally. PostgreSQL also supported (`uv sync --extra postgres`, set `DB_BACKEND=postgres` and `DB_URL` in `.env`)
- **AI backend** — Claude API (default), Claude CLI subprocess, or Ollama for local LLMs
- **Change journal** — tracks what changed (new emails, new events) so `update` can auto-scope to only the companies that need processing
- **Thread batching** — small threads (1-3 emails, <2K chars) are grouped into batches and processed in a single LLM call, reducing the number of extraction calls by ~2-3x
- **Pipeline stages** — modular, can be run individually or together, scoped by company/label/date filters
- **ProposedChanges** — all AI stages (staged, quick, agent) produce a structured changeset that is snapshotted for evaluation before being applied to the database
- **Processing run chain** — each run records its parent run, the model and prompt hash used, and the email cutoff date. This forms a per-company history that can be replayed or rolled back
- **Feedback loop** — review annotations → learned rules → injected into prompts → measurable quality improvement
