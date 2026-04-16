# Declarative Pipeline Stage Definitions Refactor

## Context

The pipeline runner and stage registry have grown organically. Stage metadata (dependencies, scope, prompt hash sources, accepted parameters) is scattered across hardcoded constants (`GLOBAL_STAGES`, `NO_AI_STAGES`), `inspect.signature()` introspection, and duplicated if/elif chains for prompt hashes. This refactor consolidates all stage metadata into declarative `StageDefinition` objects and uses `graphlib.TopologicalSorter` for stage ordering — zero new dependencies, same external behavior.

## Files to modify

1. **`email-analyser/src/email_manager/pipeline/stages.py`** — add `StageDefinition` dataclass, prompt hash functions, new `STAGES` registry
2. **`email-analyser/src/email_manager/pipeline/runner.py`** — consume `STAGES` metadata, eliminate `inspect.signature()`, deduplicate prompt hash logic, use topo sort

No changes to: CLI (`cli.py`), stage implementations (`events.py`, `analyse_discussions.py`, etc.), database layer, or tests.

---

## Step 1: Add `StageDefinition` and prompt hash functions to `stages.py`

Add at the top of `stages.py` (after existing imports):

- `StageScope` enum with `GLOBAL` and `PER_COMPANY` values
- `StageDefinition` frozen dataclass with fields:
  - `name: str`
  - `run: Callable` — the existing wrapper function
  - `scope: StageScope` — replaces `GLOBAL_STAGES` set
  - `needs_ai: bool` — replaces `NO_AI_STAGES` set
  - `accepts: frozenset[str]` — kwargs this stage handles (replaces `inspect.signature()`)
  - `depends_on: frozenset[str]` — explicit dependency graph
  - `prompt_hash_fn: Callable[[conn, config], str | None] | None` — eliminates the duplicated if/elif chains

Add 5 prompt hash helper functions (one per stage that has prompt tracking):
- `_hash_extract_events` — `EXTRACT_EVENTS_SYSTEM + format_rules_block(conn, "events")`
- `_hash_discover_discussions` — `DISCOVER_SYSTEM` (no rules layer)
- `_hash_analyse_discussions` — `ANALYSE_SYSTEM + format_rules_block(conn, "discussion_updates")`
- `_hash_propose_actions` — `PROPOSE_SYSTEM + format_rules_block(conn, "actions")`
- `_hash_label_companies` — `_build_system_prompt(labels_config) + format_rules_block(conn, "labels")`

These use lazy imports (same pattern as the existing wrapper functions).

## Step 2: Build `STAGES` registry in `stages.py`

Replace `ALL_STAGES` dict with a `STAGES: dict[str, StageDefinition]` registry. Each entry maps stage name to its `StageDefinition`. The dependency graph:

```
extract_base (GLOBAL, no AI)
  ├── fetch_homepages (no AI)
  ├── label_companies
  ├── extract_events
  │     └── discover_discussions
  │           └── analyse_discussions
  │                 └── propose_actions
  └── contact_memory
```

Keep backward-compat alias: `ALL_STAGES = {name: defn.run for name, defn in STAGES.items()}`

This alias is used by runner.py (4 references) and ensures any external code importing `ALL_STAGES` still works. The `update` command in cli.py imports individual `run_*` functions directly — unaffected.

## Step 3: Rewrite `_run_stage()` in `runner.py`

Replace `inspect.signature()` kwargs introspection (lines 60-68) with `defn.accepts` membership check:

```python
defn = STAGES[stage_name]
kwargs = dict(console=console, limit=limit, force=force)
optional = {"clean": clean, "company": company, "label": label,
            "exclude": exclude, "contact": contact}
for key, val in optional.items():
    if key in defn.accepts and val:
        kwargs[key] = val
if "concurrency" in defn.accepts and concurrency > 1:
    kwargs["concurrency"] = concurrency
count = defn.run(conn, backend, config, **kwargs)
```

Remove `import inspect` from runner.py.

## Step 4: Rewrite `_is_stage_stale()` in `runner.py`

Replace the 5-branch if/elif chain (lines 119-139) with:

```python
defn = STAGES.get(stage)
if defn and defn.prompt_hash_fn is not None:
    current_hash = defn.prompt_hash_fn(conn, config)
    if current_hash and current_hash != run["prompt_hash"]:
        return True
```

Add `config: Config` parameter to the function signature (needed for `_hash_label_companies` which accesses `company_labels_path`). Update both call sites in `run_pipeline()`.

## Step 5: Rewrite prompt hash block in `_filter_by_staleness()`

Replace the duplicated 24-line if/elif chain (lines 293-316) with:

```python
current_hashes: dict[str, str] = {}
if only_stale_prompt and backend:
    for s in stage_names:
        defn = STAGES.get(s)
        if defn and defn.prompt_hash_fn is not None:
            h = defn.prompt_hash_fn(conn, config)
            if h:
                current_hashes[f"staged:{s}"] = h
```

Also replace `modes_to_check` computation to use `STAGES[s].scope != StageScope.GLOBAL`.

## Step 6: Replace hardcoded constants in `run_pipeline()`

- Delete `GLOBAL_STAGES = {"extract_base"}` (line 94)
- Replace `NO_AI_STAGES = {"extract_base", "fetch_homepages"}` (line 178) with: `needs_ai = any(STAGES[s].needs_ai for s in stage_names)`
- Replace `s not in GLOBAL_STAGES` checks with `STAGES[s].scope != StageScope.GLOBAL`

## Step 7: Add topological ordering

Add `_topo_order()` helper using `graphlib.TopologicalSorter`:

```python
from graphlib import TopologicalSorter

def _topo_order(stage_names: list[str]) -> list[str]:
    requested = set(stage_names)
    graph = {}
    for name in stage_names:
        graph[name] = STAGES[name].depends_on & requested
    return list(TopologicalSorter(graph).static_order())
```

Use `_topo_order(stage_names)` to order stages in all three execution branches. The three branches remain (per_company, stage-first, single/no-filter) because they represent genuinely different loop nesting — but all now derive ordering and global/per-company classification from `StageDefinition` metadata.

Extract `_print_company_header()` helper for the company banner (currently inlined at lines 450-462).

## Step 8: Cleanup

- Remove `import inspect`
- Remove `GLOBAL_STAGES` constant
- Verify `ALL_STAGES` alias still works for the `stage_name not in ALL_STAGES` validation check in `_run_stage()` — switch this to `stage_name not in STAGES`

---

## What does NOT change

- Stage wrapper functions (`run_extract_events`, etc.) — signatures and bodies unchanged
- Stage implementation modules (`events.py`, `analyse_discussions.py`, etc.)
- CLI command definitions and parameter mapping
- `processing_runs` table schema and mode format (`"staged:{name}"`)
- `apply_changes()` and `ProposedChanges`
- Database layer (`db.py`)

## Verification

No automated tests cover runner.py/stages.py currently. Manual verification:

1. `email-analyser analyse --dry-run` — stage ordering matches current behavior
2. `email-analyser analyse -s extract_events -s propose_actions --dry-run` — topo ordering with subset
3. `email-analyser analyse --company <domain> --stale-prompt --dry-run` — prompt hash detection
4. `email-analyser analyse --label <label> --per-company --dry-run` — global/per-company split
5. `email-analyser analyse -s extract_events --stale-model --dry-run` — staleness filtering
6. `email-analyser update --company <domain>` — direct stage function imports still work
