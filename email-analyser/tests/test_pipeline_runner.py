"""Tests for the pipeline runner orchestration logic.

Covers: _run_stage kwargs dispatch, _is_stage_stale staleness detection,
staleness filtering, and run_pipeline execution modes.
"""
from __future__ import annotations

import io
import sqlite3
from unittest.mock import patch

import pytest
from rich.console import Console

from email_manager.config import Config
from email_manager.pipeline.runner import _run_stage, _is_stage_stale, run_pipeline
from tests.conftest import (
    MockLLMBackend,
    insert_company,
    insert_email,
    insert_processing_run,
)

# ── Helpers ─────────────────────────────────────────────────────────────────


class _NoCloseConn:
    """Wrapper that delegates everything to the real connection but makes close() a no-op."""
    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def close(self):
        pass  # run_pipeline calls conn.close(); prevent it from closing the test fixture


def _make_stub(*, accepts_clean=False, accepts_company=False, accepts_label=False,
               accepts_exclude=False, accepts_contact=False, accepts_concurrency=False,
               return_value=0, raises=None):
    """Build a stage stub function with a controlled signature and tracking."""
    params = ["conn", "backend", "config", "console", "limit", "force"]
    if accepts_clean:
        params.append("clean")
    if accepts_company:
        params.append("company")
    if accepts_label:
        params.append("label")
    if accepts_exclude:
        params.append("exclude")
    if accepts_contact:
        params.append("contact")
    if accepts_concurrency:
        params.append("concurrency")

    # Build a function with the exact signature we need
    recorded_calls: list[dict] = []

    # We need inspect.signature to see our params, so use **kwargs and record
    # But the runner uses inspect.signature, so we need real params in the sig.
    # Easiest: define closures for each combination we actually test.

    def fn(conn, backend, config, **kwargs):
        recorded_calls.append(kwargs)
        if raises:
            raise raises
        return return_value

    # Patch the function's signature to include the params we want
    import inspect
    real_params = [inspect.Parameter(p, inspect.Parameter.POSITIONAL_OR_KEYWORD)
                   for p in ["conn", "backend", "config"]]
    for p in ["console", "limit", "force"]:
        real_params.append(inspect.Parameter(p, inspect.Parameter.KEYWORD_ONLY, default=None if p != "force" else False))
    if accepts_clean:
        real_params.append(inspect.Parameter("clean", inspect.Parameter.KEYWORD_ONLY, default=False))
    if accepts_company:
        real_params.append(inspect.Parameter("company", inspect.Parameter.KEYWORD_ONLY, default=None))
    if accepts_label:
        real_params.append(inspect.Parameter("label", inspect.Parameter.KEYWORD_ONLY, default=None))
    if accepts_exclude:
        real_params.append(inspect.Parameter("exclude", inspect.Parameter.KEYWORD_ONLY, default=None))
    if accepts_contact:
        real_params.append(inspect.Parameter("contact", inspect.Parameter.KEYWORD_ONLY, default=None))
    if accepts_concurrency:
        real_params.append(inspect.Parameter("concurrency", inspect.Parameter.KEYWORD_ONLY, default=1))

    fn.__signature__ = inspect.Signature(real_params)
    fn.recorded_calls = recorded_calls
    return fn


# ── _run_stage tests ────────────────────────────────────────────────────────


class TestRunStage:
    def test_unknown_stage_returns_neg1(self, test_db, test_config):
        console = Console(quiet=True)
        result = _run_stage("nonexistent", test_db, None, test_config, console)
        assert result == -1

    def test_passes_base_kwargs(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(return_value=42)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            result = _run_stage("test_stage", test_db, None, test_config, console,
                                limit=10, force=True)
        assert result == 42
        assert len(stub.recorded_calls) == 1
        call = stub.recorded_calls[0]
        assert call["console"] is console
        assert call["limit"] == 10
        assert call["force"] is True

    def test_passes_company_when_accepted(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(accepts_company=True)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console,
                       company="acme.com")
        assert stub.recorded_calls[0]["company"] == "acme.com"

    def test_skips_company_when_not_accepted(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub()  # no accepts_company
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console,
                       company="acme.com")
        assert "company" not in stub.recorded_calls[0]

    def test_passes_clean_when_accepted_and_true(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(accepts_clean=True)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console, clean=True)
        assert stub.recorded_calls[0]["clean"] is True

    def test_skips_clean_when_false(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(accepts_clean=True)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console, clean=False)
        # clean=False is falsy, so the runner's `if clean and "clean" in sig` is False
        assert "clean" not in stub.recorded_calls[0]

    def test_passes_concurrency_when_gt1(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(accepts_concurrency=True)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console, concurrency=4)
        assert stub.recorded_calls[0]["concurrency"] == 4

    def test_skips_concurrency_when_eq1(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(accepts_concurrency=True)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console, concurrency=1)
        assert "concurrency" not in stub.recorded_calls[0]

    def test_returns_stage_count(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(return_value=99)
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            result = _run_stage("test_stage", test_db, None, test_config, console)
        assert result == 99

    def test_exception_returns_neg1(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(raises=ValueError("boom"))
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            result = _run_stage("test_stage", test_db, None, test_config, console)
        assert result == -1

    def test_exception_records_error_when_company_set(self, test_db, test_config):
        console = Console(quiet=True)
        backend = MockLLMBackend()
        stub = _make_stub(accepts_company=True, raises=ValueError("stage exploded"))
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            result = _run_stage("test_stage", test_db, backend, test_config, console,
                                company="acme.com")
        assert result == -1
        row = test_db.execute(
            "SELECT * FROM processing_runs WHERE company_domain = ? AND mode = ?",
            ("acme.com", "staged:test_stage"),
        ).fetchone()
        assert row is not None
        assert "stage exploded" in row["error"]

    def test_exception_no_error_row_without_company(self, test_db, test_config):
        console = Console(quiet=True)
        stub = _make_stub(raises=ValueError("boom"))
        with patch("email_manager.pipeline.runner.ALL_STAGES", {"test_stage": stub}):
            _run_stage("test_stage", test_db, None, test_config, console)
        row = test_db.execute("SELECT COUNT(*) as cnt FROM processing_runs").fetchone()
        assert row["cnt"] == 0


# ── _is_stage_stale tests ──────────────────────────────────────────────────


class TestIsStageStale:
    def test_no_checks_returns_false(self, test_db):
        backend = MockLLMBackend()
        result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                 check_model=False, check_prompt=False)
        assert result is False

    def test_never_run_returns_true(self, test_db):
        backend = MockLLMBackend()
        result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                 check_model=True, check_prompt=False)
        assert result is True

    def test_model_changed_returns_true(self, test_db):
        insert_processing_run(test_db, "acme.com", "extract_events", model="old-model")
        backend = MockLLMBackend(model="new-model")
        result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                 check_model=True, check_prompt=False)
        assert result is True

    def test_model_same_returns_false(self, test_db):
        insert_processing_run(test_db, "acme.com", "extract_events", model="test-model-v1")
        backend = MockLLMBackend(model="test-model-v1")
        result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                 check_model=True, check_prompt=False)
        assert result is False

    def test_prompt_changed_returns_true(self, test_db):
        insert_processing_run(test_db, "acme.com", "extract_events",
                              prompt_hash="old_hash_value_xx")
        backend = MockLLMBackend()
        with patch("email_manager.analysis.feedback.compute_prompt_hash", return_value="new_hash_value_xx"), \
             patch("email_manager.analysis.feedback.format_rules_block", return_value=""), \
             patch("email_manager.ai.prompts.EXTRACT_EVENTS_SYSTEM", "test_prompt"):
            result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                     check_model=False, check_prompt=True)
        assert result is True

    def test_prompt_same_returns_false(self, test_db):
        insert_processing_run(test_db, "acme.com", "extract_events",
                              prompt_hash="same_hash_here_xx")
        backend = MockLLMBackend()
        with patch("email_manager.analysis.feedback.compute_prompt_hash", return_value="same_hash_here_xx"), \
             patch("email_manager.analysis.feedback.format_rules_block", return_value=""), \
             patch("email_manager.ai.prompts.EXTRACT_EVENTS_SYSTEM", "test_prompt"):
            result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                     check_model=False, check_prompt=True)
        assert result is False

    def test_prompt_no_hash_stored_returns_false(self, test_db):
        """When the stored run has no prompt_hash, prompt check is skipped."""
        insert_processing_run(test_db, "acme.com", "extract_events", prompt_hash=None)
        backend = MockLLMBackend()
        result = _is_stage_stale(test_db, "acme.com", "extract_events", backend,
                                 check_model=False, check_prompt=True)
        assert result is False


# ── Staleness filtering tests (via run_pipeline dry_run) ────────────────────


class TestStalenessFiltering:
    """Test _filter_by_staleness indirectly through run_pipeline(dry_run=True)."""

    def _recording_console(self):
        """Create a console that records output without printing to terminal."""
        return Console(record=True, file=io.StringIO())

    def _run_dry(self, config, conn, backend, console, **kwargs):
        """Run pipeline in dry_run mode with patched dependencies."""
        wrapped = _NoCloseConn(conn)
        with patch("email_manager.pipeline.runner.get_db", return_value=wrapped), \
             patch("email_manager.pipeline.runner.get_backend", return_value=backend):
            return run_pipeline(config, dry_run=True, console=console, **kwargs)

    def test_filter_unprocessed_includes_new_company(self, test_db, test_config):
        insert_company(test_db, "new.com")
        insert_company(test_db, "done.com")
        insert_processing_run(test_db, "done.com", "extract_events")

        console = self._recording_console()
        self._run_dry(test_config, test_db, MockLLMBackend(), console,
                      stages=["extract_events"], only_unprocessed=True)

        output = console.export_text()
        assert "new.com" in output

    def test_filter_unprocessed_excludes_processed_company(self, test_db, test_config):
        insert_company(test_db, "done.com")
        insert_processing_run(test_db, "done.com", "extract_events")

        console = self._recording_console()
        self._run_dry(test_config, test_db, MockLLMBackend(), console,
                      stages=["extract_events"], only_unprocessed=True)

        output = console.export_text()
        assert "Targeting 0" in output or "0 companies" in output

    def test_filter_stale_model_includes_when_different(self, test_db, test_config):
        insert_company(test_db, "acme.com")
        insert_processing_run(test_db, "acme.com", "extract_events", model="old-model")

        console = self._recording_console()
        self._run_dry(test_config, test_db, MockLLMBackend(model="new-model"), console,
                      stages=["extract_events"], only_stale_model=True)

        output = console.export_text()
        assert "acme.com" in output

    def test_filter_stale_model_excludes_when_same(self, test_db, test_config):
        insert_company(test_db, "acme.com")
        insert_processing_run(test_db, "acme.com", "extract_events", model="test-model-v1")

        console = self._recording_console()
        self._run_dry(test_config, test_db, MockLLMBackend(model="test-model-v1"), console,
                      stages=["extract_events"], only_stale_model=True)

        output = console.export_text()
        assert "Targeting 0" in output or "0 companies" in output

    def test_filter_new_emails_includes_when_newer(self, test_db, test_config):
        insert_company(test_db, "acme.com")
        insert_processing_run(test_db, "acme.com", "extract_events",
                              email_cutoff_date="2025-01-01T00:00:00")
        insert_email(test_db, "new@msg", "alice@acme.com", ["bob@acme.com"],
                     "2025-06-01T10:00:00")

        console = self._recording_console()
        self._run_dry(test_config, test_db, MockLLMBackend(), console,
                      stages=["extract_events"], only_new_emails=True)

        output = console.export_text()
        assert "acme.com" in output

    def test_filter_new_emails_excludes_when_no_newer(self, test_db, test_config):
        insert_company(test_db, "acme.com")
        insert_processing_run(test_db, "acme.com", "extract_events",
                              email_cutoff_date="2025-12-01T00:00:00")
        insert_email(test_db, "old@msg", "alice@acme.com", ["bob@acme.com"],
                     "2025-01-01T10:00:00")

        console = self._recording_console()
        self._run_dry(test_config, test_db, MockLLMBackend(), console,
                      stages=["extract_events"], only_new_emails=True)

        output = console.export_text()
        assert "Targeting 0" in output or "0 companies" in output


# ── run_pipeline execution mode tests ───────────────────────────────────────


class TestRunPipelineExecutionModes:
    def _run(self, config, conn, console, **kwargs):
        """Run pipeline with patched get_db (no-close wrapper)."""
        wrapped = _NoCloseConn(conn)
        backend = kwargs.pop("backend", MockLLMBackend())
        with patch("email_manager.pipeline.runner.get_db", return_value=wrapped), \
             patch("email_manager.pipeline.runner.get_backend", return_value=backend):
            return run_pipeline(config, console=console, **kwargs)

    def test_dry_run_returns_empty(self, test_db, test_config):
        console = Console(quiet=True)
        result = self._run(test_config, test_db, console,
                           stages=["extract_base"], dry_run=True)
        assert result == {}

    def test_runs_requested_stages_only(self, test_db, test_config):
        console = Console(quiet=True)
        called_stages = []

        def make_stage(name):
            def fn(conn, backend, config, console=None, limit=None, force=False):
                called_stages.append(name)
                return 0
            return fn

        fake_stages = {
            "extract_base": make_stage("extract_base"),
            "extract_events": make_stage("extract_events"),
            "propose_actions": make_stage("propose_actions"),
        }
        with patch("email_manager.pipeline.runner.ALL_STAGES", fake_stages):
            self._run(test_config, test_db, console, stages=["extract_events"])

        assert called_stages == ["extract_events"]

    def test_no_ai_stages_skip_backend(self, test_db, test_config):
        console = Console(quiet=True)
        backend_called = []

        def fake_get_backend(config):
            backend_called.append(True)
            return MockLLMBackend()

        def fake_extract_base(conn, backend, config, console=None, limit=None, force=False):
            return 0

        fake_stages = {"extract_base": fake_extract_base}
        wrapped = _NoCloseConn(test_db)
        with patch("email_manager.pipeline.runner.ALL_STAGES", fake_stages), \
             patch("email_manager.pipeline.runner.get_db", return_value=wrapped), \
             patch("email_manager.pipeline.runner.get_backend", fake_get_backend):
            run_pipeline(test_config, stages=["extract_base"], console=console)

        assert len(backend_called) == 0

    def test_per_company_runs_global_once(self, test_db, test_config):
        console = Console(quiet=True)
        insert_company(test_db, "a.com")
        insert_company(test_db, "b.com")

        call_log = []

        def make_stage(name):
            def fn(conn, backend, config, console=None, limit=None, force=False,
                   company=None, label=None):
                call_log.append((name, company))
                return 1
            return fn

        fake_stages = {
            "extract_base": make_stage("extract_base"),
            "extract_events": make_stage("extract_events"),
        }
        with patch("email_manager.pipeline.runner.ALL_STAGES", fake_stages):
            self._run(test_config, test_db, console,
                      stages=["extract_base", "extract_events"],
                      company_list=["a.com", "b.com"], per_company=True)

        # extract_base is GLOBAL — should be called once without a company
        base_calls = [c for c in call_log if c[0] == "extract_base"]
        assert len(base_calls) == 1
        assert base_calls[0][1] is None

        # extract_events is per-company — called for each domain
        events_calls = [c for c in call_log if c[0] == "extract_events"]
        assert len(events_calls) == 2
        assert {c[1] for c in events_calls} == {"a.com", "b.com"}

    def test_pipeline_results_accumulate(self, test_db, test_config):
        console = Console(quiet=True)
        insert_company(test_db, "a.com")
        insert_company(test_db, "b.com")

        def fake_events(conn, backend, config, console=None, limit=None, force=False,
                        company=None, label=None):
            return 3

        fake_stages = {"extract_events": fake_events}
        with patch("email_manager.pipeline.runner.ALL_STAGES", fake_stages), \
             patch("email_manager.pipeline.runner.GLOBAL_STAGES", set()):
            result = self._run(test_config, test_db, console,
                               stages=["extract_events"],
                               company_list=["a.com", "b.com"], per_company=True)

        assert result["extract_events"] == 6

    def test_single_company_passes_company_kwarg(self, test_db, test_config):
        console = Console(quiet=True)
        call_log = []

        def fake_events(conn, backend, config, console=None, limit=None, force=False,
                        company=None, label=None):
            call_log.append({"company": company})
            return 1

        fake_stages = {"extract_events": fake_events}
        with patch("email_manager.pipeline.runner.ALL_STAGES", fake_stages):
            self._run(test_config, test_db, console,
                      stages=["extract_events"], company="acme.com")

        assert len(call_log) == 1
        assert call_log[0]["company"] == "acme.com"

    def test_stage_first_mode(self, test_db, test_config):
        """Without per_company, stages run in stage-first order across companies."""
        console = Console(quiet=True)
        insert_company(test_db, "a.com")
        insert_company(test_db, "b.com")

        call_log = []

        def make_stage(name):
            def fn(conn, backend, config, console=None, limit=None, force=False,
                   company=None, label=None):
                call_log.append((name, company))
                return 1
            return fn

        fake_stages = {
            "extract_events": make_stage("extract_events"),
            "discover_discussions": make_stage("discover_discussions"),
        }
        with patch("email_manager.pipeline.runner.ALL_STAGES", fake_stages), \
             patch("email_manager.pipeline.runner.GLOBAL_STAGES", set()):
            self._run(test_config, test_db, console,
                      stages=["extract_events", "discover_discussions"],
                      company_list=["a.com", "b.com"], per_company=False)

        # Stage-first: extract_events for a, b, then discover for a, b
        assert call_log[0] == ("extract_events", "a.com")
        assert call_log[1] == ("extract_events", "b.com")
        assert call_log[2] == ("discover_discussions", "a.com")
        assert call_log[3] == ("discover_discussions", "b.com")
