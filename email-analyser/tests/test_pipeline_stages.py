"""Integration tests for pipeline stages with real DB and mock LLM.

Each test inserts seed data, runs the real stage wrapper, and verifies DB state.
"""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest
from rich.console import Console

from email_manager.db import fetchall, fetchone
from email_manager.pipeline.stages import (
    run_extract_base,
    run_extract_events,
    run_label_companies,
    run_discover_discussions,
    run_analyse_discussions,
    run_propose_actions,
)
from tests.conftest import (
    MockLLMBackend,
    insert_email,
    insert_company,
    insert_event,
    insert_discussion,
    insert_processing_run,
)

CONSOLE = Console(quiet=True)

# Prevent pydantic-settings from loading .env values that override test behavior
_TEST_CONFIG_OVERRIDES = {
    "extract_events_model": "",  # prevent stage backend override
}

# Minimal category config for stages that need it
MINIMAL_CATEGORIES = [
    {
        "name": "general",
        "description": "General business",
        "workflow_states": ["active", "stalled", "resolved"],
        "terminal_states": ["resolved"],
        "milestones": ["initial_contact", "proposal_sent", "agreement_reached"],
        "event_types": [
            {"name": "meeting", "description": "A meeting"},
            {"name": "proposal_sent", "description": "Proposal sent"},
            {"name": "agreement_reached", "description": "Agreement reached"},
        ],
    }
]


def _seed_threaded_emails(conn, domain="acme.com", thread_id="thread_1", count=3):
    """Insert a set of threaded emails for a company domain."""
    user = f"alice@{domain}"
    me = "me@mycompany.com"
    for i in range(count):
        insert_email(
            conn,
            message_id=f"msg{i}@{domain}",
            from_address=user if i % 2 == 0 else me,
            to_addresses=[me if i % 2 == 0 else user],
            date=f"2025-06-{10 + i:02d}T10:00:00+00:00",
            subject="Partnership discussion",
            body_text=f"Email body number {i} about our partnership",
            thread_id=thread_id,
        )


# ── extract_base ────────────────────────────────────────────────────────────


class TestExtractBase:
    def test_creates_companies_from_emails(self, test_db, test_config):
        insert_email(test_db, "msg1@test", "alice@acme.com", ["bob@partner.org"],
                     "2025-06-01T10:00:00")
        insert_email(test_db, "msg2@test", "carol@acme.com", ["bob@partner.org"],
                     "2025-06-02T10:00:00")
        insert_email(test_db, "msg3@test", "bob@partner.org", ["alice@acme.com"],
                     "2025-06-03T10:00:00")

        count = run_extract_base(test_db, None, test_config, console=CONSOLE)
        assert count > 0

        companies = fetchall(test_db, "SELECT domain FROM companies ORDER BY domain")
        domains = {r["domain"] for r in companies}
        assert "acme.com" in domains
        assert "partner.org" in domains

    def test_creates_contacts(self, test_db, test_config):
        insert_email(test_db, "msg1@test", "alice@acme.com",
                     ["bob@partner.org", "carol@other.io"],
                     "2025-06-01T10:00:00")

        run_extract_base(test_db, None, test_config, console=CONSOLE)

        contacts = fetchall(test_db, "SELECT email FROM contacts ORDER BY email")
        emails = {r["email"] for r in contacts}
        assert "alice@acme.com" in emails
        assert "bob@partner.org" in emails
        assert "carol@other.io" in emails

    def test_idempotent(self, test_db, test_config):
        insert_email(test_db, "msg1@test", "alice@acme.com", ["bob@partner.org"],
                     "2025-06-01T10:00:00")

        count1 = run_extract_base(test_db, None, test_config, console=CONSOLE)
        assert count1 > 0

        count2 = run_extract_base(test_db, None, test_config, console=CONSOLE)
        assert count2 == 0


# ── extract_events ──────────────────────────────────────────────────────────


class TestExtractEvents:
    # Batch format: small threads get batched, so response must use "threads" key
    MOCK_EVENTS_RESPONSE = {
        "threads": {
            "thread_1": {
                "events": [
                    {
                        "type": "meeting",
                        "domain": "general",
                        "actor": "alice@acme.com",
                        "event_date": "2025-06-10",
                        "detail": "Kickoff meeting scheduled",
                        "confidence": 0.9,
                        "source_email_index": 0,
                    }
                ]
            }
        }
    }

    def _setup_company_with_emails(self, conn):
        """Create company + threaded emails, run extract_base to populate companies."""
        _seed_threaded_emails(conn, domain="acme.com")
        run_extract_base(conn, None, None, console=CONSOLE, force=True)

    def _patch_config(self, test_config):
        """Prevent the stage wrapper from overriding the mock backend."""
        return patch.object(test_config, "extract_events_model", "")

    def test_creates_events(self, test_db, test_config):
        self._setup_company_with_emails(test_db)
        backend = MockLLMBackend(responses=[self.MOCK_EVENTS_RESPONSE])

        with patch("email_manager.analysis.events.load_category_config",
                   return_value=MINIMAL_CATEGORIES), \
             self._patch_config(test_config):
            count = run_extract_events(test_db, backend, test_config, console=CONSOLE,
                                       force=True, company="acme.com")

        assert count > 0
        events = fetchall(test_db, "SELECT * FROM event_ledger WHERE domain = 'general'")
        assert len(events) >= 1
        assert events[0]["type"] == "meeting"
        assert events[0]["actor"] == "alice@acme.com"

    def test_skips_already_processed(self, test_db, test_config):
        self._setup_company_with_emails(test_db)

        with patch("email_manager.analysis.events.load_category_config",
                   return_value=MINIMAL_CATEGORIES), \
             self._patch_config(test_config):
            backend = MockLLMBackend(responses=[self.MOCK_EVENTS_RESPONSE])
            run_extract_events(test_db, backend, test_config, console=CONSOLE,
                               force=True, company="acme.com")
            # Second run — threads already have events
            backend2 = MockLLMBackend(responses=[self.MOCK_EVENTS_RESPONSE])
            count2 = run_extract_events(test_db, backend2, test_config, console=CONSOLE,
                                        company="acme.com")

        assert count2 == 0
        assert len(backend2.calls) == 0

    def test_force_reprocesses(self, test_db, test_config):
        self._setup_company_with_emails(test_db)

        with patch("email_manager.analysis.events.load_category_config",
                   return_value=MINIMAL_CATEGORIES), \
             self._patch_config(test_config):
            backend1 = MockLLMBackend(responses=[self.MOCK_EVENTS_RESPONSE])
            run_extract_events(test_db, backend1, test_config, console=CONSOLE,
                               force=True, company="acme.com")

            backend2 = MockLLMBackend(responses=[self.MOCK_EVENTS_RESPONSE])
            count2 = run_extract_events(test_db, backend2, test_config, console=CONSOLE,
                                        force=True, company="acme.com")

        assert count2 > 0
        assert len(backend2.calls) > 0


# ── label_companies ─────────────────────────────────────────────────────────


class TestLabelCompanies:
    MOCK_LABEL_RESPONSE = {
        "company_name": "Acme Corp",
        "company_description": "Technology company",
        "labels": [
            {"label": "vendor", "confidence": 0.85, "reasoning": "Provides services"}
        ],
    }

    def test_creates_labels(self, test_db, test_config):
        # Need a company with emails for labelling
        insert_email(test_db, "msg1@test", "alice@acme.com", ["me@myco.com"],
                     "2025-06-01T10:00:00")
        run_extract_base(test_db, None, test_config, console=CONSOLE, force=True)

        backend = MockLLMBackend(responses=[self.MOCK_LABEL_RESPONSE])
        count = run_label_companies(test_db, backend, test_config,
                                    console=CONSOLE, force=True, company="acme.com")

        assert count > 0
        labels = fetchall(test_db,
            "SELECT cl.label, cl.confidence FROM company_labels cl "
            "JOIN companies c ON cl.company_id = c.id WHERE c.domain = 'acme.com'")
        assert len(labels) >= 1
        assert labels[0]["label"] == "vendor"

    def test_skips_already_labelled(self, test_db, test_config):
        insert_email(test_db, "msg1@test", "alice@acme.com", ["me@myco.com"],
                     "2025-06-01T10:00:00")
        run_extract_base(test_db, None, test_config, console=CONSOLE, force=True)

        # Pre-insert a label
        co_id = fetchone(test_db, "SELECT id FROM companies WHERE domain = 'acme.com'")["id"]
        test_db.execute(
            "INSERT INTO company_labels (company_id, label, confidence) VALUES (?, ?, ?)",
            (co_id, "customer", 0.9),
        )
        test_db.commit()

        backend = MockLLMBackend(responses=[self.MOCK_LABEL_RESPONSE])
        count = run_label_companies(test_db, backend, test_config,
                                    console=CONSOLE, company="acme.com")

        assert count == 0
        assert len(backend.calls) == 0

    def test_force_relabels(self, test_db, test_config):
        insert_email(test_db, "msg1@test", "alice@acme.com", ["me@myco.com"],
                     "2025-06-01T10:00:00")
        run_extract_base(test_db, None, test_config, console=CONSOLE, force=True)

        # Pre-insert a label
        co_id = fetchone(test_db, "SELECT id FROM companies WHERE domain = 'acme.com'")["id"]
        test_db.execute(
            "INSERT INTO company_labels (company_id, label, confidence) VALUES (?, ?, ?)",
            (co_id, "old_label", 0.5),
        )
        test_db.commit()

        backend = MockLLMBackend(responses=[self.MOCK_LABEL_RESPONSE])
        count = run_label_companies(test_db, backend, test_config,
                                    console=CONSOLE, force=True, company="acme.com")

        assert count > 0
        assert len(backend.calls) > 0


# ── discover_discussions ────────────────────────────────────────────────────


class TestDiscoverDiscussions:
    MOCK_DISCOVER_RESPONSE = {
        "discussions": [
            {
                "existing_id": None,
                "parent_id": None,
                "parent_idx": None,
                "title": "Acme Partnership",
                "category": "general",
                "company_domain": "acme.com",
                "participants": ["alice@acme.com"],
                "event_ids": ["evt_001"],
                "thread_ids": ["thread_1"],
            }
        ]
    }

    def _setup_events(self, conn):
        """Insert company + unassigned events with proper email linkage."""
        co_id = insert_company(conn, "acme.com")
        run_id = insert_processing_run(conn, "acme.com", "extract_events")
        # Need emails first (for source_email_id JOIN)
        insert_email(conn, "msg1@acme.com", "alice@acme.com", ["me@myco.com"],
                     "2025-06-10T10:00:00", thread_id="thread_1")
        insert_event(conn, "evt_001", "thread_1", "general", "meeting",
                     actor="alice@acme.com", event_date="2025-06-10",
                     run_id=run_id, source_email_id="msg1@acme.com")
        return co_id

    def test_creates_discussions_and_links_events(self, test_db, test_config):
        self._setup_events(test_db)
        backend = MockLLMBackend(responses=[self.MOCK_DISCOVER_RESPONSE])

        with patch("email_manager.analysis.events.load_category_config",
                   return_value=MINIMAL_CATEGORIES):
            count = run_discover_discussions(test_db, backend, test_config,
                                            console=CONSOLE, force=True,
                                            company="acme.com")

        assert count > 0
        assert len(backend.calls) > 0
        discussions = fetchall(test_db, "SELECT * FROM discussions")
        assert len(discussions) >= 1
        assert discussions[0]["title"] == "Acme Partnership"


# ── analyse_discussions ─────────────────────────────────────────────────────


class TestAnalyseDiscussions:
    MOCK_ANALYSE_RESPONSE = {
        "milestones": [
            {
                "name": "initial_contact",
                "achieved": True,
                "achieved_date": "2025-06-10",
                "evidence_event_ids": ["evt_001"],
                "confidence": 0.9,
            }
        ],
        "workflow_state": "active",
        "summary": "Ongoing partnership discussion with Acme Corp.",
    }

    def _setup_discussion_with_events(self, conn):
        """Insert company, discussion, and linked events."""
        co_id = insert_company(conn, "acme.com")
        disc_id = insert_discussion(conn, co_id, "Acme Partnership", category="general")
        run_id = insert_processing_run(conn, "acme.com", "extract_events")
        insert_email(conn, "msg1@acme.com", "alice@acme.com", ["me@myco.com"],
                     "2025-06-10T10:00:00", thread_id="thread_1")
        insert_event(conn, "evt_001", "thread_1", "general", "meeting",
                     actor="alice@acme.com", event_date="2025-06-10",
                     discussion_id=disc_id, run_id=run_id,
                     source_email_id="msg1@acme.com")
        # Link thread to discussion
        conn.execute(
            "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
            (disc_id, "thread_1"),
        )
        conn.commit()
        return co_id, disc_id

    def test_creates_milestones(self, test_db, test_config):
        co_id, disc_id = self._setup_discussion_with_events(test_db)
        backend = MockLLMBackend(responses=[self.MOCK_ANALYSE_RESPONSE])

        with patch("email_manager.analysis.analyse_discussions.load_category_config",
                   return_value=MINIMAL_CATEGORIES):
            count = run_analyse_discussions(test_db, backend, test_config,
                                           console=CONSOLE, force=True,
                                           company="acme.com")

        assert count > 0
        milestones = fetchall(test_db,
            "SELECT * FROM milestones WHERE discussion_id = ?", (disc_id,))
        assert len(milestones) >= 1
        assert milestones[0]["name"] == "initial_contact"
        assert milestones[0]["achieved"] == 1

    def test_updates_state_and_summary(self, test_db, test_config):
        co_id, disc_id = self._setup_discussion_with_events(test_db)
        backend = MockLLMBackend(responses=[self.MOCK_ANALYSE_RESPONSE])

        with patch("email_manager.analysis.analyse_discussions.load_category_config",
                   return_value=MINIMAL_CATEGORIES):
            run_analyse_discussions(test_db, backend, test_config,
                                   console=CONSOLE, force=True, company="acme.com")

        disc = fetchone(test_db, "SELECT current_state, summary FROM discussions WHERE id = ?",
                        (disc_id,))
        assert disc["current_state"] == "active"
        assert "Acme Corp" in disc["summary"]


# ── propose_actions ─────────────────────────────────────────────────────────


class TestProposeActions:
    MOCK_PROPOSE_RESPONSE = {
        "actions": [
            {
                "action": "Schedule follow-up call",
                "reasoning": "Last contact was 2 weeks ago",
                "priority": "high",
                "wait_until": None,
                "assignee": None,
            }
        ]
    }

    def _setup_active_discussion(self, conn):
        """Insert company, active discussion, milestones, and events."""
        co_id = insert_company(conn, "acme.com")
        disc_id = insert_discussion(conn, co_id, "Acme Partnership",
                                    category="general", current_state="active")
        run_id = insert_processing_run(conn, "acme.com", "analyse_discussions")
        insert_email(conn, "msg1@acme.com", "alice@acme.com", ["me@myco.com"],
                     "2025-06-10T10:00:00", thread_id="thread_1")
        insert_event(conn, "evt_001", "thread_1", "general", "meeting",
                     actor="alice@acme.com", event_date="2025-06-10",
                     discussion_id=disc_id, run_id=run_id,
                     source_email_id="msg1@acme.com")
        # Add a milestone
        conn.execute(
            """INSERT INTO milestones (discussion_id, name, achieved, achieved_date,
               evidence_event_ids, confidence, last_evaluated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (disc_id, "initial_contact", 1, "2025-06-10",
             '["evt_001"]', 0.9, "2025-06-10T12:00:00"),
        )
        conn.commit()
        return co_id, disc_id

    def test_creates_actions(self, test_db, test_config):
        co_id, disc_id = self._setup_active_discussion(test_db)
        backend = MockLLMBackend(responses=[self.MOCK_PROPOSE_RESPONSE])

        with patch("email_manager.analysis.propose_actions.load_category_config",
                   return_value=MINIMAL_CATEGORIES):
            count = run_propose_actions(test_db, backend, test_config,
                                        console=CONSOLE, force=True,
                                        company="acme.com")

        assert count > 0
        actions = fetchall(test_db,
            "SELECT * FROM proposed_actions WHERE discussion_id = ?", (disc_id,))
        assert len(actions) >= 1
        assert "follow-up" in actions[0]["action"].lower()

    def test_skips_terminal_discussions(self, test_db, test_config):
        co_id = insert_company(test_db, "acme.com")
        insert_discussion(test_db, co_id, "Closed Deal",
                          category="general", current_state="resolved")

        backend = MockLLMBackend(responses=[self.MOCK_PROPOSE_RESPONSE])
        with patch("email_manager.analysis.propose_actions.load_category_config",
                   return_value=MINIMAL_CATEGORIES):
            count = run_propose_actions(test_db, backend, test_config,
                                        console=CONSOLE, company="acme.com")

        assert count == 0
        assert len(backend.calls) == 0
