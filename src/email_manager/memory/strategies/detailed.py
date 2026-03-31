from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone
from email_manager.memory.base import ContactMemory
from email_manager.memory.strategies.default import _gather_context

DISCUSSIONS_SYSTEM = """You are a personal CRM assistant. Given email data about a contact, identify ALL distinct discussions, projects, or topics you've worked on together.

For each discussion:
- Give it a clear, specific name
- Determine its status: "active" (ongoing, recent activity), "resolved" (concluded), or "waiting" (awaiting response)
- Write a detailed summary (2-3 sentences) covering what happened, key decisions, and current state

Be thorough — capture every meaningful thread of interaction, not just the biggest ones.

Respond with JSON only."""

DISCUSSIONS_USER = """Identify all discussions with this contact:

Name: {name} ({email})
Company: {company}

Recent emails (most recent first):
{recent_emails}

Thread summaries:
{thread_summaries}

Projects:
{projects}

Respond with:
{{
  "discussions": [
    {{"topic": "Topic name", "status": "active|resolved|waiting", "summary": "Detailed summary"}}
  ]
}}"""

PROFILE_SYSTEM = """You are a personal CRM assistant. Given data about a contact and their discussions, generate a relationship profile.

Rules:
1. Write a rich 3-5 sentence summary of the overall relationship and interactions.
2. Classify the relationship type precisely.
3. Extract specific, actionable key facts (preferences, timezone, role, context you'd want to remember).

Respond with JSON only."""

PROFILE_USER = """Generate a profile for this contact:

Name: {name} ({email})
Company: {company}
Total emails: {email_count} (received: {received_count}, sent: {sent_count})
First contact: {first_seen} | Last contact: {last_seen}

Co-email network:
{co_email_network}

Discussions identified:
{discussions_text}

Recent emails:
{recent_emails}

Respond with:
{{
  "relationship": "colleague|vendor|client|friend|manager|report|recruiter|service|newsletter|other",
  "summary": "3-5 sentence overview",
  "key_facts": ["Specific fact 1", "Specific fact 2"]
}}"""


class DetailedStrategy:
    @property
    def name(self) -> str:
        return "detailed"

    def generate(
        self,
        conn: sqlite3.Connection,
        ai_backend: LLMBackend,
        email_address: str,
    ) -> ContactMemory:
        context = _gather_context(conn, email_address, max_emails=50)

        # Call 1: Extract discussions
        discussions_prompt = DISCUSSIONS_USER.format(**context)
        disc_result = ai_backend.complete_json(DISCUSSIONS_SYSTEM, discussions_prompt)
        discussions = disc_result.get("discussions", [])

        # Format discussions for the profile call
        disc_lines = []
        for d in discussions:
            disc_lines.append(f"- [{d.get('status', '?')}] {d.get('topic', '?')}: {d.get('summary', '')}")
        context["discussions_text"] = "\n".join(disc_lines) if disc_lines else "(none identified)"

        # Call 2: Generate profile with discussion context
        profile_prompt = PROFILE_USER.format(**context)
        profile_result = ai_backend.complete_json(PROFILE_SYSTEM, profile_prompt)

        return ContactMemory(
            email=email_address,
            name=context["name"],
            relationship=profile_result.get("relationship", "unknown"),
            summary=profile_result.get("summary", ""),
            discussions=discussions,
            key_facts=profile_result.get("key_facts", []),
            generated_at=datetime.now(timezone.utc).isoformat(),
            model_used=ai_backend.model_name,
            strategy_used=self.name,
        )
