from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone
from email_manager.memory.base import ContactMemory

SYSTEM_PROMPT = """You are a personal CRM assistant. Given data about a contact from someone's email history, generate a structured memory profile.

Rules:
1. Summarise the relationship and interactions in 2-4 sentences.
2. Identify distinct discussions/projects and their current status.
3. Extract key facts about this person from the email content.
4. Classify the relationship type: colleague, manager, report, vendor, client, friend, recruiter, service, newsletter, or other.
5. For discussion status: "active" if recent emails (last 30 days) with no resolution, "resolved" if concluded, "waiting" if awaiting response.

Respond with JSON only."""

USER_PROMPT = """Generate a memory profile for this contact:

Name: {name}
Email: {email}
Company: {company}
Total emails: {email_count} (received from them: {received_count}, sent to them: {sent_count})
First contact: {first_seen}
Last contact: {last_seen}

Top co-emailers (people frequently on the same emails):
{co_email_network}

Projects they're involved in:
{projects}

Thread summaries they participated in:
{thread_summaries}

Recent emails (most recent first):
{recent_emails}

Respond with this exact JSON structure:
{{
  "relationship": "colleague|vendor|client|friend|manager|report|recruiter|service|newsletter|other",
  "summary": "2-4 sentence overview of your interactions with this person",
  "discussions": [
    {{"topic": "Discussion topic", "status": "active|resolved|waiting", "summary": "Brief summary"}}
  ],
  "key_facts": ["Fact 1", "Fact 2"]
}}"""


class DefaultStrategy:
    @property
    def name(self) -> str:
        return "default"

    def generate(
        self,
        conn: sqlite3.Connection,
        ai_backend: LLMBackend,
        email_address: str,
    ) -> ContactMemory:
        context = _gather_context(conn, email_address, max_emails=30)
        prompt = USER_PROMPT.format(**context)
        result = ai_backend.complete_json(SYSTEM_PROMPT, prompt)

        return ContactMemory(
            email=email_address,
            name=context["name"],
            relationship=result.get("relationship", "unknown"),
            summary=result.get("summary", ""),
            discussions=result.get("discussions", []),
            key_facts=result.get("key_facts", []),
            generated_at=datetime.now(timezone.utc).isoformat(),
            model_used=ai_backend.model_name,
            strategy_used=self.name,
        )


def _gather_context(conn: sqlite3.Connection, email_address: str, max_emails: int = 30) -> dict:
    """Gather all available data about a contact for prompt construction."""

    # Contact record
    contact = fetchone(conn, "SELECT * FROM contacts WHERE email = ?", (email_address,))
    name = (contact["name"] if contact else None) or email_address
    company = (contact["company"] if contact else None) or "—"
    email_count = contact["email_count"] if contact else 0
    received_count = contact["received_count"] if contact else 0
    sent_count = contact["sent_count"] if contact else 0
    first_seen = (contact["first_seen"] if contact else "")[:10] if contact else "—"
    last_seen = (contact["last_seen"] if contact else "")[:10] if contact else "—"

    # Co-email network (top 10)
    co_emailers = fetchall(
        conn,
        """SELECT email_a, email_b, co_email_count FROM co_email_stats
           WHERE email_a = ? OR email_b = ?
           ORDER BY co_email_count DESC LIMIT 10""",
        (email_address, email_address),
    )
    co_email_lines = []
    for r in co_emailers:
        other = r["email_b"] if r["email_a"] == email_address else r["email_a"]
        co_email_lines.append(f"- {other} ({r['co_email_count']} shared emails)")
    co_email_network = "\n".join(co_email_lines) if co_email_lines else "(none)"

    # Projects
    projects = fetchall(
        conn,
        """SELECT DISTINCT p.name, COUNT(ep.email_id) as cnt
           FROM projects p
           JOIN email_projects ep ON p.id = ep.project_id
           JOIN emails e ON ep.email_id = e.id
           WHERE e.from_address = ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?
           GROUP BY p.name ORDER BY cnt DESC LIMIT 15""",
        (email_address, f'%"{email_address}"%', f'%"{email_address}"%'),
    )
    projects_text = "\n".join(f"- {p['name']} ({p['cnt']} emails)" for p in projects) if projects else "(none)"

    # Thread summaries
    thread_rows = fetchall(
        conn,
        """SELECT DISTINCT t.subject, t.summary, t.last_date, t.email_count
           FROM threads t
           JOIN emails e ON e.thread_id = t.thread_id
           WHERE (e.from_address = ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?)
             AND t.summary IS NOT NULL
           ORDER BY t.last_date DESC LIMIT 10""",
        (email_address, f'%"{email_address}"%', f'%"{email_address}"%'),
    )
    thread_lines = []
    for t in thread_rows:
        thread_lines.append(f"- [{(t['last_date'] or '')[:10]}] {t['subject']}: {t['summary']}")
    thread_summaries = "\n".join(thread_lines) if thread_lines else "(none)"

    # Recent emails
    emails = fetchall(
        conn,
        """SELECT date, from_address, from_name, subject, body_text
           FROM emails
           WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?
           ORDER BY date DESC LIMIT ?""",
        (email_address, f'%"{email_address}"%', f'%"{email_address}"%', max_emails),
    )
    email_lines = []
    for e in emails:
        sender = e["from_name"] or e["from_address"]
        body_snippet = (e["body_text"] or "")[:200].replace("\n", " ")
        email_lines.append(f"[{(e['date'] or '')[:10]}] From: {sender} | Subject: {e['subject'] or '(no subject)'}\n  {body_snippet}")
    recent_emails = "\n".join(email_lines) if email_lines else "(none)"

    return {
        "name": name,
        "email": email_address,
        "company": company,
        "email_count": email_count,
        "received_count": received_count,
        "sent_count": sent_count,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "co_email_network": co_email_network,
        "projects": projects_text,
        "thread_summaries": thread_summaries,
        "recent_emails": recent_emails,
    }
