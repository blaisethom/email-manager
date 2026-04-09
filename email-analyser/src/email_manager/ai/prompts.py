from __future__ import annotations

CATEGORISE_SYSTEM = """You are an email categorisation assistant. Your job is to analyse emails and assign them to projects.

A "project" is a coherent body of work, initiative, or ongoing topic. Examples:
- A product launch ("Website Redesign", "Q1 Marketing Campaign")
- A business process ("Hiring - Engineering", "Vendor Negotiations")
- A recurring activity ("Weekly Standups", "Monthly Reporting")
- A personal category ("Travel Plans", "Family")

Rules:
1. Each email can belong to 1-3 projects.
2. Create descriptive, specific project names (not generic like "Work" or "Email").
3. Reuse existing project names when the email clearly fits an existing project.
4. If an email is genuinely miscellaneous (newsletters, notifications, spam), assign it to "Uncategorised".
5. Return a confidence score (0.0-1.0) for each assignment.

Respond with JSON only."""

CATEGORISE_USER = """Here are the existing projects so far:
{existing_projects}

Categorise these emails into projects. For each email, return project assignments.

Emails:
{emails}

Respond with this exact JSON structure:
{{
  "assignments": [
    {{
      "email_index": 0,
      "projects": [
        {{"name": "Project Name", "confidence": 0.85}}
      ]
    }}
  ]
}}"""

ENTITY_EXTRACTION_SYSTEM = """You are an entity extraction assistant. Extract key entities from emails.

Entity types to extract:
- "person": Named individuals mentioned (not just email addresses)
- "company": Companies or organisations mentioned
- "topic": Key topics or themes discussed
- "action_item": Action items, tasks, or commitments mentioned

Rules:
1. Only extract entities that are clearly present in the text.
2. For action items, include who is responsible if mentioned.
3. Provide confidence scores (0.0-1.0).
4. Include a brief context snippet for each entity.

Respond with JSON only."""

ENTITY_EXTRACTION_USER = """Extract entities from these emails:

{emails}

Respond with this exact JSON structure:
{{
  "extractions": [
    {{
      "email_index": 0,
      "entities": [
        {{"type": "person", "value": "John Smith", "context": "John will handle the review", "confidence": 0.9}}
      ]
    }}
  ]
}}"""

THREAD_SUMMARY_SYSTEM = """You are an email thread summarisation assistant. Summarise email threads concisely.

Rules:
1. Capture the key topic, decisions made, and current status.
2. Mention key participants and their contributions.
3. Note any open action items or unresolved questions.
4. Keep summaries to 2-4 sentences.

Respond with JSON only."""

THREAD_SUMMARY_USER = """Summarise this email thread:

Subject: {subject}
Participants: {participants}

Messages (chronological):
{messages}

Respond with this exact JSON structure:
{{
  "summary": "Your summary here",
  "key_decisions": ["decision 1", "decision 2"],
  "open_items": ["item 1"],
  "status": "active|resolved|waiting"
}}"""


EXTRACT_EVENTS_SYSTEM = """You are a business event extraction system. Given an email thread (and optionally related calendar events), your job is to:

1. Classify the business domain(s) present in the thread (e.g. investment, pharma-deal, hiring).
2. Extract discrete, factual business events from the emails using the domain-specific event vocabulary.

An "event" is something observable that happened — a deck was sent, an NDA was signed, a meeting took place. Events are facts, not interpretations. Extract only events that are clearly evidenced by the email content.

Rules:
1. Each event must have a type from the provided vocabulary for its domain.
2. If a thread covers multiple domains (e.g. an investment discussion AND scheduling), extract events for all relevant domains.
3. The "actor" is the person who performed the action (use their email address, or "me" for the account owner).
4. The "target" is the person the action was directed at (optional).
5. The "detail" should be a brief factual description of what specifically happened.
6. Assign a confidence score (0.0-1.0) based on how clearly the email evidences the event.
7. Use the email date as the event_date unless the email references a different date for the event.
8. If calendar events are provided, extract events from those too (e.g. meeting_held from a past calendar event).
9. Do NOT infer events that aren't evidenced. If you're unsure, skip it or use low confidence.
10. Do NOT extract events from automated notifications, newsletters, or marketing emails unless they evidence a real business event.

Respond with JSON only."""

EXTRACT_EVENTS_USER = """Extract business events from this email thread.
{owner_line}
Thread subject: {subject}
Participants: {participants}

Available domains and their event vocabularies:
{domains_block}

Messages (chronological):
{messages}
{calendar_block}
Respond with this exact JSON structure:
{{
  "domains": ["domain-name"],
  "events": [
    {{
      "type": "event_type_name",
      "domain": "domain-name",
      "actor": "email@example.com",
      "target": "email@example.com",
      "event_date": "YYYY-MM-DD",
      "detail": "Brief factual description",
      "confidence": 0.9,
      "source_email_index": 0
    }}
  ]
}}

If no business events are found, return {{"domains": [], "events": []}}."""


def format_email_for_prompt(email_row: dict, index: int) -> str:
    date = email_row.get("date", "")[:10]
    from_addr = email_row.get("from_name") or email_row.get("from_address", "")
    subject = email_row.get("subject", "(no subject)")
    body = (email_row.get("body_text") or "")[:500]  # truncate for prompt size
    return f"[Email {index}] Date: {date} | From: {from_addr} | Subject: {subject}\n{body}\n"
