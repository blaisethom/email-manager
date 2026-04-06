"""Assign relationship labels to companies using AI analysis of emails + homepage."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone


# ── Label config loading ─────────────────────────────────────────────────────

DEFAULT_LABELS = [
    {
        "name": "customer",
        "description": "A company that pays us for products or services.",
    },
    {
        "name": "prospect",
        "description": "A company we are trying to sell to but is not yet a customer.",
    },
    {
        "name": "vendor",
        "description": "A company that provides products or services to us.",
    },
    {
        "name": "partner",
        "description": "A company we collaborate with on joint initiatives.",
    },
    {
        "name": "investor",
        "description": "A company or fund that has invested in us or is considering investment.",
    },
    {
        "name": "recruiter",
        "description": "A recruitment agency or headhunter.",
    },
    {
        "name": "service-provider",
        "description": "A company providing professional services (legal, accounting, consulting).",
    },
    {
        "name": "internal",
        "description": "Our own company or a subsidiary/division of it.",
    },
    {
        "name": "other",
        "description": "Does not fit any of the above categories.",
    },
]


def load_label_config(config_path: Path | None = None) -> list[dict[str, str]]:
    """Load label definitions from a YAML or JSON file, or use defaults."""
    if config_path is None:
        # Try standard locations
        for candidate in (
            Path("company_labels.yaml"),
            Path("company_labels.yml"),
            Path("company_labels.json"),
            Path("data/company_labels.yaml"),
            Path("data/company_labels.yml"),
            Path("data/company_labels.json"),
        ):
            if candidate.exists():
                config_path = candidate
                break

    if config_path is None or not config_path.exists():
        return DEFAULT_LABELS

    text = config_path.read_text()
    if config_path.suffix in (".yaml", ".yml"):
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)

    # Accept either {"labels": [...]} or just [...]
    if isinstance(data, dict):
        labels = data.get("labels", [])
    else:
        labels = data

    if not labels or not isinstance(labels, list):
        return DEFAULT_LABELS

    return labels


# ── Prompt construction ──────────────────────────────────────────────────────

def _build_system_prompt(labels: list[dict[str, str]]) -> str:
    label_block = "\n".join(
        f'- "{l["name"]}": {l["description"]}' for l in labels
    )
    return f"""You are a company relationship classifier. Given information about a company — including email exchanges and their homepage — assign one or more relationship labels from the list below.

Available labels:
{label_block}

Rules:
1. Assign 1-3 labels that best describe the relationship.
2. Provide a confidence score (0.0-1.0) for each label.
3. Provide a brief one-sentence reasoning for each label.
4. Base your assessment on the email content, tone, direction of communication, and homepage content.
5. If the homepage is unavailable, rely on email evidence alone.
6. Also provide a 1-2 line description of what the company does.
7. Extract the official company name as it appears on the homepage or in emails (e.g. "Four Hats" not "Fourhats", "DeepMind" not "Deepmind"). If you cannot determine it, set "company_name" to null.

Respond with JSON only."""


def _build_user_prompt(
    company_name: str,
    domain: str,
    homepage_snippet: str,
    email_summaries: str,
    account_owner: str | None,
) -> str:
    owner_line = f"\nAccount owner: {account_owner}" if account_owner else ""
    return f"""Classify this company's relationship to the account owner.
{owner_line}
Company: {company_name}
Domain: {domain}

Homepage content (excerpt):
{homepage_snippet or "[Homepage not available]"}

Recent email exchanges:
{email_summaries or "[No emails available]"}

Respond with this exact JSON structure:
{{
  "company_name": "Official Company Name or null if unknown",
  "company_description": "1-2 line description of what the company does",
  "labels": [
    {{"label": "label-name", "confidence": 0.85, "reasoning": "Brief explanation"}}
  ]
}}"""


# ── Context gathering ────────────────────────────────────────────────────────

def _get_homepage_snippet(domain: str, max_chars: int = 3000) -> str:
    """Read the homepage markdown file for a domain and return a truncated excerpt."""
    from email_manager.analysis.homepage import homepage_path

    md_path = homepage_path(domain)
    if not md_path.exists():
        return ""

    text = md_path.read_text(encoding="utf-8")
    return text[:max_chars]


def _get_email_summaries(
    conn: sqlite3.Connection, domain: str, max_emails: int = 20
) -> str:
    """Get recent email snippets involving a company's domain."""
    like_pattern = f"%@{domain}%"
    rows = fetchall(
        conn,
        """SELECT date, from_address, from_name, subject, body_text
           FROM emails
           WHERE from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?
           ORDER BY date DESC
           LIMIT ?""",
        (like_pattern, like_pattern, like_pattern, max_emails),
    )
    if not rows:
        return ""

    parts = []
    for r in rows:
        sender = r["from_name"] or r["from_address"]
        date = (r["date"] or "")[:10]
        subject = r["subject"] or "(no subject)"
        body = (r["body_text"] or "")[:300]
        parts.append(f"[{date}] From: {sender} | Subject: {subject}\n{body}")

    return "\n---\n".join(parts)


def _detect_account_owner(conn: sqlite3.Connection) -> str | None:
    """Infer the account owner from the most common sender address."""
    row = fetchone(
        conn,
        "SELECT from_address, COUNT(*) as cnt FROM emails GROUP BY from_address ORDER BY cnt DESC LIMIT 1",
    )
    return row["from_address"] if row else None


# ── Main labelling function ──────────────────────────────────────────────────

def label_companies(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    labels_config: list[dict[str, str]] | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
    limit: int | None = None,
    force: bool = False,
    company_domain: str | None = None,
) -> int:
    """Assign relationship labels to companies using AI."""
    if labels_config is None:
        labels_config = load_label_config()

    system_prompt = _build_system_prompt(labels_config)
    valid_label_names = {l["name"] for l in labels_config}

    # Find companies that haven't been labelled yet (or all if force)
    if company_domain:
        # Scope to a single company by domain or name
        row = fetchone(
            conn,
            "SELECT id, name, domain FROM companies WHERE domain = ? COLLATE NOCASE",
            (company_domain,),
        )
        if not row:
            row = fetchone(
                conn,
                "SELECT id, name, domain FROM companies WHERE name LIKE ? COLLATE NOCASE",
                (f"%{company_domain}%",),
            )
        companies = [row] if row else []
    elif force:
        sql = "SELECT id, name, domain FROM companies ORDER BY email_count DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        companies = fetchall(conn, sql)
    else:
        sql = """SELECT c.id, c.name, c.domain FROM companies c
                 LEFT JOIN company_labels cl ON c.id = cl.company_id
                 WHERE cl.company_id IS NULL
                 ORDER BY c.email_count DESC"""
        if limit:
            sql += f" LIMIT {int(limit)}"
        companies = fetchall(conn, sql)
    if not companies:
        return 0

    account_owner = _detect_account_owner(conn)
    now = datetime.now(timezone.utc).isoformat()
    labelled = 0

    for i, company in enumerate(companies):
        company_display = f"{company['name']} ({company['domain']})"
        if on_progress:
            on_progress(i, len(companies), company_display)

        homepage_snippet = _get_homepage_snippet(company["domain"])
        email_summaries = _get_email_summaries(conn, company["domain"])

        if not email_summaries and not homepage_snippet:
            # Nothing to base a label on — skip
            continue

        user_prompt = _build_user_prompt(
            company["name"],
            company["domain"],
            homepage_snippet,
            email_summaries,
            account_owner,
        )

        try:
            result = backend.complete_json(system_prompt, user_prompt)
        except Exception:
            continue

        assigned_labels = result.get("labels", [])
        company_description = result.get("company_description", "")
        company_name = result.get("company_name")

        # Update name and/or description
        updates = []
        params = []
        if company_name and isinstance(company_name, str) and company_name.lower() != "null":
            updates.append("name = ?")
            params.append(company_name.strip())
        if company_description:
            updates.append("description = ?")
            params.append(company_description.strip())
        if updates:
            params.append(company["id"])
            conn.execute(
                f"UPDATE companies SET {', '.join(updates)} WHERE id = ?",
                params,
            )

        # Clear old labels if force
        if force:
            conn.execute("DELETE FROM company_labels WHERE company_id = ?", (company["id"],))

        for entry in assigned_labels:
            label_name = entry.get("label", "").strip()
            if label_name not in valid_label_names:
                continue
            confidence = min(1.0, max(0.0, float(entry.get("confidence", 0.5))))
            reasoning = entry.get("reasoning", "")

            conn.execute(
                """INSERT OR REPLACE INTO company_labels
                   (company_id, label, confidence, reasoning, model_used, assigned_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (company["id"], label_name, confidence, reasoning, backend.model_name, now),
            )

        conn.commit()
        labelled += 1

    if on_progress:
        on_progress(len(companies), len(companies), "done")

    return labelled
