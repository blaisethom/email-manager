"""Extract and track discussions from emails, with category and state tracking."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from difflib import SequenceMatcher
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone


# ── Category config loading ─────────────────────────────────────────────────

DEFAULT_CATEGORIES = [
    {
        "name": "scheduling",
        "description": "Scheduling a meeting, call, or event",
        "states": ["proposed", "confirmed", "completed", "cancelled"],
        "terminal_states": ["completed", "cancelled"],
    },
    {
        "name": "contract-negotiation",
        "description": "Negotiating terms of a contract or agreement",
        "states": ["initial_draft", "redlines", "final_review", "signed", "abandoned"],
        "terminal_states": ["signed", "abandoned"],
    },
    {
        "name": "vendor-selection",
        "description": "Evaluating and selecting a vendor or service provider",
        "states": ["research", "shortlisted", "evaluating", "selected", "onboarded", "rejected"],
        "terminal_states": ["onboarded", "rejected"],
    },
    {
        "name": "internal-decision",
        "description": "Internal process or decision-making discussion",
        "states": ["raised", "under_discussion", "decided", "implemented", "deferred"],
        "terminal_states": ["implemented", "deferred"],
    },
    {
        "name": "other",
        "description": "Discussion that does not fit any other category",
        "states": ["active", "resolved", "stalled"],
        "terminal_states": ["resolved"],
    },
]


def load_category_config(config_path: Path | None = None) -> list[dict[str, Any]]:
    """Load discussion category definitions from a YAML or JSON file, or use defaults."""
    if config_path is None:
        for candidate in (
            Path("discussion_categories.yaml"),
            Path("discussion_categories.yml"),
            Path("discussion_categories.json"),
            Path("data/discussion_categories.yaml"),
            Path("data/discussion_categories.yml"),
            Path("data/discussion_categories.json"),
        ):
            if candidate.exists():
                config_path = candidate
                break

    if config_path is None or not config_path.exists():
        return DEFAULT_CATEGORIES

    text = config_path.read_text()
    if config_path.suffix in (".yaml", ".yml"):
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)

    if isinstance(data, dict):
        categories = data.get("categories", [])
    else:
        categories = data

    if not categories or not isinstance(categories, list):
        return DEFAULT_CATEGORIES

    return categories


# ── Prompt construction ──────────────────────────────────────────────────────

def _build_system_prompt(categories: list[dict[str, Any]]) -> str:
    cat_block = ""
    for cat in categories:
        terminal = set(cat.get("terminal_states", []))
        states_parts = []
        for s in cat["states"]:
            states_parts.append(f"{s}*" if s in terminal else s)
        states_str = " → ".join(states_parts)
        cat_block += f'- "{cat["name"]}": {cat["description"]}\n  States: {states_str}  (* = terminal)\n'

    return f"""You are a discussion extraction system. Given email threads found via a company, identify distinct discussions and classify each into a category with its current state.

Available categories and their state sequences:
{cat_block}
Rules:
1. Each discussion should have a clear topic/title (short, descriptive, 5-10 words).
2. Assign exactly one category per discussion. Use "other" if nothing else fits.
3. Assign the current state from that category's state sequence.
4. For each state transition you can infer from the emails, provide the date it occurred and which email evidences it.
5. Link each discussion to the thread IDs that relate to it. A discussion may span multiple threads.
6. Provide a 1-2 sentence summary of each discussion.
7. List all participants (email addresses) for each discussion. Select from the provided list of email addresses found in the threads.
8. States are ordered — earlier states come before later ones in time. Some states are terminal exits (e.g. lost, abandoned, cancelled) that can be reached from any prior state.
9. For each discussion, identify the most relevant external company domain (not the account owner's domain). A thread may contain conversations with multiple companies — assign each discussion to the company it is primarily about, using "company_domain".
10. For each discussion, extract any actions/tasks that are required. An action is something a specific person has been asked to do, has committed to doing, or clearly needs to do. For each action include:
    - A clear description of what needs to be done — this should describe the TASK ITSELF, not who does it
    - The email addresses of ALL people responsible or involved in the action (as a JSON array). Include both the person who needs to do it and any key participants (e.g. for "schedule a meeting between A and B", include both A and B)
    - The target/due date if one is mentioned or implied (YYYY-MM-DD format), or null if not specified
    - The status: "open" if the action is still pending, "done" if it has been completed based on the email evidence
    - The approximate date the action was identified/requested (YYYY-MM-DD format)
    - If the action is done, the approximate date it was completed (YYYY-MM-DD format), or null if unknown
    IMPORTANT: Consolidate related actions into a single action. If multiple emails refer to the same underlying task (e.g. scheduling the same meeting, sharing the same document), emit it ONCE with the most complete information. Do NOT create separate actions for each email mentioning the same task. For example, if email 1 says "let's schedule a call" and email 2 says "how about Tuesday?", that is ONE action, not two.
    Extract actions from ALL emails, including historical ones. Even if an action was requested and completed long ago, include it with status "done" and the completed_date. This builds a complete history of what was asked and delivered.
    Only include concrete, actionable items — not vague intentions. If no actions are evident, return an empty actions list.
    When updating existing discussions that already have actions, preserve all existing actions and update their status if you see evidence of completion. Add any new actions found in the current batch of emails.

Respond with JSON only."""


def _build_user_prompt(
    company_name: str,
    domain: str,
    emails_text: str,
    existing_discussions: list[dict] | None = None,
    account_owner: str | None = None,
    batch_num: int | None = None,
    total_batches: int | None = None,
    email_addresses: set[str] | None = None,
) -> str:
    owner_line = f"\nAccount owner: {account_owner}" if account_owner else ""
    batch_line = ""
    if batch_num is not None and total_batches is not None and total_batches > 1:
        batch_line = f"\nThis is batch {batch_num}/{total_batches} of emails for this company."

    existing_block = ""
    if existing_discussions:
        existing_block = "\n\nDiscussions identified so far (update these or add new ones):\n"
        for d in existing_discussions:
            state_hist = ""
            if d.get("state_history"):
                transitions = ", ".join(
                    f'{sh["state"]}@{sh.get("date", "?")}' for sh in d["state_history"]
                )
                state_hist = f" history=[{transitions}]"
            company_dom = d.get("company_domain", "")
            company_info = f' company={company_dom}' if company_dom else ""
            actions_info = ""
            if d.get("actions"):
                action_parts = []
                for a in d["actions"]:
                    assignees = a.get("assignee_emails") or "?"
                    a_str = f'{a["status"]}: "{a["description"]}" -> {assignees}'
                    if a.get("completed_date"):
                        a_str += f' (completed {a["completed_date"]})'
                    action_parts.append(a_str)
                actions_info = f' actions=[{"; ".join(action_parts)}]'
            existing_block += (
                f'- ID {d["id"]}: "{d["title"]}" [{d["category"]}] '
                f'state={d["current_state"]}{state_hist}{company_info} threads={d["thread_ids"]}{actions_info}\n'
            )

    addresses_block = ""
    if email_addresses:
        sorted_addrs = sorted(email_addresses)
        addresses_block = "\n\nEmail addresses found in these threads (select participants from this list):\n" + ", ".join(sorted_addrs)

    return f"""Identify all distinct discussions in the emails below for this company.
{owner_line}{batch_line}
Company: {company_name}
Domain: {domain}
{existing_block}{addresses_block}
Emails:
{emails_text}

Respond with this exact JSON structure:
{{
  "discussions": [
    {{
      "existing_id": null,
      "title": "Short descriptive title",
      "category": "category-name",
      "current_state": "state-name",
      "company_domain": "example.com",
      "summary": "1-2 sentence summary",
      "participants": ["email@example.com"],
      "thread_ids": ["thread-id-1", "thread-id-2"],
      "state_history": [
        {{
          "state": "state-name",
          "date": "YYYY-MM-DD",
          "evidence_summary": "Brief description of what email shows this state"
        }}
      ],
      "actions": [
        {{
          "description": "What needs to be done",
          "assignee_emails": ["person1@example.com", "person2@example.com"],
          "target_date": "YYYY-MM-DD or null",
          "status": "open or done",
          "source_date": "YYYY-MM-DD",
          "completed_date": "YYYY-MM-DD or null"
        }}
      ]
    }}
  ]
}}

Notes:
- Set "existing_id" to the ID number if updating an existing discussion, or null for new ones.
- Include ALL discussions you can identify, even minor ones like scheduling.
- Each thread should belong to at most one discussion (but a discussion can span multiple threads).
- For state_history, include each state transition you can infer from the email content with the approximate date.
- When updating an existing discussion, include its full updated state_history (existing + new transitions), not just new ones.
- "company_domain" should be the domain of the external company this discussion is primarily about (not the account owner's domain). For example, if the account owner emails investor@vc-firm.com, the company_domain should be "vc-firm.com".
- For "actions": extract concrete tasks/actions from the emails, including historical/completed ones. Each action must have assignee_emails (array) from the participants list — include all people involved in carrying out the action. Consolidate: if the same task is mentioned across multiple emails, emit it only ONCE. Set target_date only if a deadline is mentioned. Set status to "done" if follow-up emails show the action was completed, otherwise "open". Set source_date to the date of the email where the action was first identified. Set completed_date to the date of the email that shows completion (null if still open or completion date unknown). When updating existing discussions, include all their existing actions (update status/completed_date if needed) plus any new ones."""


# ── Quote stripping & deduplication ──────────────────────────────────────────

# Matches "On <date>, <person> wrote:" (Gmail-style) and variants
_ON_WROTE_RE = re.compile(
    r"^On .{10,80} wrote:\s*$", re.MULTILINE
)

# Matches Outlook-style quote headers: "From: ... Sent: ..."
_OUTLOOK_HEADER_RE = re.compile(
    r"^-{2,}\s*Original Message\s*-{2,}\s*$"
    r"|^From:\s+.+\nSent:\s+.+\nTo:\s+.+",
    re.MULTILINE,
)


def _strip_quoted_text(body: str) -> str:
    """Remove quoted/forwarded content from an email body.

    Handles:
    - Lines starting with '>' (standard quoting)
    - 'On ... wrote:' blocks followed by quoted text (Gmail)
    - '-----Original Message-----' / 'From: ... Sent: ...' blocks (Outlook)
    - 'From: ... Sent: ... To: ... Subject: ...' inline forwards
    """
    if not body:
        return ""

    lines = body.split("\n")
    cleaned: list[str] = []
    skip_rest = False

    for line in lines:
        stripped = line.strip()

        # Once we decide to skip, everything below is quoted
        if skip_rest:
            continue

        # Skip '>' quoted lines
        if stripped.startswith(">"):
            continue

        # Detect "On ... wrote:" — skip this line and everything after
        if _ON_WROTE_RE.match(stripped):
            skip_rest = True
            continue

        # Detect "-----Original Message-----"
        if re.match(r"^-{2,}\s*Original Message\s*-{2,}$", stripped, re.IGNORECASE):
            skip_rest = True
            continue

        # Detect Outlook inline header block: "From: X\nSent: Y\nTo: Z\nSubject: W"
        # We catch the "From:" line that's followed by "Sent:" — but we only see
        # one line at a time, so use a heuristic: "From:" at start of line with
        # a known Outlook pattern (contains "Sent:" nearby in prior context)
        if re.match(r"^From:\s+\S+.*", stripped) and len(cleaned) > 0:
            # Look back: if previous non-empty line is blank or a signature separator,
            # this is likely a quoted header block
            prev = cleaned[-1].strip() if cleaned else ""
            if prev == "" or prev.startswith("--") or prev.startswith("__"):
                skip_rest = True
                continue

        cleaned.append(line)

    # Strip trailing blank lines
    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()

    return "\n".join(cleaned)


def _dedup_against_previous(body: str, previous_bodies: list[str], min_dup_lines: int = 3) -> str:
    """Remove runs of lines that appeared in previous emails in the thread.

    Catches cases where the quoted text has no '>' markers (plain copy-paste).
    Only removes consecutive runs of `min_dup_lines` or more matching lines.
    """
    if not body or not previous_bodies:
        return body

    # Build set of normalised lines from all previous emails
    prev_lines: set[str] = set()
    for pb in previous_bodies:
        for line in pb.split("\n"):
            norm = line.strip().lower()
            if len(norm) > 20:  # Only dedup substantial lines
                prev_lines.add(norm)

    lines = body.split("\n")
    # Mark each line as duplicate or not
    is_dup = [line.strip().lower() in prev_lines for line in lines]

    # Only remove consecutive runs of min_dup_lines or more
    result: list[str] = []
    run_start = 0
    i = 0
    while i < len(lines):
        if is_dup[i]:
            run_start = i
            while i < len(lines) and is_dup[i]:
                i += 1
            run_len = i - run_start
            if run_len < min_dup_lines:
                # Short run — keep it (likely a coincidence)
                result.extend(lines[run_start:i])
            # else: drop the whole run
        else:
            result.append(lines[i])
            i += 1

    # Strip trailing blank lines
    while result and result[-1].strip() == "":
        result.pop()

    return "\n".join(result)


# ── Context gathering ────────────────────────────────────────────────────────

def _format_email(e: dict, body_limit: int = 500) -> str:
    """Format a single email row into a prompt-friendly string."""
    sender = e["from_name"] or e["from_address"]
    date = (e["date"] or "")[:10]
    body = (e["body_text"] or "")[:body_limit]
    return (
        f"[{date}] From: {sender} To: {e['to_addresses'] or ''}\n"
        f"Subject: {e['subject'] or '(no subject)'}\n"
        f"{body}\n"
    )


def _format_thread_emails(emails: list[dict], body_per_email: int = 500) -> list[str]:
    """Format emails for a thread, stripping quoted text and deduplicating.

    Processes emails in chronological order. Each email's body is:
    1. Stripped of quoted text (> lines, On...wrote:, Outlook headers)
    2. Deduplicated against previous emails' bodies in the thread
    3. Truncated to body_per_email chars
    """
    previous_bodies: list[str] = []
    formatted: list[str] = []

    for e in emails:
        raw_body = e["body_text"] or ""

        # Step 1: strip quoted text patterns
        clean_body = _strip_quoted_text(raw_body)

        # Step 2: remove runs of lines that appeared in earlier emails
        clean_body = _dedup_against_previous(clean_body, previous_bodies)

        # Track the cleaned body for future dedup (before truncation)
        previous_bodies.append(clean_body)

        # Step 3: truncate
        body = clean_body[:body_per_email]

        sender = e["from_name"] or e["from_address"]
        date = (e["date"] or "")[:10]
        formatted.append(
            f"[{date}] From: {sender} To: {e['to_addresses'] or ''}\n"
            f"Subject: {e['subject'] or '(no subject)'}\n"
            f"{body}\n"
        )

    return formatted


_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _extract_addresses_from_emails(emails: list[dict]) -> set[str]:
    """Extract all unique email addresses from email records."""
    addrs: set[str] = set()
    for e in emails:
        for field in ("from_address", "to_addresses", "cc_addresses"):
            val = e[field]
            if val:
                addrs.update(_EMAIL_RE.findall(val))
    return {a.lower() for a in addrs}


def _get_company_emails_batched(
    conn: sqlite3.Connection, domain: str, max_threads: int = 50,
    batch_char_limit: int = 30000, body_per_email: int = 500,
) -> list[tuple[str, list[str], set[str]]]:
    """Get all emails for a company domain, split into batches.

    Each batch is a chunk of emails (in chronological order across threads)
    formatted for a single AI call. Returns list of (formatted_text, thread_ids_in_batch, email_addresses).
    """
    like_pattern = f"%@{domain}%"

    thread_ids_rows = fetchall(
        conn,
        """SELECT DISTINCT e.thread_id
           FROM emails e
           WHERE (e.from_address LIKE ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?)
             AND e.thread_id IS NOT NULL
           ORDER BY MAX(e.date) OVER (PARTITION BY e.thread_id) DESC
           LIMIT ?""",
        (like_pattern, like_pattern, like_pattern, max_threads),
    )
    if not thread_ids_rows:
        return []

    # Collect all emails grouped by thread, sorted chronologically within each
    thread_emails: dict[str, list[dict]] = {}
    for r in thread_ids_rows:
        tid = r["thread_id"]
        emails = fetchall(
            conn,
            """SELECT date, from_address, from_name, to_addresses, cc_addresses, subject, body_text
               FROM emails
               WHERE thread_id = ?
               ORDER BY date ASC""",
            (tid,),
        )
        if emails:
            thread_emails[tid] = emails

    if not thread_emails:
        return []

    # Build batches: pack threads into batches up to the character limit.
    # Keep whole threads together when possible, but split large threads.
    batches: list[tuple[str, list[str], set[str]]] = []
    current_parts: list[str] = []
    current_thread_ids: list[str] = []
    current_addrs: set[str] = set()
    current_chars = 0

    for tid, emails in thread_emails.items():
        thread_subject = emails[0]["subject"] or "(no subject)"
        header = f"=== Thread [{tid}]: {thread_subject} ({len(emails)} emails) ==="
        thread_addrs = _extract_addresses_from_emails(emails)

        # Format emails with quote stripping and deduplication
        formatted_emails = _format_thread_emails(emails, body_per_email)
        thread_text = header + "\n" + "\n".join(formatted_emails)

        if len(thread_text) + current_chars <= batch_char_limit or not current_parts:
            # Fits in current batch (or batch is empty — always add at least one thread)
            current_parts.append(thread_text)
            current_thread_ids.append(tid)
            current_addrs.update(thread_addrs)
            current_chars += len(thread_text)
        elif len(thread_text) > batch_char_limit:
            # Thread itself exceeds limit — flush current batch, then split thread
            if current_parts:
                batches.append(("\n\n".join(current_parts), current_thread_ids, current_addrs))
                current_parts = []
                current_thread_ids = []
                current_addrs = set()
                current_chars = 0

            # Split this large thread into sub-batches of emails
            chunk_parts: list[str] = [header]
            chunk_chars = len(header)
            for fe in formatted_emails:
                if chunk_chars + len(fe) > batch_char_limit and chunk_chars > len(header):
                    batches.append(("\n".join(chunk_parts), [tid], thread_addrs))
                    chunk_parts = [header + " (continued)"]
                    chunk_chars = len(chunk_parts[0])
                chunk_parts.append(fe)
                chunk_chars += len(fe)
            if len(chunk_parts) > 1:
                batches.append(("\n".join(chunk_parts), [tid], thread_addrs))
        else:
            # Doesn't fit — flush current batch, start new one with this thread
            batches.append(("\n\n".join(current_parts), current_thread_ids, current_addrs))
            current_parts = [thread_text]
            current_thread_ids = [tid]
            current_addrs = thread_addrs.copy()
            current_chars = len(thread_text)

    if current_parts:
        batches.append(("\n\n".join(current_parts), current_thread_ids, current_addrs))

    return batches




def _detect_account_owner(conn: sqlite3.Connection) -> str | None:
    row = fetchone(
        conn,
        "SELECT from_address, COUNT(*) as cnt FROM emails GROUP BY from_address ORDER BY cnt DESC LIMIT 1",
    )
    return row["from_address"] if row else None


# ── Saving results ───────────────────────────────────────────────────────────

def _resolve_company_id(
    conn: sqlite3.Connection, domain: str | None, fallback_company_id: int,
) -> int:
    """Resolve a company domain from the AI response to a company_id.

    If the domain exists in the companies table, return its id.
    Otherwise fall back to the company we were processing.
    """
    if not domain:
        return fallback_company_id
    domain = domain.strip().lower()
    row = fetchone(
        conn,
        "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE",
        (domain,),
    )
    return row["id"] if row else fallback_company_id


logger = logging.getLogger("email_manager.analysis.discussions")


def _find_duplicate_discussion(
    conn: sqlite3.Connection,
    company_id: int,
    title: str,
    category: str,
    thread_ids: list[str],
) -> int | None:
    """Find an existing discussion that likely duplicates the proposed one.

    Uses three signals (strongest first):
    1. Thread overlap — if any thread_ids are already linked to an existing
       discussion for this company, that's almost certainly the same discussion.
    2. Category + title similarity — same category and a high title-similarity
       ratio (>=0.7) indicates a duplicate with a slightly different title.

    Returns the existing discussion id, or None.
    """
    # Signal 1: thread overlap within the same category
    if thread_ids:
        placeholders = ",".join("?" for _ in thread_ids)
        row = fetchone(
            conn,
            f"""SELECT dt.discussion_id
                FROM discussion_threads dt
                JOIN discussions d ON dt.discussion_id = d.id
                WHERE d.company_id = ? AND d.category = ?
                  AND dt.thread_id IN ({placeholders})
                LIMIT 1""",
            (company_id, category, *thread_ids),
        )
        if row:
            return row["discussion_id"]

    # Signal 2: category + fuzzy title
    existing = fetchall(
        conn,
        "SELECT id, title FROM discussions WHERE company_id = ? AND category = ?",
        (company_id, category),
    )
    title_lower = title.lower()
    for ex in existing:
        ratio = SequenceMatcher(None, title_lower, ex["title"].lower()).ratio()
        if ratio >= 0.7:
            return ex["id"]

    return None


def dedupe_discussions(conn: sqlite3.Connection, dry_run: bool = True) -> list[tuple[int, int]]:
    """Scan existing discussions and merge duplicates.

    Groups discussions by company, then within each company finds duplicates
    using thread overlap and title similarity.  The oldest discussion (lowest id)
    is kept; newer duplicates are merged into it.

    Returns list of (kept_id, merged_id) pairs.
    """
    companies = fetchall(
        conn,
        "SELECT DISTINCT company_id FROM discussions ORDER BY company_id",
    )
    merges: list[tuple[int, int]] = []

    for company_row in companies:
        cid = company_row["company_id"]
        discs = fetchall(
            conn,
            """SELECT d.id, d.title, d.category, d.current_state, d.summary,
                      d.participants, d.first_seen, d.last_seen
               FROM discussions d
               WHERE d.company_id = ?
               ORDER BY d.id ASC""",
            (cid,),
        )

        # Load thread_ids for each discussion
        disc_threads: dict[int, set[str]] = {}
        for d in discs:
            rows = fetchall(
                conn,
                "SELECT thread_id FROM discussion_threads WHERE discussion_id = ?",
                (d["id"],),
            )
            disc_threads[d["id"]] = {r["thread_id"] for r in rows}

        # Track which discussions have been merged away
        merged_away: set[int] = set()

        for i, d in enumerate(discs):
            if d["id"] in merged_away:
                continue
            for j in range(i + 1, len(discs)):
                other = discs[j]
                if other["id"] in merged_away:
                    continue

                is_dup = False
                # Check thread overlap (same category only)
                overlap = disc_threads[d["id"]] & disc_threads[other["id"]]
                if overlap and d["category"] == other["category"]:
                    is_dup = True
                # Check category + title similarity
                elif d["category"] == other["category"]:
                    ratio = SequenceMatcher(
                        None, d["title"].lower(), other["title"].lower()
                    ).ratio()
                    if ratio >= 0.7:
                        is_dup = True

                if is_dup:
                    merges.append((d["id"], other["id"]))
                    merged_away.add(other["id"])
                    # Absorb threads from the duplicate
                    disc_threads[d["id"]] |= disc_threads[other["id"]]

    if dry_run:
        return merges

    # Apply merges
    for keep_id, remove_id in merges:
        # Move threads to the kept discussion
        threads = fetchall(
            conn,
            "SELECT thread_id FROM discussion_threads WHERE discussion_id = ?",
            (remove_id,),
        )
        for t in threads:
            conn.execute(
                "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
                (keep_id, t["thread_id"]),
            )

        # Move state history entries
        conn.execute(
            "UPDATE discussion_state_history SET discussion_id = ? WHERE discussion_id = ?",
            (keep_id, remove_id),
        )

        # Move actions
        conn.execute(
            "UPDATE actions SET discussion_id = ? WHERE discussion_id = ?",
            (keep_id, remove_id),
        )

        # Update date range on kept discussion
        conn.execute(
            """UPDATE discussions SET
                 first_seen = COALESCE(
                   (SELECT MIN(d.first_seen) FROM discussions d WHERE d.id IN (?, ?)),
                   first_seen),
                 last_seen = COALESCE(
                   (SELECT MAX(d.last_seen) FROM discussions d WHERE d.id IN (?, ?)),
                   last_seen)
               WHERE id = ?""",
            (keep_id, remove_id, keep_id, remove_id, keep_id),
        )

        # Delete the duplicate
        conn.execute("DELETE FROM actions WHERE discussion_id = ?", (remove_id,))
        conn.execute("DELETE FROM discussion_threads WHERE discussion_id = ?", (remove_id,))
        conn.execute("DELETE FROM discussion_state_history WHERE discussion_id = ?", (remove_id,))
        conn.execute("DELETE FROM discussions WHERE id = ?", (remove_id,))

    conn.commit()
    return merges


def repair_discussion_dates(conn: sqlite3.Connection) -> int:
    """Recompute first_seen/last_seen for all discussions.

    Uses state-history dates first, then extends with thread emails filtered
    by the company domain (so old unrelated emails in reused threads are
    excluded).  Returns the number of discussions updated.
    """
    discussions = fetchall(
        conn,
        """SELECT d.id, d.company_id, d.first_seen, d.last_seen
           FROM discussions d""",
    )
    updated = 0

    for disc in discussions:
        did = disc["id"]
        company_id = disc["company_id"]

        # 1. State-history dates
        history = fetchall(
            conn,
            "SELECT entered_at FROM discussion_state_history WHERE discussion_id = ? AND entered_at IS NOT NULL AND entered_at != ''",
            (did,),
        )
        state_dates = sorted(r["entered_at"] for r in history if r["entered_at"])
        first_seen = state_dates[0] if state_dates else None
        last_seen = state_dates[-1] if state_dates else None

        # 2. Thread emails filtered by company domain
        thread_rows = fetchall(
            conn,
            "SELECT thread_id FROM discussion_threads WHERE discussion_id = ?",
            (did,),
        )
        thread_ids = [r["thread_id"] for r in thread_rows]

        if thread_ids:
            company_row = fetchone(
                conn, "SELECT domain FROM companies WHERE id = ?", (company_id,),
            )
            placeholders = ",".join("?" for _ in thread_ids)

            if company_row and company_row["domain"]:
                like_pattern = f"%@{company_row['domain']}%"
                date_row = fetchone(
                    conn,
                    f"""SELECT MIN(date) as first_d, MAX(date) as last_d
                        FROM emails
                        WHERE thread_id IN ({placeholders})
                          AND (from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?)""",
                    (*thread_ids, like_pattern, like_pattern, like_pattern),
                )
            else:
                date_row = fetchone(
                    conn,
                    f"SELECT MIN(date) as first_d, MAX(date) as last_d FROM emails WHERE thread_id IN ({placeholders})",
                    tuple(thread_ids),
                )

            if date_row and date_row["first_d"]:
                if not first_seen or date_row["first_d"] < first_seen:
                    first_seen = date_row["first_d"]
                if not last_seen or date_row["last_d"] > last_seen:
                    last_seen = date_row["last_d"]

        if first_seen != disc["first_seen"] or last_seen != disc["last_seen"]:
            conn.execute(
                "UPDATE discussions SET first_seen = ?, last_seen = ? WHERE id = ?",
                (first_seen, last_seen, did),
            )
            updated += 1

    conn.commit()
    return updated


def _save_discussions(
    conn: sqlite3.Connection,
    company_id: int,
    discussions: list[dict],
    valid_categories: dict[str, set[str]],
    model_name: str,
    force: bool,
) -> int:
    """Save extracted discussions to database. Returns number saved."""
    now = datetime.now(timezone.utc).isoformat()
    saved = 0

    for disc in discussions:
        category = disc.get("category", "other").strip()
        if category not in valid_categories:
            category = "other"

        valid_states = valid_categories.get(category, set())
        current_state = disc.get("current_state", "").strip()
        if current_state not in valid_states:
            # Pick first state as default
            current_state = next(iter(valid_states)) if valid_states else ""

        title = (disc.get("title") or "Untitled").strip()
        summary = (disc.get("summary") or "").strip()
        participants = json.dumps(disc.get("participants", []))
        thread_ids = disc.get("thread_ids", [])

        # Resolve the company from the AI-assigned domain, falling back to the
        # company we were originally processing.
        resolved_company_id = _resolve_company_id(
            conn, disc.get("company_domain"), company_id,
        )

        # Compute date range.  Prefer state-history dates (they track the
        # actual discussion timeline).  Fall back to thread emails filtered
        # by the company domain so that old, unrelated emails in reused
        # threads (e.g. generic "Materials" subject) don't pull the date back.
        first_seen = None
        last_seen = None

        state_history = disc.get("state_history", [])
        state_dates = sorted(
            sh.get("date", "") for sh in state_history if sh.get("date")
        )
        if state_dates:
            first_seen = state_dates[0]
            last_seen = state_dates[-1]

        if thread_ids:
            placeholders = ",".join("?" for _ in thread_ids)
            # Resolve the company domain for filtering
            company_row = fetchone(
                conn,
                "SELECT domain FROM companies WHERE id = ?",
                (resolved_company_id,),
            )
            company_domain = company_row["domain"] if company_row else None

            if company_domain:
                like_pattern = f"%@{company_domain}%"
                date_row = fetchone(
                    conn,
                    f"""SELECT MIN(date) as first_d, MAX(date) as last_d
                        FROM emails
                        WHERE thread_id IN ({placeholders})
                          AND (from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?)""",
                    (*thread_ids, like_pattern, like_pattern, like_pattern),
                )
            else:
                date_row = fetchone(
                    conn,
                    f"SELECT MIN(date) as first_d, MAX(date) as last_d FROM emails WHERE thread_id IN ({placeholders})",
                    tuple(thread_ids),
                )

            if date_row and date_row["first_d"]:
                # Use thread dates if state history is missing, or to extend
                # the range when threads cover a wider window.
                if not first_seen or date_row["first_d"] < first_seen:
                    first_seen = date_row["first_d"]
                if not last_seen or date_row["last_d"] > last_seen:
                    last_seen = date_row["last_d"]

        existing_id = disc.get("existing_id")

        # Guard against duplicates: if the AI didn't link to an existing
        # discussion, check whether one already exists that matches.
        if not existing_id:
            dup_id = _find_duplicate_discussion(
                conn, resolved_company_id, title, category, thread_ids,
            )
            if dup_id:
                logger.debug("Dedup: merging new '%s' into existing discussion %d", title, dup_id)
                existing_id = dup_id

        if existing_id:
            # Update existing discussion
            conn.execute(
                """UPDATE discussions
                   SET title = ?, category = ?, current_state = ?, company_id = ?,
                       summary = ?, participants = ?,
                       first_seen = COALESCE(?, first_seen),
                       last_seen = COALESCE(?, last_seen),
                       model_used = ?, updated_at = ?
                   WHERE id = ?""",
                (title, category, current_state, resolved_company_id,
                 summary, participants,
                 first_seen, last_seen, model_name, now,
                 existing_id),
            )
            discussion_id = existing_id
        else:
            cursor = conn.execute(
                """INSERT INTO discussions
                   (title, category, current_state, company_id, summary, participants,
                    first_seen, last_seen, model_used, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (title, category, current_state, resolved_company_id, summary,
                 participants, first_seen, last_seen, model_name, now),
            )
            discussion_id = cursor.lastrowid

        # Link threads
        for tid in thread_ids:
            conn.execute(
                "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
                (discussion_id, tid),
            )

        # Save state history
        for sh in disc.get("state_history", []):
            state = sh.get("state", "").strip()
            if state not in valid_states:
                continue
            entered_at = sh.get("date", "")
            evidence = sh.get("evidence_summary", "")

            # Avoid duplicate state entries for same discussion+state+date
            existing = fetchone(
                conn,
                """SELECT id FROM discussion_state_history
                   WHERE discussion_id = ? AND state = ? AND entered_at = ?""",
                (discussion_id, state, entered_at),
            )
            if not existing:
                conn.execute(
                    """INSERT INTO discussion_state_history
                       (discussion_id, state, entered_at, reasoning, model_used, detected_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (discussion_id, state, entered_at, evidence, model_name, now),
                )

        # Save actions
        for action in disc.get("actions", []):
            description = (action.get("description") or "").strip()
            if not description:
                continue

            # Handle assignee_emails — accept both array and legacy single string
            raw_assignees = action.get("assignee_emails") or action.get("assignee_email")
            if isinstance(raw_assignees, str):
                assignee_list = [raw_assignees.strip().lower()] if raw_assignees.strip() else []
            elif isinstance(raw_assignees, list):
                assignee_list = [a.strip().lower() for a in raw_assignees if isinstance(a, str) and a.strip()]
            else:
                assignee_list = []
            assignee_emails_json = json.dumps(assignee_list)

            target_date = action.get("target_date") or None
            if target_date == "null":
                target_date = None
            status = (action.get("status") or "open").strip().lower()
            if status not in ("open", "done"):
                status = "open"
            source_date = action.get("source_date") or None
            completed_date = action.get("completed_date") or None
            if completed_date == "null":
                completed_date = None

            # Dedup on discussion + description (the task itself)
            existing_action = fetchone(
                conn,
                """SELECT id FROM actions
                   WHERE discussion_id = ? AND description = ?""",
                (discussion_id, description),
            )
            if existing_action:
                # Update status, assignees, target_date, and completed_date
                conn.execute(
                    """UPDATE actions SET status = ?,
                       assignee_emails = ?,
                       target_date = COALESCE(?, target_date),
                       completed_date = COALESCE(?, completed_date)
                       WHERE id = ?""",
                    (status, assignee_emails_json, target_date, completed_date, existing_action["id"]),
                )
            else:
                conn.execute(
                    """INSERT INTO actions
                       (discussion_id, description, assignee_emails, target_date, status,
                        source_date, completed_date, model_used, detected_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (discussion_id, description, assignee_emails_json, target_date, status,
                     source_date, completed_date, model_name, now),
                )

        saved += 1

    return saved


# ── Main extraction function ─────────────────────────────────────────────────

def _discussions_to_context(conn: sqlite3.Connection, company_id: int) -> list[dict]:
    """Build discussion context dicts (with state history) for passing back to the AI."""
    rows = fetchall(
        conn,
        """SELECT d.id, d.title, d.category, d.current_state,
                  c.domain as company_domain
           FROM discussions d
           LEFT JOIN companies c ON d.company_id = c.id
           WHERE d.company_id = ?
           ORDER BY d.last_seen DESC""",
        (company_id,),
    )
    result = []
    for r in rows:
        thread_rows = fetchall(
            conn,
            "SELECT thread_id FROM discussion_threads WHERE discussion_id = ?",
            (r["id"],),
        )
        history_rows = fetchall(
            conn,
            """SELECT state, entered_at as date, reasoning as evidence_summary
               FROM discussion_state_history
               WHERE discussion_id = ?
               ORDER BY entered_at ASC, id ASC""",
            (r["id"],),
        )
        action_rows = fetchall(
            conn,
            """SELECT description, assignee_emails, target_date, status,
                      source_date, completed_date
               FROM actions
               WHERE discussion_id = ?
               ORDER BY source_date ASC, id ASC""",
            (r["id"],),
        )
        result.append({
            "id": r["id"],
            "title": r["title"],
            "category": r["category"],
            "current_state": r["current_state"],
            "company_domain": r["company_domain"],
            "thread_ids": [t["thread_id"] for t in thread_rows],
            "state_history": [dict(h) for h in history_rows],
            "actions": [dict(a) for a in action_rows],
        })
    return result


def extract_discussions(
    conn: sqlite3.Connection,
    backend: LLMBackend,
    categories_config: list[dict[str, Any]] | None = None,
    on_company_progress: Callable[[int, int, str], None] | None = None,
    on_batch_progress: Callable[[int, int], None] | None = None,
    on_step: Callable[[str], None] | None = None,
    limit: int | None = None,
    force: bool = False,
    company_domain: str | None = None,
    company_label: str | None = None,
    exclude_companies: list[str] | None = None,
    contact_email: str | None = None,
) -> int:
    """Extract discussions from email threads, grouped by company.

    Emails are split into batches and processed incrementally — each AI call
    sees the discussions accumulated so far and can update or add to them.

    on_company_progress(done, total, company_name) — called when moving to next company.
    on_batch_progress(done, total) — called when moving to next batch within a company.
    on_step(description) — called for sub-steps (gathering, calling AI, saving).
    """
    if categories_config is None:
        categories_config = load_category_config()

    system_prompt = _build_system_prompt(categories_config)
    valid_categories: dict[str, set[str]] = {
        c["name"]: set(c["states"]) for c in categories_config
    }

    # Build company query based on filters
    if contact_email:
        contact_row = fetchone(
            conn,
            """SELECT c.id, c.name, c.domain FROM companies c
               JOIN company_contacts cc ON c.id = cc.company_id
               WHERE cc.contact_email = ?""",
            (contact_email,),
        )
        if not contact_row:
            return 0
        companies = [contact_row]
    elif company_domain:
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
        if not row:
            return 0
        companies = [row]
    elif company_label:
        sql = """SELECT c.id, c.name, c.domain FROM companies c
                 JOIN company_labels cl ON c.id = cl.company_id
                 WHERE cl.label = ?"""
        if not force:
            sql += """ AND NOT EXISTS (
                         SELECT 1 FROM discussions d WHERE d.company_id = c.id
                     )"""
        sql += " ORDER BY c.email_count DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        companies = fetchall(conn, sql, (company_label,))
    else:
        if force:
            sql = "SELECT id, name, domain FROM companies ORDER BY email_count DESC"
        else:
            sql = """SELECT c.id, c.name, c.domain FROM companies c
                     WHERE NOT EXISTS (
                         SELECT 1 FROM discussions d WHERE d.company_id = c.id
                     )
                     ORDER BY c.email_count DESC"""
        if limit:
            sql += f" LIMIT {int(limit)}"
        companies = fetchall(conn, sql)

    # Apply exclusions
    if exclude_companies and companies:
        exclude_lower = {e.lower() for e in exclude_companies}
        companies = [
            c for c in companies
            if c["domain"].lower() not in exclude_lower
            and c["name"].lower() not in exclude_lower
        ]

    if not companies:
        return 0

    account_owner = _detect_account_owner(conn)
    processed = 0

    for i, company in enumerate(companies):
        company_name = company["name"]
        company_domain = company["domain"]
        company_display = f"{company_name} ({company_domain})"
        company_id = company["id"]
        if on_company_progress:
            on_company_progress(i, len(companies), company_display)

        if on_step:
            on_step(f"Gathering emails for {company_display}")
        batches = _get_company_emails_batched(conn, company["domain"])
        if not batches:
            continue

        # Clear existing discussions if force
        if force:
            disc_ids = fetchall(
                conn,
                "SELECT id FROM discussions WHERE company_id = ?",
                (company_id,),
            )
            for d in disc_ids:
                conn.execute("DELETE FROM actions WHERE discussion_id = ?", (d["id"],))
                conn.execute("DELETE FROM discussion_state_history WHERE discussion_id = ?", (d["id"],))
                conn.execute("DELETE FROM discussion_threads WHERE discussion_id = ?", (d["id"],))
            conn.execute("DELETE FROM discussions WHERE company_id = ?", (company_id,))
            conn.commit()

        total_batches = len(batches)
        company_saved = 0

        for batch_idx, (batch_text, batch_thread_ids, batch_addrs) in enumerate(batches):
            if on_batch_progress:
                on_batch_progress(batch_idx, total_batches)

            # Load current discussions (from DB) to pass as context to the AI
            existing = _discussions_to_context(conn, company_id)

            if on_step:
                on_step(f"AI batch {batch_idx + 1}/{total_batches} for {company_display}")

            user_prompt = _build_user_prompt(
                company["name"],
                company["domain"],
                batch_text,
                existing_discussions=existing if existing else None,
                account_owner=account_owner,
                batch_num=batch_idx + 1,
                total_batches=total_batches,
                email_addresses=batch_addrs,
            )

            try:
                result = backend.complete_json(system_prompt, user_prompt)
            except Exception:
                continue

            discussions = result.get("discussions", [])
            if on_step:
                on_step(f"Saving batch {batch_idx + 1}/{total_batches} ({len(discussions)} discussions)")
            saved = _save_discussions(
                conn, company_id, discussions, valid_categories, backend.model_name, force
            )
            conn.commit()
            company_saved += saved

        if on_batch_progress:
            on_batch_progress(total_batches, total_batches)

        processed += (1 if company_saved > 0 else 0)

    if on_company_progress:
        on_company_progress(len(companies), len(companies), "done")

    return processed
