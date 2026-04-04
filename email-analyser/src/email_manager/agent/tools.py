from __future__ import annotations

import json
import sqlite3

from email_manager.db import fetchall, fetchone


TOOL_DEFINITIONS = [
    {
        "name": "query_emails",
        "description": "Search emails by keyword, sender, date range, or project. Returns matching emails.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search term for subject or body"},
                "from_address": {"type": "string", "description": "Filter by sender email address (partial match)"},
                "project": {"type": "string", "description": "Filter by project name"},
                "date_from": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "description": "End date (YYYY-MM-DD)"},
                "limit": {"type": "integer", "description": "Max results (default 20)"},
            },
        },
    },
    {
        "name": "list_projects",
        "description": "List all projects with email counts. Use this to see what projects exist.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 50)"},
            },
        },
    },
    {
        "name": "merge_projects",
        "description": "Merge multiple projects into one. Reassigns all emails from source projects to the target.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_names": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Names of projects to merge FROM",
                },
                "target_name": {"type": "string", "description": "Name of the project to merge INTO (will be created if needed)"},
            },
            "required": ["source_names", "target_name"],
        },
    },
    {
        "name": "rename_project",
        "description": "Rename a project.",
        "input_schema": {
            "type": "object",
            "properties": {
                "old_name": {"type": "string", "description": "Current project name"},
                "new_name": {"type": "string", "description": "New project name"},
            },
            "required": ["old_name", "new_name"],
        },
    },
    {
        "name": "delete_project",
        "description": "Delete a project and unassign all its emails.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Project name to delete"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "contact_summary",
        "description": "Get detailed info about a contact: email count, projects, recent emails.",
        "input_schema": {
            "type": "object",
            "properties": {
                "email_address": {"type": "string", "description": "Contact's email address (partial match supported)"},
            },
            "required": ["email_address"],
        },
    },
    {
        "name": "thread_summary",
        "description": "Get summary and emails for a specific thread by subject keyword.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search term for thread subject"},
                "limit": {"type": "integer", "description": "Max threads to return (default 5)"},
            },
            "required": ["keyword"],
        },
    },
    {
        "name": "run_sql",
        "description": "Execute a read-only SQL query against the email database. Tables: emails, contacts, threads, projects, email_projects, companies, company_contacts, pipeline_runs. Only SELECT queries allowed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL SELECT query"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "set_project_details",
        "description": "Update a project's description, department, or workstream.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Project name"},
                "description": {"type": "string", "description": "Project description"},
                "department": {"type": "string", "description": "Department this project belongs to"},
                "workstream": {"type": "string", "description": "Workstream within the department"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "get_stats",
        "description": "Get overall statistics: email count, project count, contact count, pipeline progress.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "contact_memory",
        "description": "Get the AI-generated memory profile for a contact: relationship type, discussion summaries with status, and key facts. Much richer than contact_summary.",
        "input_schema": {
            "type": "object",
            "properties": {
                "email_address": {"type": "string", "description": "Contact's email address (partial match supported)"},
            },
            "required": ["email_address"],
        },
    },
]


def execute_tool(conn: sqlite3.Connection, tool_name: str, args: dict) -> str:
    try:
        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return f"Unknown tool: {tool_name}"
        return handler(conn, args)
    except Exception as e:
        return f"Error: {e}"


def _query_emails(conn: sqlite3.Connection, args: dict) -> str:
    conditions = []
    params = []

    if keyword := args.get("keyword"):
        conditions.append("(e.subject LIKE ? OR e.body_text LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    if from_addr := args.get("from_address"):
        conditions.append("e.from_address LIKE ?")
        params.append(f"%{from_addr}%")
    if date_from := args.get("date_from"):
        conditions.append("e.date >= ?")
        params.append(date_from)
    if date_to := args.get("date_to"):
        conditions.append("e.date <= ?")
        params.append(date_to + "T23:59:59")
    if project := args.get("project"):
        conditions.append("p.name LIKE ?")
        params.append(f"%{project}%")

    limit = min(args.get("limit", 20), 50)

    where = " AND ".join(conditions) if conditions else "1=1"

    if args.get("project"):
        sql = f"""SELECT e.id, e.date, e.from_address, e.from_name, e.subject, e.folder
                  FROM emails e
                  JOIN email_projects ep ON e.id = ep.email_id
                  JOIN projects p ON ep.project_id = p.id
                  WHERE {where}
                  ORDER BY e.date DESC LIMIT ?"""
    else:
        sql = f"""SELECT e.id, e.date, e.from_address, e.from_name, e.subject, e.folder
                  FROM emails e
                  WHERE {where}
                  ORDER BY e.date DESC LIMIT ?"""

    params.append(limit)
    rows = fetchall(conn, sql, tuple(params))

    if not rows:
        return "No emails found matching the criteria."

    results = []
    for r in rows:
        results.append(
            f"[{r['id']}] {(r['date'] or '')[:10]} | {r['from_name'] or r['from_address']} | {r['subject'] or '(no subject)'}"
        )
    return f"Found {len(rows)} email(s):\n" + "\n".join(results)


def _list_projects(conn: sqlite3.Connection, args: dict) -> str:
    limit = min(args.get("limit", 50), 100)
    rows = fetchall(
        conn,
        """SELECT p.name, p.description, p.department, p.workstream,
                  COUNT(ep.email_id) as email_count
           FROM projects p
           LEFT JOIN email_projects ep ON p.id = ep.project_id
           GROUP BY p.id
           ORDER BY email_count DESC LIMIT ?""",
        (limit,),
    )
    if not rows:
        return "No projects found."

    lines = []
    for r in rows:
        dept = f" [{r['department']}]" if r['department'] else ""
        desc = f" — {r['description']}" if r['description'] else ""
        lines.append(f"- {r['name']}{dept} ({r['email_count']} emails){desc}")
    return f"{len(rows)} project(s):\n" + "\n".join(lines)


def _merge_projects(conn: sqlite3.Connection, args: dict) -> str:
    source_names = args["source_names"]
    target_name = args["target_name"]

    # Get or create target
    target = fetchone(conn, "SELECT id FROM projects WHERE name = ?", (target_name,))
    if not target:
        from datetime import datetime, timezone
        conn.execute(
            "INSERT INTO projects (name, created_at, is_auto) VALUES (?, ?, 0)",
            (target_name, datetime.now(timezone.utc).isoformat()),
        )
        target = fetchone(conn, "SELECT id FROM projects WHERE name = ?", (target_name,))

    target_id = target["id"]
    merged_count = 0

    for source_name in source_names:
        source = fetchone(conn, "SELECT id FROM projects WHERE name = ?", (source_name,))
        if not source:
            continue
        source_id = source["id"]
        if source_id == target_id:
            continue

        # Move email assignments
        conn.execute(
            """UPDATE OR IGNORE email_projects SET project_id = ? WHERE project_id = ?""",
            (target_id, source_id),
        )
        # Delete orphaned assignments (duplicates that couldn't be moved)
        conn.execute("DELETE FROM email_projects WHERE project_id = ?", (source_id,))
        conn.execute("DELETE FROM projects WHERE id = ?", (source_id,))
        merged_count += 1

    conn.commit()
    return f"Merged {merged_count} project(s) into '{target_name}'."


def _rename_project(conn: sqlite3.Connection, args: dict) -> str:
    old_name = args["old_name"]
    new_name = args["new_name"]
    result = conn.execute("UPDATE projects SET name = ? WHERE name = ?", (new_name, old_name))
    conn.commit()
    if result.rowcount == 0:
        return f"Project '{old_name}' not found."
    return f"Renamed '{old_name}' to '{new_name}'."


def _delete_project(conn: sqlite3.Connection, args: dict) -> str:
    name = args["name"]
    proj = fetchone(conn, "SELECT id FROM projects WHERE name = ?", (name,))
    if not proj:
        return f"Project '{name}' not found."
    conn.execute("DELETE FROM email_projects WHERE project_id = ?", (proj["id"],))
    conn.execute("DELETE FROM projects WHERE id = ?", (proj["id"],))
    conn.commit()
    return f"Deleted project '{name}' and unassigned all its emails."


def _contact_summary(conn: sqlite3.Connection, args: dict) -> str:
    email_addr = args["email_address"]
    contact = fetchone(conn, "SELECT * FROM contacts WHERE email LIKE ?", (f"%{email_addr}%",))
    if not contact:
        return f"No contact found matching '{email_addr}'."

    lines = [
        f"Name: {contact['name'] or '—'}",
        f"Email: {contact['email']}",
        f"Company: {contact['company'] or '—'}",
        f"Total emails: {contact['email_count']} (sent: {contact['sent_count']}, received: {contact['received_count']})",
        f"First seen: {(contact['first_seen'] or '')[:10]}",
        f"Last seen: {(contact['last_seen'] or '')[:10]}",
    ]

    # Recent emails
    emails = fetchall(
        conn,
        """SELECT date, subject, from_address FROM emails
           WHERE from_address = ? OR to_addresses LIKE ?
           ORDER BY date DESC LIMIT 5""",
        (contact["email"], f'%"{contact["email"]}"%'),
    )
    if emails:
        lines.append("\nRecent emails:")
        for e in emails:
            lines.append(f"  {(e['date'] or '')[:10]} | {e['from_address']} | {e['subject'] or '(no subject)'}")

    # Projects
    projs = fetchall(
        conn,
        """SELECT DISTINCT p.name, COUNT(ep.email_id) as cnt
           FROM projects p
           JOIN email_projects ep ON p.id = ep.project_id
           JOIN emails e ON ep.email_id = e.id
           WHERE e.from_address = ? OR e.to_addresses LIKE ?
           GROUP BY p.name ORDER BY cnt DESC LIMIT 10""",
        (contact["email"], f'%"{contact["email"]}"%'),
    )
    if projs:
        lines.append("\nProjects:")
        for p in projs:
            lines.append(f"  {p['name']} ({p['cnt']} emails)")

    return "\n".join(lines)


def _thread_summary(conn: sqlite3.Connection, args: dict) -> str:
    keyword = args["keyword"]
    limit = min(args.get("limit", 5), 20)

    threads = fetchall(
        conn,
        """SELECT thread_id, subject, email_count, first_date, last_date, summary, participants
           FROM threads WHERE subject LIKE ? ORDER BY last_date DESC LIMIT ?""",
        (f"%{keyword}%", limit),
    )
    if not threads:
        return f"No threads found matching '{keyword}'."

    lines = []
    for t in threads:
        lines.append(f"Subject: {t['subject']}")
        lines.append(f"  Messages: {t['email_count']} | {(t['first_date'] or '')[:10]} to {(t['last_date'] or '')[:10]}")
        if t["summary"]:
            lines.append(f"  Summary: {t['summary']}")
        try:
            parts = json.loads(t["participants"] or "[]")
            lines.append(f"  Participants: {', '.join(parts[:5])}")
        except (json.JSONDecodeError, TypeError):
            pass
        lines.append("")

    return "\n".join(lines)


def _run_sql(conn: sqlite3.Connection, args: dict) -> str:
    query = args["query"].strip()
    if not query.upper().startswith("SELECT"):
        return "Only SELECT queries are allowed."

    # Block dangerous patterns
    lower = query.lower()
    for forbidden in ["drop", "delete", "update", "insert", "alter", "create", ";--"]:
        if forbidden in lower:
            return f"Query contains forbidden keyword: {forbidden}"

    rows = fetchall(conn, query)
    if not rows:
        return "No results."

    # Format as a simple table
    keys = rows[0].keys()
    lines = [" | ".join(keys)]
    lines.append("-" * len(lines[0]))
    for r in rows[:50]:
        lines.append(" | ".join(str(r[k])[:40] for k in keys))

    if len(rows) > 50:
        lines.append(f"... ({len(rows)} total rows, showing first 50)")

    return "\n".join(lines)


def _set_project_details(conn: sqlite3.Connection, args: dict) -> str:
    name = args["name"]
    proj = fetchone(conn, "SELECT id FROM projects WHERE name = ?", (name,))
    if not proj:
        return f"Project '{name}' not found."

    updates = []
    params = []
    for field in ["description", "department", "workstream"]:
        if field in args and args[field] is not None:
            updates.append(f"{field} = ?")
            params.append(args[field])

    if not updates:
        return "No fields to update."

    params.append(proj["id"])
    conn.execute(f"UPDATE projects SET {', '.join(updates)} WHERE id = ?", tuple(params))
    conn.commit()
    return f"Updated project '{name}'."


def _get_stats(conn: sqlite3.Connection, args: dict) -> str:
    emails = fetchone(conn, "SELECT COUNT(*) as cnt FROM emails")
    projects = fetchone(conn, "SELECT COUNT(*) as cnt FROM projects")
    contacts = fetchone(conn, "SELECT COUNT(*) as cnt FROM contacts")
    threads = fetchone(conn, "SELECT COUNT(*) as cnt FROM threads")
    companies = fetchone(conn, "SELECT COUNT(*) as cnt FROM companies")

    pipeline = fetchall(
        conn,
        "SELECT stage, status, COUNT(*) as cnt FROM pipeline_runs GROUP BY stage, status",
    )
    pipeline_str = ""
    if pipeline:
        pipeline_str = "\nPipeline:\n" + "\n".join(
            f"  {p['stage']}: {p['status']} = {p['cnt']}" for p in pipeline
        )

    return (
        f"Emails: {emails['cnt']}\n"
        f"Threads: {threads['cnt']}\n"
        f"Projects: {projects['cnt']}\n"
        f"Contacts: {contacts['cnt']}\n"
        f"Companies: {companies['cnt']}"
        f"{pipeline_str}"
    )


TOOL_HANDLERS = {
    "query_emails": _query_emails,
    "list_projects": _list_projects,
    "merge_projects": _merge_projects,
    "rename_project": _rename_project,
    "delete_project": _delete_project,
    "contact_summary": _contact_summary,
    "thread_summary": _thread_summary,
    "run_sql": _run_sql,
    "set_project_details": _set_project_details,
    "get_stats": _get_stats,
    "contact_memory": _contact_memory,
}


def _contact_memory(conn: sqlite3.Connection, args: dict) -> str:
    email_addr = args["email_address"]

    # Try loading from SQLite
    from email_manager.memory.sqlite_backend import SQLiteMemoryBackend
    backend = SQLiteMemoryBackend(conn)

    # Partial match
    mem = backend.load(email_addr)
    if not mem:
        row = fetchone(conn, "SELECT email FROM contact_memories WHERE email LIKE ?", (f"%{email_addr}%",))
        if row:
            mem = backend.load(row["email"])

    if not mem:
        return f"No memory found for '{email_addr}'. Generate one with: email-manager memory {email_addr}"

    lines = [
        f"Name: {mem.name or '—'}",
        f"Email: {mem.email}",
        f"Relationship: {mem.relationship}",
        f"",
        f"Summary: {mem.summary}",
    ]

    if mem.discussions:
        lines.append(f"\nDiscussions ({len(mem.discussions)}):")
        for d in mem.discussions:
            lines.append(f"  [{d.get('status', '?')}] {d.get('topic', '?')}: {d.get('summary', '')}")

    if mem.key_facts:
        lines.append(f"\nKey Facts:")
        for fact in mem.key_facts:
            lines.append(f"  - {fact}")

    lines.append(f"\nGenerated: {mem.generated_at[:10] if mem.generated_at else '?'} | Strategy: {mem.strategy_used} | Model: {mem.model_used}")

    return "\n".join(lines)
