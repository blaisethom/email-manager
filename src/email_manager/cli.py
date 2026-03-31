from __future__ import annotations

import json

import click
from rich.console import Console
from rich.table import Table

from email_manager.config import Config
from email_manager.db import get_db, fetchall, fetchone


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Email Manager — personal email data pipeline."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = Config()


@cli.command()
@click.option("--account", "-a", default=None, help="Sync only this account (by name)")
@click.option("--folders", "-f", multiple=True, help="Override folders to sync (IMAP only)")
@click.option("--list-folders", is_flag=True, help="List available folders on IMAP accounts and exit")
@click.pass_context
def sync(ctx: click.Context, account: str | None, folders: tuple[str, ...], list_folders: bool) -> None:
    """Fetch new emails from all configured accounts."""
    from email_manager.ingestion.threading import compute_threads

    config: Config = ctx.obj["config"]
    console = Console()
    conn = get_db(config)

    accounts = config.get_accounts()
    if account:
        accounts = [a for a in accounts if a.name == account]
        if not accounts:
            console.print(f"[red]No account named '{account}'. Available: {', '.join(a.name for a in config.get_accounts())}[/red]")
            conn.close()
            raise SystemExit(1)

    if list_folders:
        for acct in accounts:
            label = acct.name or acct.backend
            if acct.backend == "gmail":
                console.print(f"\n[bold]{label}[/bold] (Gmail — uses labels, not folders)")
            else:
                from email_manager.ingestion.imap_client import _connect_with_retry, _list_folders
                console.print(f"\n[bold]{label}[/bold] ({acct.imap_host})")
                client = _connect_with_retry(acct)
                try:
                    folder_list = _list_folders(client)
                    for f in folder_list:
                        console.print(f"  {f}")
                    console.print(f"  [dim]({len(folder_list)} folders)[/dim]")
                finally:
                    try:
                        client.logout()
                    except Exception:
                        pass
        conn.close()
        return

    total_new = 0
    try:
        for acct in accounts:
            if folders:
                acct.imap_folders = list(folders)

            label = acct.name or acct.backend
            console.print(f"\n[bold]Syncing account: {label}[/bold]")

            if acct.backend == "gmail":
                from email_manager.ingestion.gmail_client import sync_emails as gmail_sync
                console.print(f"  Syncing via Gmail API...")
                new_count = gmail_sync(conn, acct)
            else:
                from email_manager.ingestion.imap_client import sync_emails as imap_sync
                if not acct.imap_host:
                    console.print(f"  [red]Error: IMAP_HOST not configured for account '{label}'[/red]")
                    continue
                from email_manager.ingestion.imap_client import _is_yahoo
                host = "export.imap.mail.yahoo.com" if _is_yahoo(acct.imap_host) else acct.imap_host
                console.print(f"  Connecting to {host}...")
                new_count = imap_sync(conn, acct)

            console.print(f"  [green]Fetched {new_count} new email(s)[/green]")
            total_new += new_count

        if total_new > 0:
            console.print("\nComputing threads...")
            updated = compute_threads(conn)
            console.print(f"[green]Updated {updated} thread assignment(s)[/green]")
        else:
            console.print(f"\n[green]Total: {total_new} new email(s) across {len(accounts)} account(s)[/green]")
    finally:
        conn.close()


@cli.command()
@click.pass_context
def accounts(ctx: click.Context) -> None:
    """List configured email accounts."""
    config: Config = ctx.obj["config"]
    console = Console()
    accts = config.get_accounts()

    if not accts:
        console.print("[dim]No accounts configured. See accounts.json.example[/dim]")
        return

    table = Table(title="Email Accounts")
    table.add_column("Name", width=20)
    table.add_column("Backend", width=10)
    table.add_column("Details", width=40)

    for acct in accts:
        if acct.backend == "gmail":
            details = f"credentials: {acct.gmail_credentials_path}"
        else:
            details = f"{acct.imap_user}@{acct.imap_host}:{acct.imap_port}"
        table.add_row(acct.name or "(unnamed)", acct.backend, details)

    console.print(table)


@cli.command(name="list")
@click.option("--limit", "-n", default=20, help="Number of emails to show")
@click.option("--folder", "-f", default=None, help="Filter by folder")
@click.pass_context
def list_emails(ctx: click.Context, limit: int, folder: str | None) -> None:
    """Show recent emails."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    sql = "SELECT id, date, from_address, from_name, subject, folder FROM emails"
    params: list = []
    if folder:
        sql += " WHERE folder = ?"
        params.append(folder)
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)

    rows = fetchall(conn, sql, tuple(params))
    conn.close()

    console = Console()
    if not rows:
        console.print("[dim]No emails found.[/dim]")
        return

    table = Table(title="Recent Emails")
    table.add_column("ID", style="dim", width=6)
    table.add_column("Date", width=12)
    table.add_column("From", width=30)
    table.add_column("Subject", width=50)
    table.add_column("Folder", style="dim", width=12)

    for row in rows:
        from_display = row["from_name"] or row["from_address"]
        date_short = row["date"][:10] if row["date"] else ""
        subject = (row["subject"] or "")[:50]
        table.add_row(
            str(row["id"]),
            date_short,
            from_display[:30],
            subject,
            row["folder"] or "",
        )

    console.print(table)


@cli.command()
@click.argument("query")
@click.option("--limit", "-n", default=20)
@click.pass_context
def search(ctx: click.Context, query: str, limit: int) -> None:
    """Search emails by subject or body text."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    rows = fetchall(
        conn,
        """SELECT id, date, from_address, from_name, subject, folder
        FROM emails
        WHERE subject LIKE ? OR body_text LIKE ?
        ORDER BY date DESC LIMIT ?""",
        (f"%{query}%", f"%{query}%", limit),
    )
    conn.close()

    console = Console()
    if not rows:
        console.print(f"[dim]No emails matching '{query}'[/dim]")
        return

    table = Table(title=f"Search: {query}")
    table.add_column("ID", style="dim", width=6)
    table.add_column("Date", width=12)
    table.add_column("From", width=30)
    table.add_column("Subject", width=50)

    for row in rows:
        from_display = row["from_name"] or row["from_address"]
        date_short = row["date"][:10] if row["date"] else ""
        table.add_row(
            str(row["id"]),
            date_short,
            from_display[:30],
            (row["subject"] or "")[:50],
        )

    console.print(table)


@cli.command()
@click.option("--stage", "-s", type=click.Choice(["extract_base", "contact_memory", "extract_entities", "categorise", "summarise_threads"]), multiple=True, help="Run specific stage(s) only")
@click.option("--limit", "-n", default=None, type=int, help="Only process the N most recent unprocessed emails/threads")
@click.pass_context
def analyse(ctx: click.Context, stage: tuple[str, ...], limit: int | None) -> None:
    """Run AI analysis pipeline on synced emails."""
    from email_manager.pipeline.runner import run_pipeline

    config: Config = ctx.obj["config"]
    console = Console()
    stages = list(stage) if stage else None
    run_pipeline(config, stages=stages, console=console, limit=limit)


@cli.command()
@click.pass_context
def run(ctx: click.Context) -> None:
    """Full pipeline: sync + analyse."""
    ctx.invoke(sync)
    ctx.invoke(analyse)


@cli.command()
@click.option("--limit", "-n", default=50)
@click.pass_context
def projects(ctx: click.Context, limit: int) -> None:
    """List discovered projects."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    rows = fetchall(
        conn,
        """SELECT p.id, p.name, p.department, p.workstream,
                  COUNT(ep.email_id) as email_count,
                  MIN(e.date) as first_date,
                  MAX(e.date) as last_date
           FROM projects p
           LEFT JOIN email_projects ep ON p.id = ep.project_id
           LEFT JOIN emails e ON ep.email_id = e.id
           GROUP BY p.id
           ORDER BY email_count DESC
           LIMIT ?""",
        (limit,),
    )
    conn.close()

    console = Console()
    if not rows:
        console.print("[dim]No projects found. Run 'email-manager analyse' first.[/dim]")
        return

    table = Table(title="Projects")
    table.add_column("ID", style="dim", width=5)
    table.add_column("Name", width=40)
    table.add_column("Emails", width=8, justify="right")
    table.add_column("First", width=12)
    table.add_column("Last", width=12)
    table.add_column("Department", width=20)

    for row in rows:
        table.add_row(
            str(row["id"]),
            row["name"][:40],
            str(row["email_count"]),
            (row["first_date"] or "")[:10],
            (row["last_date"] or "")[:10],
            row["department"] or "",
        )

    console.print(table)


@cli.command()
@click.option("--type", "-t", "entity_type", default=None, type=click.Choice(["person", "company", "topic", "action_item"]), help="Filter by entity type")
@click.option("--limit", "-n", default=30)
@click.pass_context
def entities(ctx: click.Context, entity_type: str | None, limit: int) -> None:
    """List extracted entities."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    if entity_type:
        rows = fetchall(
            conn,
            """SELECT ent.entity_type, ent.value, ent.context, ent.confidence,
                      e.subject, e.date
               FROM entities ent
               JOIN emails e ON ent.email_id = e.id
               WHERE ent.entity_type = ?
               ORDER BY ent.confidence DESC
               LIMIT ?""",
            (entity_type, limit),
        )
    else:
        rows = fetchall(
            conn,
            """SELECT ent.entity_type, ent.value, ent.context, ent.confidence,
                      e.subject, e.date
               FROM entities ent
               JOIN emails e ON ent.email_id = e.id
               ORDER BY ent.confidence DESC
               LIMIT ?""",
            (limit,),
        )

    conn.close()

    console = Console()
    if not rows:
        console.print("[dim]No entities found. Run 'email-manager analyse --stage extract_entities' first.[/dim]")
        return

    # Also show a summary count by type
    config2 = Config()
    conn2 = get_db(config2)
    counts = fetchall(conn2, "SELECT entity_type, COUNT(*) as cnt FROM entities GROUP BY entity_type ORDER BY cnt DESC")
    conn2.close()
    if counts:
        console.print("[bold]Entity counts:[/bold]")
        for c in counts:
            console.print(f"  {c['entity_type']}: {c['cnt']}")
        console.print()

    table = Table(title=f"Entities{f' ({entity_type})' if entity_type else ''}")
    table.add_column("Type", width=12)
    table.add_column("Value", width=25)
    table.add_column("Confidence", width=6, justify="right")
    table.add_column("Email Subject", width=35)
    table.add_column("Context", width=40)

    for row in rows:
        table.add_row(
            row["entity_type"],
            row["value"][:25],
            f"{row['confidence']:.0%}" if row["confidence"] else "—",
            (row["subject"] or "")[:35],
            (row["context"] or "")[:40],
        )

    console.print(table)


@cli.command()
@click.argument("email_address", required=False)
@click.option("--limit", "-n", default=30)
@click.pass_context
def coemail(ctx: click.Context, email_address: str | None, limit: int) -> None:
    """Show co-emailing stats. Optionally filter by one address to see who they co-email with most."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    if email_address:
        rows = fetchall(
            conn,
            """SELECT email_a, email_b, co_email_count, first_co_email, last_co_email
               FROM co_email_stats
               WHERE email_a = ? OR email_b = ?
               ORDER BY co_email_count DESC LIMIT ?""",
            (email_address.lower(), email_address.lower(), limit),
        )
    else:
        rows = fetchall(
            conn,
            """SELECT email_a, email_b, co_email_count, first_co_email, last_co_email
               FROM co_email_stats
               ORDER BY co_email_count DESC LIMIT ?""",
            (limit,),
        )

    conn.close()

    console = Console()
    if not rows:
        console.print("[dim]No co-email stats found. Run 'email-manager analyse --stage extract_base' first.[/dim]")
        return

    total = fetchone(get_db(config), "SELECT COUNT(*) as cnt FROM co_email_stats")
    console.print(f"[bold]{total['cnt']}[/bold] co-email pairs total\n")

    table = Table(title=f"Co-email stats{f' for {email_address}' if email_address else ' (top pairs)'}")
    table.add_column("Person A", width=30)
    table.add_column("Person B", width=30)
    table.add_column("Count", width=8, justify="right")
    table.add_column("First", width=12)
    table.add_column("Last", width=12)

    for row in rows:
        table.add_row(
            row["email_a"][:30],
            row["email_b"][:30],
            str(row["co_email_count"]),
            (row["first_co_email"] or "")[:10],
            (row["last_co_email"] or "")[:10],
        )

    console.print(table)


@cli.command()
@click.option("--limit", "-n", default=30)
@click.pass_context
def threads(ctx: click.Context, limit: int) -> None:
    """List threads with summaries."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    rows = fetchall(
        conn,
        """SELECT thread_id, subject, email_count, first_date, last_date, summary
           FROM threads
           ORDER BY last_date DESC
           LIMIT ?""",
        (limit,),
    )
    conn.close()

    console = Console()
    if not rows:
        console.print("[dim]No threads found.[/dim]")
        return

    table = Table(title="Threads")
    table.add_column("Subject", width=40)
    table.add_column("Msgs", width=5, justify="right")
    table.add_column("Last", width=12)
    table.add_column("Summary", width=60)

    for row in rows:
        table.add_row(
            (row["subject"] or "")[:40],
            str(row["email_count"]),
            (row["last_date"] or "")[:10],
            (row["summary"] or "[dim]—[/dim]")[:60],
        )

    console.print(table)


@cli.command()
@click.option("--limit", "-n", default=30)
@click.pass_context
def contacts(ctx: click.Context, limit: int) -> None:
    """List contacts ranked by frequency."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    rows = fetchall(
        conn,
        """SELECT email, name, company, email_count, sent_count, received_count,
                  first_seen, last_seen
           FROM contacts
           ORDER BY email_count DESC
           LIMIT ?""",
        (limit,),
    )
    conn.close()

    console = Console()
    if not rows:
        console.print("[dim]No contacts found. Run 'email-manager analyse --stage build_crm' first.[/dim]")
        return

    table = Table(title="Contacts")
    table.add_column("Name", width=25)
    table.add_column("Email", width=30)
    table.add_column("Company", width=15)
    table.add_column("Total", width=6, justify="right")
    table.add_column("Sent", width=6, justify="right")
    table.add_column("Recv", width=6, justify="right")
    table.add_column("Last Seen", width=12)

    for row in rows:
        table.add_row(
            (row["name"] or "")[:25],
            row["email"][:30],
            (row["company"] or "")[:15],
            str(row["email_count"]),
            str(row["sent_count"]),
            str(row["received_count"]),
            (row["last_seen"] or "")[:10],
        )

    console.print(table)


@cli.command()
@click.argument("email_address")
@click.pass_context
def contact(ctx: click.Context, email_address: str) -> None:
    """Show detail for a specific contact."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    row = fetchone(conn, "SELECT * FROM contacts WHERE email = ?", (email_address,))
    if not row:
        console.print(f"[dim]No contact found for {email_address}[/dim]")
        conn.close()
        return

    console.print(f"\n[bold]{row['name'] or email_address}[/bold]")
    console.print(f"  Email:    {row['email']}")
    console.print(f"  Company:  {row['company'] or '—'}")
    console.print(f"  Total:    {row['email_count']} emails ({row['sent_count']} sent, {row['received_count']} received)")
    console.print(f"  First:    {(row['first_seen'] or '')[:10]}")
    console.print(f"  Last:     {(row['last_seen'] or '')[:10]}")

    # Show recent emails involving this contact
    emails = fetchall(
        conn,
        """SELECT date, subject, from_address FROM emails
           WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?
           ORDER BY date DESC LIMIT 10""",
        (email_address, f'%"{email_address}"%', f'%"{email_address}"%'),
    )

    if emails:
        console.print(f"\n[bold]Recent emails:[/bold]")
        table = Table()
        table.add_column("Date", width=12)
        table.add_column("From", width=25)
        table.add_column("Subject", width=50)
        for e in emails:
            table.add_row(
                (e["date"] or "")[:10],
                e["from_address"][:25],
                (e["subject"] or "")[:50],
            )
        console.print(table)

    # Show projects this contact is involved in
    projs = fetchall(
        conn,
        """SELECT DISTINCT p.name, COUNT(ep.email_id) as cnt
           FROM projects p
           JOIN email_projects ep ON p.id = ep.project_id
           JOIN emails e ON ep.email_id = e.id
           WHERE e.from_address = ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?
           GROUP BY p.name ORDER BY cnt DESC LIMIT 10""",
        (email_address, f'%"{email_address}"%', f'%"{email_address}"%'),
    )

    if projs:
        console.print(f"\n[bold]Projects:[/bold]")
        for p in projs:
            console.print(f"  {p['name']} ({p['cnt']} emails)")

    conn.close()


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show sync status and pipeline progress."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    # Email counts
    total = fetchone(conn, "SELECT COUNT(*) as cnt FROM emails")
    console.print(f"Total emails: [bold]{total['cnt']}[/bold]")

    # Per-folder sync state
    states = fetchall(conn, "SELECT folder, last_uid, last_sync FROM sync_state")
    if states:
        table = Table(title="Sync State")
        table.add_column("Folder")
        table.add_column("Last UID")
        table.add_column("Last Sync")
        for s in states:
            table.add_row(s["folder"], str(s["last_uid"]), s["last_sync"][:19])
        console.print(table)

    # Thread count
    threads = fetchone(conn, "SELECT COUNT(*) as cnt FROM threads")
    console.print(f"Threads: [bold]{threads['cnt']}[/bold]")

    # Project count
    projects = fetchone(conn, "SELECT COUNT(*) as cnt FROM projects")
    console.print(f"Projects: [bold]{projects['cnt']}[/bold]")

    # Pipeline progress
    pipeline = fetchall(
        conn,
        """SELECT stage, status, COUNT(*) as cnt
        FROM pipeline_runs GROUP BY stage, status ORDER BY stage""",
    )
    if pipeline:
        table = Table(title="Pipeline Progress")
        table.add_column("Stage")
        table.add_column("Status")
        table.add_column("Count")
        for p in pipeline:
            table.add_row(p["stage"], p["status"], str(p["cnt"]))
        console.print(table)

    conn.close()


@cli.command()
@click.argument("email_address", required=False)
@click.option("--all", "process_all", is_flag=True, help="Process all contacts")
@click.option("--force", is_flag=True, help="Regenerate even if up to date")
@click.option("--limit", "-n", default=None, type=int, help="Process top N contacts by email count")
@click.option("--strategy", type=click.Choice(["default", "detailed"]), default=None, help="Override memory strategy")
@click.pass_context
def memory(ctx: click.Context, email_address: str | None, process_all: bool, force: bool, limit: int | None, strategy: str | None) -> None:
    """View or generate contact memory profiles."""
    from email_manager.ai.factory import get_backend
    from email_manager.analysis.contact_memory import build_contact_memories
    from email_manager.memory.factory import get_memory_backends, get_memory_strategy

    config: Config = ctx.obj["config"]
    if strategy:
        config.memory_strategy = strategy

    conn = get_db(config)
    console = Console()
    memory_backends = get_memory_backends(config, conn)

    if email_address:
        # Show existing memory, or generate if missing
        existing = memory_backends[0].load(email_address)

        if existing and not force:
            _display_memory(console, existing)
        else:
            backend = get_backend(config)
            strat = get_memory_strategy(config)
            console.print(f"Generating memory for {email_address} using [bold]{strat.name}[/bold] strategy...")
            count = build_contact_memories(
                conn, backend, memory_backends, strat,
                email_address=email_address, console=console, force=force,
            )
            if count > 0:
                mem = memory_backends[0].load(email_address)
                if mem:
                    _display_memory(console, mem)
            else:
                console.print("[dim]No memory generated (contact may not exist or no emails found).[/dim]")

    elif process_all or limit:
        backend = get_backend(config)
        strat = get_memory_strategy(config)
        console.print(f"Using strategy: [bold]{strat.name}[/bold] | AI: [bold]{backend.model_name}[/bold]")
        count = build_contact_memories(
            conn, backend, memory_backends, strat,
            console=console, limit=limit, force=force,
        )
        console.print(f"\n[green]Generated {count} contact memories[/green]")

    else:
        # List all existing memories
        all_memories = memory_backends[0].load_all()
        if not all_memories:
            console.print("[dim]No memories yet. Run 'email-manager memory --all --limit 10' to generate.[/dim]")
            conn.close()
            return

        table = Table(title=f"Contact Memories ({len(all_memories)})")
        table.add_column("Name", width=25)
        table.add_column("Email", width=30)
        table.add_column("Relationship", width=12)
        table.add_column("Discussions", width=6, justify="right")
        table.add_column("Strategy", width=10)
        table.add_column("Generated", width=12)

        for mem in all_memories:
            table.add_row(
                (mem.name or "")[:25],
                mem.email[:30],
                mem.relationship,
                str(len(mem.discussions)),
                mem.strategy_used,
                mem.generated_at[:10] if mem.generated_at else "",
            )
        console.print(table)

    conn.close()


def _display_memory(console: Console, mem) -> None:
    from rich.panel import Panel

    # Header
    rel_colors = {
        "colleague": "blue", "manager": "magenta", "report": "cyan",
        "vendor": "yellow", "client": "green", "friend": "bright_green",
        "recruiter": "red", "service": "dim", "newsletter": "dim",
    }
    rel_color = rel_colors.get(mem.relationship, "white")

    console.print(Panel(
        f"[bold]{mem.name or mem.email}[/bold]  [{rel_color}]{mem.relationship}[/{rel_color}]\n"
        f"[dim]{mem.email}[/dim]\n\n"
        f"{mem.summary}",
        title="Contact Memory",
        subtitle=f"v{mem.version} | {mem.strategy_used} | {mem.model_used} | {mem.generated_at[:10] if mem.generated_at else ''}",
    ))

    # Discussions
    if mem.discussions:
        table = Table(title="Discussions")
        table.add_column("Status", width=10)
        table.add_column("Topic", width=30)
        table.add_column("Summary", width=60)

        status_icons = {"active": "[green]active[/green]", "waiting": "[yellow]waiting[/yellow]", "resolved": "[dim]resolved[/dim]"}
        for d in mem.discussions:
            status = d.get("status", "unknown")
            table.add_row(
                status_icons.get(status, status),
                d.get("topic", "")[:30],
                d.get("summary", "")[:60],
            )
        console.print(table)

    # Key facts
    if mem.key_facts:
        console.print("\n[bold]Key Facts:[/bold]")
        for fact in mem.key_facts:
            console.print(f"  - {fact}")


@cli.command()
@click.pass_context
def chat(ctx: click.Context) -> None:
    """Interactive agent — ask questions about your emails, manage projects."""
    from email_manager.ai.factory import get_backend
    from email_manager.agent.repl import run_repl

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    try:
        backend = get_backend(config)
        run_repl(conn, backend, console)
    finally:
        conn.close()


if __name__ == "__main__":
    cli()
