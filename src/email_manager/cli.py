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
@click.option("--folders", "-f", multiple=True, help="Override folders to sync (IMAP only)")
@click.option("--backend", "-b", type=click.Choice(["imap", "gmail"]), default=None, help="Override email backend")
@click.pass_context
def sync(ctx: click.Context, folders: tuple[str, ...], backend: str | None) -> None:
    """Fetch new emails from IMAP or Gmail."""
    from email_manager.ingestion.threading import compute_threads

    config: Config = ctx.obj["config"]
    if backend:
        config.email_backend = backend
    if folders:
        config.imap_folders = list(folders)

    console = Console()
    conn = get_db(config)

    try:
        if config.email_backend == "gmail":
            from email_manager.ingestion.gmail_client import sync_emails as gmail_sync
            console.print("Syncing via Gmail API...")
            new_count = gmail_sync(conn, config)
        else:
            from email_manager.ingestion.imap_client import sync_emails as imap_sync
            if not config.imap_host:
                console.print("[red]Error: IMAP_HOST not configured. See .env.example[/red]")
                raise SystemExit(1)
            console.print(f"Connecting to {config.imap_host}...")
            new_count = imap_sync(conn, config)

        console.print(f"[green]Fetched {new_count} new email(s)[/green]")

        if new_count > 0:
            console.print("Computing threads...")
            updated = compute_threads(conn)
            console.print(f"[green]Updated {updated} thread assignment(s)[/green]")
    finally:
        conn.close()


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
@click.option("--stage", "-s", type=click.Choice(["categorise", "extract_entities", "summarise_threads", "build_crm"]), multiple=True, help="Run specific stage(s) only")
@click.pass_context
def analyse(ctx: click.Context, stage: tuple[str, ...]) -> None:
    """Run AI analysis pipeline on synced emails."""
    from email_manager.pipeline.runner import run_pipeline

    config: Config = ctx.obj["config"]
    console = Console()
    stages = list(stage) if stage else None
    run_pipeline(config, stages=stages, console=console)


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
