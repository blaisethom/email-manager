from __future__ import annotations

import json

import click
from rich.console import Console
from rich.table import Table

from email_manager.config import Config, EmailAccount
from email_manager.db import get_db, fetchall, fetchone


def _gmail_token_email(acct: EmailAccount) -> str:
    """Read the authenticated email from a Gmail token file, if available."""
    try:
        token_data = json.loads(acct.gmail_token_path.read_text())
        return token_data.get("authenticated_email", "[not authenticated]")
    except (FileNotFoundError, json.JSONDecodeError):
        return "[not authenticated]"


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Email Manager — personal email data pipeline."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = Config()


@cli.command()
@click.option("--account", "-a", default=None, help="Authenticate only this account (by name)")
@click.option("--remote", is_flag=True, help="Headless mode — prints OAuth URL instead of opening a browser")
@click.option("--calendar", is_flag=True, help="Also authorize Google Calendar access")
@click.pass_context
def auth(ctx: click.Context, account: str | None, remote: bool, calendar: bool) -> None:
    """Authenticate Gmail accounts (useful on remote/headless machines)."""
    from email_manager.ingestion.gmail_client import authenticate

    config: Config = ctx.obj["config"]
    console = Console()

    accounts = config.get_accounts()
    gmail_accounts = [a for a in accounts if a.backend == "gmail"]
    if account:
        gmail_accounts = [a for a in gmail_accounts if a.name == account]
        if not gmail_accounts:
            console.print(f"[red]No Gmail account named '{account}'.[/red]")
            raise SystemExit(1)

    if not gmail_accounts:
        console.print("[red]No Gmail accounts configured.[/red]")
        raise SystemExit(1)

    for acct in gmail_accounts:
        label = acct.name or "gmail"
        console.print(f"\n[bold]Authenticating: {label}[/bold]")
        if calendar:
            from email_manager.ingestion.calendar_client import _get_calendar_service
            console.print("  Requesting Gmail + Calendar access...")
            _get_calendar_service(acct, remote=remote)
            console.print(f"[green]Token saved for {label} (Gmail + Calendar)[/green]")
        else:
            email_addr = authenticate(acct, remote=remote)
            console.print(f"[green]Token saved for {label} ({email_addr})[/green]")


@cli.command()
@click.option("--account", "-a", default=None, help="Sync only this account (by name)")
@click.option("--folders", "-f", multiple=True, help="Override folders to sync (IMAP only)")
@click.option("--list-folders", is_flag=True, help="List available folders on IMAP accounts and exit")
@click.option("--remote", is_flag=True, help="Headless mode for Gmail OAuth — prints URL instead of opening a browser")
@click.option("--rebuild-threads", is_flag=True, help="Force a full thread rebuild instead of incremental")
@click.option("--no-calendar", is_flag=True, help="Skip calendar sync for Gmail accounts")
@click.option("--calendar-months", type=int, default=6, help="How many months back to sync calendar events (default: 6)")
@click.pass_context
def sync(ctx: click.Context, account: str | None, folders: tuple[str, ...], list_folders: bool, remote: bool, rebuild_threads: bool, no_calendar: bool, calendar_months: int) -> None:
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
                new_count = gmail_sync(conn, acct, remote=remote)
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

        unthreaded = conn.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE thread_id IS NULL"
        ).fetchone()["cnt"]

        if rebuild_threads or total_new > 0 or unthreaded > 0:
            console.print(f"\nComputing threads ({unthreaded} unthreaded emails)...")
            updated = compute_threads(conn, console=console, force_rebuild=rebuild_threads)
            console.print(f"[green]Updated {updated} thread assignment(s)[/green]")
        else:
            console.print(f"\n[green]Total: {total_new} new email(s) across {len(accounts)} account(s)[/green]")

        # Sync calendar events for Gmail accounts
        if not no_calendar:
            gmail_accounts = [a for a in accounts if a.backend == "gmail"]
            if gmail_accounts:
                from email_manager.ingestion.calendar_client import sync_calendar_events, needs_calendar_auth
                for acct in gmail_accounts:
                    label = acct.name or "gmail"
                    if needs_calendar_auth(acct):
                        console.print(f"\n[yellow]Calendar ({label}): needs authorization. Run 'auth --account {acct.name} --calendar' first to grant calendar access.[/yellow]")
                        continue
                    console.print(f"\n[bold]Syncing calendar: {label}[/bold]")
                    try:
                        cal_count = sync_calendar_events(conn, acct, console=console, remote=remote, months_back=calendar_months)
                        console.print(f"  [green]{cal_count} calendar event(s) synced[/green]")
                    except Exception as e:
                        console.print(f"  [yellow]Calendar sync failed: {e}[/yellow]")
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
    table.add_column("Email", width=30)
    table.add_column("Details", width=40)

    for acct in accts:
        if acct.backend == "gmail":
            email_addr = _gmail_token_email(acct)
            details = f"credentials: {acct.gmail_credentials_path}"
        else:
            email_addr = acct.imap_user or ""
            details = f"{acct.imap_host}:{acct.imap_port}"
        table.add_row(acct.name or "(unnamed)", acct.backend, email_addr, details)

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
@click.option("--stage", "-s", type=click.Choice(["sync_calendar", "extract_base", "fetch_homepages", "label_companies", "extract_events", "discover_discussions", "analyse_discussions", "contact_memory", "categorise", "summarise_threads", "discussions", "link_calendar"]), multiple=True, help="Run specific stage(s) only")
@click.option("--limit", "-n", default=None, type=int, help="Only process the N most recent unprocessed emails/threads")
@click.option("--force", "-f", is_flag=True, help="Force regeneration even if already processed")
@click.option("--company", "-c", default=None, help="Scope to a specific company (domain or name)")
@click.option("--label", "-l", default=None, help="Scope to all companies with this label (e.g. customer, vendor)")
@click.option("--exclude", "-x", multiple=True, help="Exclude company by domain or name (repeatable)")
@click.option("--contact", default=None, help="Scope to a specific contact's company (email address)")
@click.pass_context
def analyse(ctx: click.Context, stage: tuple[str, ...], limit: int | None, force: bool, company: str | None, label: str | None, exclude: tuple[str, ...], contact: str | None) -> None:
    """Run AI analysis pipeline on synced emails."""
    from email_manager.pipeline.runner import run_pipeline

    config: Config = ctx.obj["config"]
    console = Console()
    stages = list(stage) if stage else None
    run_pipeline(config, stages=stages, console=console, limit=limit, force=force, company=company, label=label, exclude=list(exclude) if exclude else None, contact=contact)


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
@click.option("--label", "-l", default=None, help="Filter by label (e.g. customer, vendor, partner)")
@click.option("--unlabelled", is_flag=True, help="Show only companies without labels")
@click.pass_context
def companies(ctx: click.Context, limit: int, label: str | None, unlabelled: bool) -> None:
    """List companies you interact with and their associated email addresses."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    if label:
        rows = fetchall(
            conn,
            """SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen
               FROM companies c
               JOIN company_labels cl ON c.id = cl.company_id
               WHERE cl.label = ?
               ORDER BY cl.confidence DESC, c.email_count DESC
               LIMIT ?""",
            (label, limit),
        )
    elif unlabelled:
        rows = fetchall(
            conn,
            """SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen
               FROM companies c
               LEFT JOIN company_labels cl ON c.id = cl.company_id
               WHERE cl.company_id IS NULL
               ORDER BY c.email_count DESC
               LIMIT ?""",
            (limit,),
        )
    else:
        rows = fetchall(
            conn,
            """SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen
               FROM companies c
               ORDER BY c.email_count DESC
               LIMIT ?""",
            (limit,),
        )

    console = Console()
    if not rows:
        console.print("[dim]No companies found. Run 'email-manager analyse --stage extract_base' first.[/dim]")
        conn.close()
        return

    table = Table(title="Companies")
    table.add_column("Company", width=20)
    table.add_column("Domain", width=25)
    table.add_column("Emails", width=8, justify="right")
    table.add_column("Labels", width=25)
    table.add_column("Contacts", width=35)
    table.add_column("First Seen", width=12)
    table.add_column("Last Seen", width=12)

    for row in rows:
        contacts = fetchall(
            conn,
            "SELECT contact_email FROM company_contacts WHERE company_id = ? ORDER BY contact_email",
            (row["id"],),
        )
        contact_list = ", ".join(c["contact_email"] for c in contacts[:5])
        if len(contacts) > 5:
            contact_list += f" (+{len(contacts) - 5} more)"

        labels = fetchall(
            conn,
            "SELECT label, confidence FROM company_labels WHERE company_id = ? ORDER BY confidence DESC",
            (row["id"],),
        )
        label_str = ", ".join(f'{l["label"]} ({l["confidence"]:.0%})' for l in labels) if labels else ""

        table.add_row(
            row["name"],
            row["domain"],
            str(row["email_count"]),
            label_str,
            contact_list,
            (row["first_seen"] or "")[:10],
            (row["last_seen"] or "")[:10],
        )

    console.print(table)

    # Summary stats (only on unfiltered view)
    if not label and not unlabelled:
        total = fetchone(conn, "SELECT COUNT(*) as cnt FROM companies")["cnt"]
        labelled_count = fetchone(conn, "SELECT COUNT(DISTINCT company_id) as cnt FROM company_labels")["cnt"]
        unlabelled_count = total - labelled_count
        label_counts = fetchall(
            conn,
            "SELECT label, COUNT(*) as cnt FROM company_labels GROUP BY label ORDER BY cnt DESC",
        )

        console.print()
        console.print(f"[bold]Total:[/bold] {total} companies — {labelled_count} labelled, {unlabelled_count} unlabelled")
        if label_counts:
            parts = [f"{r['label']} ({r['cnt']})" for r in label_counts]
            console.print(f"[dim]Labels: {', '.join(parts)}[/dim]")

    conn.close()


@cli.command()
@click.argument("identifier")
@click.pass_context
def company(ctx: click.Context, identifier: str) -> None:
    """Show detailed information about a company (by domain or name)."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    # Look up by domain first, then by name (case-insensitive)
    row = fetchone(
        conn,
        "SELECT * FROM companies WHERE domain = ? COLLATE NOCASE",
        (identifier,),
    )
    if not row:
        row = fetchone(
            conn,
            "SELECT * FROM companies WHERE name LIKE ? COLLATE NOCASE",
            (f"%{identifier}%",),
        )
    if not row:
        console.print(f"[red]No company found matching '{identifier}'[/red]")
        conn.close()
        return

    company_id = row["id"]

    # Header
    console.print(f"\n[bold]{row['name']}[/bold]  ({row['domain']})")
    if row["description"]:
        console.print(f"[dim]{row['description']}[/dim]")

    # Labels
    labels = fetchall(
        conn,
        "SELECT label, confidence, reasoning FROM company_labels WHERE company_id = ? ORDER BY confidence DESC",
        (company_id,),
    )
    if labels:
        console.print(f"\n[bold]Labels:[/bold]")
        for l in labels:
            conf = f" ({l['confidence']:.0%})" if l["confidence"] else ""
            reason = f" — {l['reasoning']}" if l["reasoning"] else ""
            console.print(f"  {l['label']}{conf}{reason}")

    # Email statistics
    like_pattern = f"%@{row['domain']}%"
    total_emails = fetchone(
        conn,
        """SELECT COUNT(*) as cnt FROM emails
           WHERE from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?""",
        (like_pattern, like_pattern, like_pattern),
    )["cnt"]
    received = fetchone(
        conn,
        "SELECT COUNT(*) as cnt FROM emails WHERE from_address LIKE ?",
        (like_pattern,),
    )["cnt"]
    sent = total_emails - received
    first_email = fetchone(
        conn,
        """SELECT MIN(date) as d FROM emails
           WHERE from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?""",
        (like_pattern, like_pattern, like_pattern),
    )
    last_email = fetchone(
        conn,
        """SELECT MAX(date) as d FROM emails
           WHERE from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?""",
        (like_pattern, like_pattern, like_pattern),
    )

    console.print(f"\n[bold]Email Statistics:[/bold]")
    console.print(f"  Total emails: {total_emails}  (received: {received}, sent: {sent})")
    console.print(f"  First email:  {(first_email['d'] or '')[:10]}")
    console.print(f"  Last email:   {(last_email['d'] or '')[:10]}")

    # Top 5 contacts
    contacts = fetchall(
        conn,
        """SELECT cc.contact_email, c.name, c.email_count, c.sent_count, c.received_count
           FROM company_contacts cc
           JOIN contacts c ON cc.contact_email = c.email
           WHERE cc.company_id = ?
           ORDER BY c.email_count DESC
           LIMIT 5""",
        (company_id,),
    )
    if contacts:
        console.print(f"\n[bold]Top Contacts:[/bold]")
        table = Table(show_header=True, box=None, pad_edge=False, padding=(0, 2))
        table.add_column("Email", width=35)
        table.add_column("Name", width=25)
        table.add_column("Emails", width=8, justify="right")
        table.add_column("Sent", width=6, justify="right")
        table.add_column("Received", width=10, justify="right")
        for c in contacts:
            table.add_row(
                c["contact_email"],
                c["name"] or "",
                str(c["email_count"]),
                str(c["sent_count"]),
                str(c["received_count"]),
            )
        console.print(table)

    console.print()
    conn.close()


@cli.command()
@click.option("--limit", "-n", default=30)
@click.option("--label", "-l", default=None, help="Filter by label name")
@click.pass_context
def labels(ctx: click.Context, limit: int, label: str | None) -> None:
    """Show company relationship labels."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    if label:
        rows = fetchall(
            conn,
            """SELECT c.name, c.domain, c.email_count, cl.label, cl.confidence, cl.reasoning
               FROM company_labels cl
               JOIN companies c ON cl.company_id = c.id
               WHERE cl.label = ?
               ORDER BY cl.confidence DESC
               LIMIT ?""",
            (label, limit),
        )
    else:
        rows = fetchall(
            conn,
            """SELECT c.name, c.domain, c.email_count, cl.label, cl.confidence, cl.reasoning
               FROM company_labels cl
               JOIN companies c ON cl.company_id = c.id
               ORDER BY c.email_count DESC, cl.confidence DESC
               LIMIT ?""",
            (limit,),
        )

    conn.close()

    if not rows:
        console.print("[dim]No labels found. Run 'email-manager analyse --stage label_companies' first.[/dim]")
        return

    table = Table(title=f"Company Labels{f' ({label})' if label else ''}")
    table.add_column("Company", width=20)
    table.add_column("Domain", width=25)
    table.add_column("Emails", width=8, justify="right")
    table.add_column("Label", width=18)
    table.add_column("Conf", width=6, justify="right")
    table.add_column("Reasoning", width=50)

    for row in rows:
        table.add_row(
            row["name"],
            row["domain"],
            str(row["email_count"]),
            row["label"],
            f"{row['confidence']:.0%}" if row["confidence"] else "",
            (row["reasoning"] or "")[:50],
        )

    console.print(table)


@cli.command()
@click.option("--limit", "-n", default=50)
@click.option("--company", "-c", default=None, help="Filter by company domain or name")
@click.option("--contact", default=None, help="Filter by contact email")
@click.option("--category", default=None, help="Filter by discussion category")
@click.option("--state", default=None, help="Filter by current state")
@click.pass_context
def discussions(ctx: click.Context, limit: int, company: str | None, contact: str | None, category: str | None, state: str | None) -> None:
    """List extracted discussions and their current status."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    conditions = []
    params: list = []

    if company:
        # Resolve to company_id
        row = fetchone(conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (company,))
        if not row:
            row = fetchone(conn, "SELECT id FROM companies WHERE name LIKE ? COLLATE NOCASE", (f"%{company}%",))
        if not row:
            console.print(f"[red]No company found matching '{company}'[/red]")
            conn.close()
            return
        conditions.append("d.company_id = ?")
        params.append(row["id"])

    if contact:
        contact_row = fetchone(
            conn,
            """SELECT c.id FROM companies c
               JOIN company_contacts cc ON c.id = cc.company_id
               WHERE cc.contact_email = ?""",
            (contact,),
        )
        if not contact_row:
            console.print(f"[red]No company found for contact '{contact}'[/red]")
            conn.close()
            return
        conditions.append("d.company_id = ?")
        params.append(contact_row["id"])

    if category:
        conditions.append("d.category = ?")
        params.append(category)

    if state:
        conditions.append("d.current_state = ?")
        params.append(state)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = fetchall(
        conn,
        f"""SELECT d.id, d.title, d.category, d.current_state, d.summary,
                   d.first_seen, d.last_seen, c.name as company_name, c.domain
            FROM discussions d
            JOIN companies c ON d.company_id = c.id
            {where}
            ORDER BY d.last_seen DESC
            LIMIT ?""",
        tuple(params),
    )

    if not rows:
        console.print("[dim]No discussions found. Run 'email-manager analyse --stage discussions' first.[/dim]")
        conn.close()
        return

    table = Table(title="Discussions")
    table.add_column("ID", width=5, justify="right")
    table.add_column("Company", width=30)
    table.add_column("Category", width=18)
    table.add_column("State", width=16)
    table.add_column("Title", width=35)
    table.add_column("Start", width=10)
    table.add_column("End", width=10)

    for row in rows:
        company_display = f"{row['company_name']} ({row['domain']})"
        table.add_row(
            str(row["id"]),
            company_display[:30],
            row["category"],
            row["current_state"] or "",
            row["title"][:35],
            (row["first_seen"] or "")[:10],
            (row["last_seen"] or "")[:10],
        )

    console.print(table)

    # Summary
    total = fetchone(conn, "SELECT COUNT(*) as cnt FROM discussions")["cnt"]
    cat_counts = fetchall(conn, "SELECT category, COUNT(*) as cnt FROM discussions GROUP BY category ORDER BY cnt DESC")
    console.print()
    console.print(f"[bold]Total:[/bold] {total} discussions")
    if cat_counts:
        parts = [f"{r['category']} ({r['cnt']})" for r in cat_counts]
        console.print(f"[dim]Categories: {', '.join(parts)}[/dim]")

    conn.close()


@cli.command()
@click.argument("discussion_id", type=int)
@click.pass_context
def discussion(ctx: click.Context, discussion_id: int) -> None:
    """Show detailed information about a specific discussion."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    row = fetchone(
        conn,
        """SELECT d.*, c.name as company_name, c.domain
           FROM discussions d
           JOIN companies c ON d.company_id = c.id
           WHERE d.id = ?""",
        (discussion_id,),
    )
    if not row:
        console.print(f"[red]Discussion {discussion_id} not found[/red]")
        conn.close()
        return

    # Header
    console.print(f"\n[bold]{row['title']}[/bold]")
    console.print(f"Company: {row['company_name']} ({row['domain']})")
    console.print(f"Category: [bold]{row['category']}[/bold]  State: [bold]{row['current_state']}[/bold]")
    if row["summary"]:
        console.print(f"\n{row['summary']}")

    # Participants
    participants = json.loads(row["participants"]) if row["participants"] else []
    if participants:
        console.print(f"\n[bold]Participants:[/bold]")
        for p in participants:
            console.print(f"  {p}")

    # State history timeline
    history = fetchall(
        conn,
        """SELECT state, entered_at, reasoning
           FROM discussion_state_history
           WHERE discussion_id = ?
           ORDER BY entered_at ASC, id ASC""",
        (discussion_id,),
    )
    if history:
        console.print(f"\n[bold]State Timeline:[/bold]")
        for h in history:
            date = (h["entered_at"] or "unknown")[:10]
            reasoning = f" — {h['reasoning']}" if h["reasoning"] else ""
            console.print(f"  {date}  {h['state']}{reasoning}")

    # Linked threads
    threads = fetchall(
        conn,
        """SELECT dt.thread_id, t.subject, t.email_count, t.first_date, t.last_date
           FROM discussion_threads dt
           LEFT JOIN threads t ON dt.thread_id = t.thread_id
           WHERE dt.discussion_id = ?
           ORDER BY t.last_date DESC""",
        (discussion_id,),
    )
    if threads:
        console.print(f"\n[bold]Linked Threads:[/bold]")
        for t in threads:
            subject = (t["subject"] or "(no subject)")[:50]
            dates = f"{(t['first_date'] or '')[:10]} — {(t['last_date'] or '')[:10]}" if t["first_date"] else ""
            count = f"({t['email_count']} emails)" if t["email_count"] else ""
            console.print(f"  {subject}  {count}  {dates}")

    # Actions
    actions = fetchall(
        conn,
        """SELECT description, assignee_emails, target_date, status, source_date, completed_date
           FROM actions
           WHERE discussion_id = ?
           ORDER BY source_date ASC, id ASC""",
        (discussion_id,),
    )
    if actions:
        console.print(f"\n[bold]Actions:[/bold]")
        for a in actions:
            status_icon = "[green]done[/green]" if a["status"] == "done" else "[yellow]open[/yellow]"
            assignees = json.loads(a["assignee_emails"]) if a["assignee_emails"] else []
            assignee_str = ", ".join(assignees) if assignees else "unassigned"
            target = f" due {a['target_date']}" if a["target_date"] else ""
            completed = f" completed {a['completed_date'][:10]}" if a.get("completed_date") else ""
            source = f" (from {a['source_date'][:10]})" if a["source_date"] else ""
            console.print(f"  [{status_icon}] {a['description']}")
            console.print(f"         assignees: {assignee_str}{target}{completed}{source}")

    console.print()
    conn.close()


@cli.command()
@click.option("--limit", "-n", default=50)
@click.option("--company", "-c", default=None, help="Filter by company domain or name")
@click.option("--assignee", "-a", default=None, help="Filter by assignee email address")
@click.option("--status", "-s", default=None, type=click.Choice(["open", "done"]), help="Filter by action status")
@click.option("--discussion", "-d", "discussion_id", default=None, type=int, help="Filter by discussion ID")
@click.pass_context
def actions(ctx: click.Context, limit: int, company: str | None, assignee: str | None, status: str | None, discussion_id: int | None) -> None:
    """List extracted actions from discussions."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    conditions = []
    params: list = []

    if company:
        row = fetchone(conn, "SELECT id FROM companies WHERE domain = ? COLLATE NOCASE", (company,))
        if not row:
            row = fetchone(conn, "SELECT id FROM companies WHERE name LIKE ? COLLATE NOCASE", (f"%{company}%",))
        if not row:
            console.print(f"[red]No company found matching '{company}'[/red]")
            conn.close()
            return
        conditions.append("d.company_id = ?")
        params.append(row["id"])

    if assignee:
        conditions.append("a.assignee_emails LIKE ?")
        params.append(f"%{assignee.lower()}%")

    if status:
        conditions.append("a.status = ?")
        params.append(status)

    if discussion_id:
        conditions.append("a.discussion_id = ?")
        params.append(discussion_id)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = fetchall(
        conn,
        f"""SELECT a.id, a.description, a.assignee_emails, a.target_date, a.status,
                   a.source_date, d.title as discussion_title, d.id as discussion_id,
                   c.name as company_name, c.domain
            FROM actions a
            JOIN discussions d ON a.discussion_id = d.id
            JOIN companies c ON d.company_id = c.id
            {where}
            ORDER BY a.status ASC, a.target_date ASC NULLS LAST, a.source_date DESC
            LIMIT ?""",
        tuple(params),
    )

    if not rows:
        console.print("[dim]No actions found. Run 'email-manager analyse --stage discussions' first.[/dim]")
        conn.close()
        return

    table = Table(title="Actions")
    table.add_column("ID", width=5, justify="right")
    table.add_column("Status", width=6)
    table.add_column("Description", width=40)
    table.add_column("Assignees", width=30)
    table.add_column("Due", width=10)
    table.add_column("Discussion", width=30)
    table.add_column("Company", width=20)

    for row in rows:
        status_str = "[green]done[/green]" if row["status"] == "done" else "[yellow]open[/yellow]"
        assignees = json.loads(row["assignee_emails"]) if row["assignee_emails"] else []
        assignees_str = ", ".join(assignees) if assignees else ""
        table.add_row(
            str(row["id"]),
            status_str,
            row["description"][:40],
            assignees_str[:30],
            (row["target_date"] or "")[:10],
            f'#{row["discussion_id"]} {row["discussion_title"][:22]}',
            f'{row["company_name"][:20]}',
        )

    console.print(table)

    # Summary
    total = fetchone(conn, "SELECT COUNT(*) as cnt FROM actions")["cnt"]
    open_count = fetchone(conn, "SELECT COUNT(*) as cnt FROM actions WHERE status = 'open'")["cnt"]
    done_count = fetchone(conn, "SELECT COUNT(*) as cnt FROM actions WHERE status = 'done'")["cnt"]
    console.print()
    console.print(f"[bold]Total:[/bold] {total} actions ({open_count} open, {done_count} done)")

    conn.close()


@cli.command(name="discussion-stats")
@click.option("--category", default=None, help="Filter by discussion category")
@click.pass_context
def discussion_stats(ctx: click.Context, category: str | None) -> None:
    """Show discussion funnel statistics and state transition analysis."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    # Load category config for state ordering
    from email_manager.analysis.discussions import load_category_config
    categories = load_category_config(getattr(config, "discussion_categories_path", None))
    state_order = {c["name"]: c["states"] for c in categories}

    cats_to_show = [category] if category else [c["name"] for c in categories]

    for cat_name in cats_to_show:
        states = state_order.get(cat_name)
        if not states:
            continue

        # Count discussions per current state
        state_counts = fetchall(
            conn,
            """SELECT current_state, COUNT(*) as cnt
               FROM discussions
               WHERE category = ?
               GROUP BY current_state""",
            (cat_name,),
        )
        count_map = {r["current_state"]: r["cnt"] for r in state_counts}
        total = sum(count_map.values())

        if total == 0:
            continue

        console.print(f"\n[bold]{cat_name}[/bold] ({total} discussions)")

        # Funnel view — show states in order
        table = Table(show_header=True, box=None, pad_edge=False, padding=(0, 2))
        table.add_column("State", width=25)
        table.add_column("Current", width=10, justify="right")
        table.add_column("Ever reached", width=12, justify="right")
        table.add_column("Avg days in state", width=18, justify="right")

        for s in states:
            current = count_map.get(s, 0)

            # Count how many discussions ever reached this state
            ever_reached = fetchone(
                conn,
                """SELECT COUNT(DISTINCT discussion_id) as cnt
                   FROM discussion_state_history
                   WHERE state = ? AND discussion_id IN (
                       SELECT id FROM discussions WHERE category = ?
                   )""",
                (s, cat_name),
            )
            ever = ever_reached["cnt"] if ever_reached else 0

            # Average time spent in this state (for discussions that moved past it)
            avg_days_str = ""
            avg_row = fetchone(
                conn,
                """SELECT AVG(julianday(next_date) - julianday(h.entered_at)) as avg_d
                   FROM discussion_state_history h
                   JOIN (
                       SELECT discussion_id, MIN(entered_at) as next_date
                       FROM discussion_state_history
                       WHERE entered_at > (
                           SELECT entered_at FROM discussion_state_history h2
                           WHERE h2.discussion_id = discussion_state_history.discussion_id
                             AND h2.state = ?
                           LIMIT 1
                       )
                       AND discussion_id IN (SELECT id FROM discussions WHERE category = ?)
                       GROUP BY discussion_id
                   ) nxt ON h.discussion_id = nxt.discussion_id
                   WHERE h.state = ?
                     AND h.discussion_id IN (SELECT id FROM discussions WHERE category = ?)""",
                (s, cat_name, s, cat_name),
            )
            if avg_row and avg_row["avg_d"] is not None:
                avg_days_str = f"{avg_row['avg_d']:.1f}"

            bar = "#" * current + "." * (total - current)
            table.add_row(s, str(current), str(ever), avg_days_str)

        console.print(table)

        # Conversion rates between consecutive states
        console.print(f"\n  [dim]Conversion rates:[/dim]")
        for i in range(len(states) - 1):
            from_s, to_s = states[i], states[i + 1]
            from_count = fetchone(
                conn,
                """SELECT COUNT(DISTINCT discussion_id) as cnt
                   FROM discussion_state_history
                   WHERE state = ? AND discussion_id IN (SELECT id FROM discussions WHERE category = ?)""",
                (from_s, cat_name),
            )
            to_count = fetchone(
                conn,
                """SELECT COUNT(DISTINCT discussion_id) as cnt
                   FROM discussion_state_history
                   WHERE state = ? AND discussion_id IN (SELECT id FROM discussions WHERE category = ?)""",
                (to_s, cat_name),
            )
            from_n = from_count["cnt"] if from_count else 0
            to_n = to_count["cnt"] if to_count else 0
            rate = f"{to_n / from_n:.0%}" if from_n > 0 else "—"
            console.print(f"    {from_s} → {to_s}: {rate} ({to_n}/{from_n})")

    conn.close()




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


@cli.command(name="delete-contact")
@click.argument("email_address")
@click.option("--remote", is_flag=True, help="Headless mode for Gmail OAuth")
@click.pass_context
def delete_contact(ctx: click.Context, email_address: str, remote: bool) -> None:
    """Delete all emails from/to a contact on the remote server(s) and local database."""
    from collections import defaultdict
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    email_address = email_address.lower()
    like_pattern = f'%"{email_address}"%'

    # Gather stats from actual emails, not the (possibly stale) contacts table
    contact = fetchone(conn, "SELECT * FROM contacts WHERE email = ?", (email_address,))
    stats = fetchone(
        conn,
        """SELECT
               SUM(CASE WHEN from_address = ? THEN 1 ELSE 0 END) as sent_by,
               SUM(CASE WHEN from_address != ? THEN 1 ELSE 0 END) as sent_to,
               COUNT(*) as total,
               MIN(date) as first_seen,
               MAX(date) as last_seen
           FROM emails
           WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?""",
        (email_address, email_address, email_address, like_pattern, like_pattern),
    )
    sent = stats["sent_by"] or 0
    received = stats["sent_to"] or 0
    total = stats["total"] or 0

    if total == 0:
        console.print(f"[dim]No emails found involving {email_address}.[/dim]")
        conn.close()
        return

    # Display stats
    name = contact["name"] if contact and contact["name"] else email_address
    console.print(f"\n[bold]Contact:[/bold] {name}")
    console.print(f"  Emails sent by them:     [bold]{sent}[/bold]")
    console.print(f"  Emails sent to them:     [bold]{received}[/bold]")
    console.print(f"  Total to delete:         [bold red]{total}[/bold red]")
    console.print(f"  First seen: {(stats['first_seen'] or '')[:10]}")
    console.print(f"  Last seen:  {(stats['last_seen'] or '')[:10]}")

    # Confirm
    if not click.confirm(f"\nDelete all {total} emails involving {email_address}?", default=False):
        console.print("[dim]Aborted.[/dim]")
        conn.close()
        return

    # Collect emails to delete, grouped by account
    rows = fetchall(
        conn,
        "SELECT id, message_id, gmail_id, folder, account_name FROM emails "
        "WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?",
        (email_address, like_pattern, like_pattern),
    )

    # Build account lookup
    accounts_by_name = {a.name: a for a in config.get_accounts()}

    # Group rows by account_name
    by_account: dict[str, list] = defaultdict(list)
    no_account: list = []
    for r in rows:
        if r["account_name"] and r["account_name"] in accounts_by_name:
            by_account[r["account_name"]].append(r)
        else:
            no_account.append(r)

    if no_account:
        console.print(
            f"[yellow]{len(no_account)} email(s) have no account_name set "
            f"(synced before this feature). Re-sync to backfill, or these "
            f"will only be deleted locally.[/yellow]"
        )

    deleted_count = 0
    failed_count = 0

    def _delete_local_batch(ids: list[int]) -> None:
        """Delete a batch of emails from local DB and commit immediately."""
        CHUNK = 500
        for i in range(0, len(ids), CHUNK):
            chunk = ids[i : i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            conn.execute(f"DELETE FROM email_projects WHERE email_id IN ({placeholders})", chunk)
            conn.execute(f"DELETE FROM email_references WHERE email_id IN ({placeholders})", chunk)
            conn.execute(f"DELETE FROM pipeline_runs WHERE email_id IN ({placeholders})", chunk)
            conn.execute(f"DELETE FROM emails WHERE id IN ({placeholders})", chunk)
        conn.commit()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Deleting from remote", total=len(rows) - len(no_account))

        for acct_name, acct_rows in by_account.items():
            acct = accounts_by_name[acct_name]

            if acct.backend == "gmail":
                from email_manager.ingestion.gmail_client import trash_messages

                gmail_rows = [r for r in acct_rows if r["gmail_id"]]
                gmail_ids = [r["gmail_id"] for r in gmail_rows]
                id_by_gmail = {r["gmail_id"]: r["id"] for r in gmail_rows}

                BATCH = 50
                for i in range(0, len(gmail_ids), BATCH):
                    batch = gmail_ids[i : i + BATCH]
                    succeeded, failed = trash_messages(acct, batch, remote=remote)
                    batch_ids = [id_by_gmail[gid] for gid in succeeded]
                    if batch_ids:
                        _delete_local_batch(batch_ids)
                        deleted_count += len(batch_ids)
                    failed_count += len(failed)
                    progress.advance(task, len(batch))
            else:
                from email_manager.ingestion.imap_client import delete_messages

                by_folder: dict[str, list[str]] = defaultdict(list)
                mid_to_id: dict[str, int] = {}
                for r in acct_rows:
                    folder = r["folder"] or "INBOX"
                    by_folder[folder].append(r["message_id"])
                    mid_to_id[r["message_id"]] = r["id"]

                succeeded, failed = delete_messages(acct, dict(by_folder))
                batch_ids = [mid_to_id[mid] for mid in succeeded]
                if batch_ids:
                    _delete_local_batch(batch_ids)
                    deleted_count += len(batch_ids)
                failed_count += len(failed)
                progress.advance(task, len(acct_rows))

    # Emails with no account — delete locally only
    no_account_ids = [r["id"] for r in no_account]
    if no_account_ids:
        _delete_local_batch(no_account_ids)
        deleted_count += len(no_account_ids)

    # Clean up contact-level data if all emails were deleted
    remaining = fetchone(
        conn,
        "SELECT COUNT(*) as cnt FROM emails WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?",
        (email_address, like_pattern, like_pattern),
    )["cnt"]

    if remaining == 0:
        conn.execute("DELETE FROM contacts WHERE email = ?", (email_address,))
        conn.execute("DELETE FROM contact_memories WHERE email = ?", (email_address,))
        conn.execute(
            "DELETE FROM co_email_stats WHERE email_a = ? OR email_b = ?",
            (email_address, email_address),
        )

    conn.commit()
    conn.close()

    console.print(f"\n[green]Deleted {deleted_count} email(s) from remote and local database.[/green]")
    if failed_count:
        console.print(f"[yellow]{failed_count} email(s) failed to delete remotely and were kept locally.[/yellow]")
    if remaining > 0 and failed_count == 0:
        console.print(f"[dim]{remaining} email(s) remain locally (failed remote deletion).[/dim]")


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
