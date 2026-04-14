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


def _read_company_file(path: str) -> list[str]:
    """Read company domains/names from a file (one per line, or CSV with 'domain' column).

    Handles CSV files with headers (even if there are leading non-CSV lines like
    migration output). Also handles plain text files with one domain per line.
    """
    from pathlib import Path as _Path
    text = _Path(path).read_text()
    lines = text.splitlines()
    if not lines:
        return []

    # Detect CSV: scan first 10 lines for a header containing "domain"
    for i, line in enumerate(lines[:10]):
        stripped = line.strip()
        if "," in stripped and "domain" in stripped.lower():
            import csv
            import io
            # Re-parse from the header line onward
            csv_text = "\n".join(lines[i:])
            reader = csv.DictReader(io.StringIO(csv_text))
            domain_col = None
            for col in (reader.fieldnames or []):
                if col.strip().lower() == "domain":
                    domain_col = col
                    break
            if domain_col:
                return [row[domain_col].strip() for row in reader
                        if row.get(domain_col, "").strip()]

    # Plain text: one entry per line, first whitespace-delimited token
    entries = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("["):
            continue
        entries.append(line.split()[0])
    return entries


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
@click.option("--stage", "-s", type=click.Choice(["extract_base", "fetch_homepages", "label_companies", "extract_events", "discover_discussions", "analyse_discussions", "propose_actions", "contact_memory"]), multiple=True, help="Run specific stage(s) only")
@click.option("--limit", "-n", default=None, type=int, help="Only process the N most recent unprocessed emails/threads")
@click.option("--force", "-f", is_flag=True, help="Force regeneration even if already processed")
@click.option("--clean", is_flag=True, help="Delete previous output for the scoped stages before reprocessing")
@click.option("--company", "-c", default=None, help="Scope to a specific company (domain or name)")
@click.option("--label", "-l", default=None, help="Scope to all companies with this label (e.g. customer, vendor)")
@click.option("--exclude", "-x", multiple=True, help="Exclude company by domain or name (repeatable)")
@click.option("--exclude-file", default=None, type=click.Path(exists=True), help="File with company domains/names to exclude (one per line)")
@click.option("--company-file", default=None, type=click.Path(exists=True), help="File with company domains/names to process (one per line)")
@click.option("--contact", default=None, help="Scope to a specific contact's company (email address)")
@click.option("--per-company", is_flag=True, help="Run all stages per company before moving to the next (requires a multi-company filter like --label, --company-file, --last-seen-*, etc.)")
@click.option("--stale-before", default=None, help="Only process companies whose last analysis is before this date (YYYY-MM-DD)")
@click.option("--last-seen-after", default=None, help="Only process companies with email activity after this date (YYYY-MM-DD)")
@click.option("--last-seen-before", default=None, help="Only process companies with last email activity before this date (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, help="Show which companies would be processed without running anything")
@click.option("--concurrency", type=int, default=1, show_default=True, help="Max concurrent LLM calls within stages")
@click.option("--new-emails", is_flag=True, help="Only process companies with emails newer than their last analysis")
@click.option("--stale-prompt", is_flag=True, help="Only process companies where the prompt has changed since last run")
@click.option("--stale-model", is_flag=True, help="Only process companies last analysed with a different model")
@click.option("--unprocessed", is_flag=True, help="Only process companies with no prior runs for the requested stages")
@click.pass_context
def analyse(ctx: click.Context, stage: tuple[str, ...], limit: int | None, force: bool, clean: bool, company: str | None, label: str | None, exclude: tuple[str, ...], exclude_file: str | None, company_file: str | None, contact: str | None, per_company: bool, stale_before: str | None, last_seen_after: str | None, last_seen_before: str | None, dry_run: bool, concurrency: int, new_emails: bool, stale_prompt: bool, stale_model: bool, unprocessed: bool) -> None:
    """Run AI analysis pipeline on synced emails.

    Pipeline stages (in order):

    \b
      1. extract_base         Extract contacts, companies, domains (no AI)
      2. fetch_homepages      Download company homepages (no AI)
      3. label_companies      Classify company relationships (AI)
      4. extract_events       Extract business events from threads (AI)
      5. discover_discussions  Cluster events into discussions (AI)
      6. analyse_discussions   Evaluate milestones, state & summary (AI)
      7. propose_actions      Suggest next steps for active discussions (AI)
      8. contact_memory       Generate contact relationship profiles (AI)

    Use --stage/-s to run specific stages. Use --company/-c to scope to one
    company. Use --clean to delete previous output before reprocessing.

    \b
    Staleness filters (combinable, any match includes the company):
      --new-emails     Companies with emails since their last analysis
      --stale-prompt   Companies where the prompt changed (e.g. new learned rules)
      --stale-model    Companies last analysed with a different model
      --unprocessed    Companies with no prior runs for the requested stages

    Example: Rebuild analysis for one company from scratch:
      email-analyser analyse -s extract_events -s discover_discussions \\
        -s analyse_discussions --company acme.com --clean
    """
    from pathlib import Path
    from email_manager.pipeline.runner import run_pipeline


    config: Config = ctx.obj["config"]
    console = Console()
    stages = list(stage) if stage else None

    # Merge file-based company lists with CLI options
    exclude_list = list(exclude) if exclude else []
    if exclude_file:
        exclude_list.extend(_read_company_file(exclude_file))

    companies_from_file = _read_company_file(company_file) if company_file else []

    run_pipeline(config, stages=stages, console=console, limit=limit, force=force, clean=clean, company=company, company_list=companies_from_file or None, label=label, exclude=exclude_list or None, contact=contact, per_company=per_company, stale_before=stale_before, last_seen_after=last_seen_after, last_seen_before=last_seen_before, dry_run=dry_run, concurrency=concurrency, only_new_emails=new_emails, only_stale_prompt=stale_prompt, only_stale_model=stale_model, only_unprocessed=unprocessed)


QUICK_UPDATE_THRESHOLD = 10  # Max new threads before switching to staged pipeline


@cli.command()
@click.option("--company", "-c", default=None, help="Scope to a specific company (domain or name)")
@click.option("--label", "-l", default=None, help="Scope to all companies with this label")
@click.option("--company-file", default=None, type=click.Path(exists=True), help="File with company domains/names to process (one per line)")
@click.option("--threshold", type=int, default=QUICK_UPDATE_THRESHOLD, show_default=True, help="Max new threads for single-call mode; above this uses staged pipeline")
@click.option("--agent", is_flag=True, help="Use agent mode: Claude processes each company in an autonomous session with database tools")
@click.option("--concurrency", type=int, default=1, show_default=True, help="Max concurrent LLM calls for staged pipeline")
@click.pass_context
def update(ctx: click.Context, company: str | None, label: str | None, company_file: str | None, threshold: int, agent: bool, concurrency: int) -> None:
    """Incremental update: process new emails for companies that need it.

    Adapts strategy per company based on volume of changes:
    - Few new threads (<=threshold): single merged LLM call (fast)
    - Many new threads (>threshold): staged pipeline (thorough)

    With --agent, uses an autonomous agent session per company. The agent has
    tools to read emails, check discussions, save events, and update state.
    This is more thorough but uses more tokens.

    With no scoping options, automatically detects companies with new/changed
    emails via the change journal.

    \b
    Example:
      email-analyser update
      email-analyser update --company acme.com
      email-analyser update --agent --company acme.com
      email-analyser update --threshold 5
    """
    from pathlib import Path
    from email_manager.analysis.quick_update import quick_update, count_new_threads_for_company
    from email_manager.analysis.events import load_category_config
    from email_manager.ai.factory import get_backend
    from email_manager.change_journal import get_dirty_company_domains, mark_processed
    from email_manager.pipeline.stages import (
        run_extract_events,
        run_discover_discussions,
        run_analyse_discussions,
        run_propose_actions,
    )

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))
    backend = get_backend(config)
    console.print(f"Using AI backend: [bold]{backend.model_name}[/bold]")


    # Resolve company domains
    auto_scoped = False
    domains: list[str] = []
    if company:
        row = fetchone(conn, "SELECT domain FROM companies WHERE domain = ? COLLATE NOCASE OR name = ? COLLATE NOCASE", (company, company))
        if not row:
            console.print(f"[red]Company not found: {company}[/red]")
            conn.close()
            return
        domains = [row["domain"]]
    elif label:
        rows = fetchall(conn, "SELECT DISTINCT c.domain FROM companies c JOIN company_labels cl ON c.id = cl.company_id WHERE cl.label = ?", (label,))
        domains = [r["domain"] for r in rows]
    elif company_file:
        entries = _read_company_file(company_file)
        lowered = [v.lower() for v in entries]
        placeholders = ", ".join("?" for _ in lowered)
        rows = fetchall(conn, f"SELECT DISTINCT domain FROM companies WHERE LOWER(domain) IN ({placeholders}) OR LOWER(name) IN ({placeholders})", tuple(lowered + lowered))
        domains = [r["domain"] for r in rows]
    else:
        # Auto-scope: find companies with unprocessed changes
        domains = get_dirty_company_domains(conn)
        auto_scoped = True

    if not domains:
        if auto_scoped:
            console.print("[dim]No companies with pending changes. Nothing to update.[/dim]")
        else:
            console.print("[red]No companies to update. Use --company, --label, or --company-file.[/red]")
        conn.close()
        return

    scope_label = "auto-detected" if auto_scoped else "scoped"
    console.print(f"Updating [bold]{len(domains)}[/bold] {scope_label} company{'s' if len(domains) != 1 else ''}")

    total_events = 0
    total_updates = 0
    total_actions = 0
    total_new = 0
    staged_count = 0
    agent_count = 0

    for i, domain in enumerate(domains):
        if agent:
            # Agent mode: autonomous session, propose changes, review, apply
            from email_manager.ai.agent_backend import agent_update_company, apply_changes

            console.print(f"\n[bold cyan]{domain}[/bold cyan] ({i+1}/{len(domains)}) [agent]")
            agent_count += 1

            result = agent_update_company(
                conn, domain, model=backend.model_name,
                auto_apply=False, console=console,
            )

            proposed = result.get("proposed")
            agent_tracker = result.get("token_tracker")

            if agent_tracker and agent_tracker.call_count > 0:
                console.print(
                    f"  [dim]{agent_tracker.total_input}+{agent_tracker.total_output}"
                    f"={agent_tracker.total} tokens, {agent_tracker.call_count} iterations[/dim]"
                )

            if result.get("summary"):
                summary_text = result["summary"][:300]
                if len(result["summary"]) > 300:
                    summary_text += "..."
                console.print(f"  [dim]{summary_text}[/dim]")

            if not proposed or proposed.is_empty:
                console.print("  [dim]No changes proposed[/dim]")
                continue

            console.print(f"\n  [bold]Proposed changes:[/bold]")
            for line in proposed.summary_lines():
                console.print(line)

            # Single company: ask for confirmation. Batch: auto-apply.
            should_apply = True
            if len(domains) == 1:
                should_apply = click.confirm("\n  Apply these changes?", default=True)

            if should_apply:
                company_row = fetchone(conn, "SELECT id, domain FROM companies WHERE domain = ?", (domain,))
                counts = apply_changes(
                    conn, proposed, company_row["id"], company_row["domain"],
                    mode="agent", model=backend.model_name,
                    token_tracker=agent_tracker,
                )
                total_events += counts["events"]
                total_new += counts["new_discussions"]
                total_updates += counts["updates"]
                total_actions += counts["actions"]
                console.print(f"  [green]Applied: {counts['events']} events, {counts['new_discussions']} new discussions, {counts['updates']} updated[/green]")
            else:
                console.print("  [yellow]Skipped[/yellow]")
        else:
            new_threads = count_new_threads_for_company(conn, domain)
            use_staged = new_threads > threshold

            mode = f"staged ({new_threads} threads)" if use_staged else f"quick ({new_threads} threads)"
            console.print(f"\n[bold cyan]{domain}[/bold cyan] ({i+1}/{len(domains)}) [{mode}]")

            if new_threads == 0:
                console.print("  [dim]No new emails to process[/dim]")
                continue

            if use_staged:
                # Staged pipeline: extract → discover → analyse → propose
                staged_count += 1
                ev_count = run_extract_events(conn, backend, config, console=console, company=domain, concurrency=concurrency)
                disc_count = run_discover_discussions(conn, backend, config, console=console, company=domain)
                ana_count = run_analyse_discussions(conn, backend, config, console=console, company=domain, concurrency=concurrency)
                act_count = run_propose_actions(conn, backend, config, console=console, company=domain, concurrency=concurrency)
                total_events += max(ev_count, 0)
                total_new += max(disc_count, 0)
                total_updates += max(ana_count, 0)
                total_actions += max(act_count, 0)
            else:
                # Single merged LLM call
                counts = quick_update(
                    conn, backend, domain,
                    categories_config=categories_config,
                )

                if counts["events"] == 0:
                    console.print("  [dim]No new emails to process[/dim]")
                else:
                    console.print(
                        f"  [green]{counts['events']} events, "
                        f"{counts['new_discussions']} new discussions, "
                        f"{counts['updates']} updated, "
                        f"{counts['actions']} actions[/green]"
                    )

                total_events += counts["events"]
                total_new += counts["new_discussions"]
                total_updates += counts["updates"]
                total_actions += counts["actions"]

    # Mark journal entries as processed for the companies we just updated
    mark_processed(conn, entity_type="company", entity_ids=domains)
    # Mark thread-level entries for threads belonging to these companies
    like_clauses = " OR ".join(
        "(e.from_address LIKE ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?)"
        for _ in domains
    )
    like_params: list[str] = []
    for d in domains:
        like = f"%@{d}%"
        like_params.extend([like, like, like])
    if like_clauses:
        conn.execute(
            f"""UPDATE change_journal SET processed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE entity_type = 'thread' AND processed_at IS NULL
                AND entity_id IN (
                    SELECT DISTINCT e.thread_id FROM emails e WHERE {like_clauses}
                )""",
            like_params,
        )
    conn.commit()

    mode_summary = ""
    if agent_count > 0:
        mode_summary = f" ({agent_count} agent)"
    elif staged_count > 0:
        quick_count = len(domains) - staged_count
        mode_summary = f" ({quick_count} quick, {staged_count} staged)"

    console.print(f"\n[bold green]Done.{mode_summary}[/bold green] {total_events} events, {total_new} new discussions, {total_updates} updated, {total_actions} actions")
    conn.close()


@cli.command(name="add-event")
@click.option("--company", "-c", required=True, help="Company domain or name")
@click.option("--type", "event_type", required=True, help="Event type from vocabulary (e.g. meeting_held)")
@click.option("--domain", "event_domain", required=True, help="Business domain (e.g. fundraising, pharma-deal)")
@click.option("--detail", required=True, help="Description of what happened")
@click.option("--date", "event_date", default=None, help="When it happened (YYYY-MM-DD, default: today)")
@click.option("--actor", default=None, help="Who performed the action (email address)")
@click.option("--target", default=None, help="Who the action was directed at (email address)")
@click.option("--discussion", "-d", "discussion_id", type=int, default=None, help="Assign to this discussion ID")
@click.option("--confidence", type=float, default=1.0, help="Confidence score (default: 1.0)")
@click.pass_context
def add_event(ctx: click.Context, company: str, event_type: str, event_domain: str, detail: str, event_date: str | None, actor: str | None, target: str | None, discussion_id: int | None, confidence: float) -> None:
    """Manually add a business event (meeting, call, decision, etc.).

    Writes directly to the event ledger with source_type='manual'.
    Inserts a change journal entry so the company gets re-analysed on next update.

    \b
    Example:
      email-analyser add-event --company acme.com \\
        --type meeting_held --domain fundraising \\
        --detail "Met with Sarah, discussed term sheet" \\
        --date 2026-04-10 --actor me@example.com
    """
    import uuid
    from datetime import datetime, timezone
    from email_manager.change_journal import record_change

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    # Resolve company
    row = fetchone(conn, "SELECT id, domain, name FROM companies WHERE domain = ? COLLATE NOCASE OR name = ? COLLATE NOCASE", (company, company))
    if not row:
        console.print(f"[red]Company not found: {company}[/red]")
        conn.close()
        return
    company_id = row["id"]
    company_domain = row["domain"]

    # Validate discussion if provided
    if discussion_id is not None:
        disc = fetchone(conn, "SELECT id, title FROM discussions WHERE id = ?", (discussion_id,))
        if not disc:
            console.print(f"[red]Discussion {discussion_id} not found[/red]")
            conn.close()
            return

    now = datetime.now(timezone.utc)
    if not event_date:
        event_date = now.strftime("%Y-%m-%d")

    evt_id = f"evt_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """INSERT INTO event_ledger
           (id, thread_id, source_email_id, source_calendar_event_id,
            source_type, source_id, discussion_id,
            domain, type, actor, target, event_date, detail, confidence,
            model_version, prompt_version, created_at)
           VALUES (?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            evt_id, "manual", evt_id, discussion_id,
            event_domain, event_type, actor, target,
            event_date, detail, confidence,
            "manual", "manual", now.isoformat(),
        ),
    )

    # Record in change journal
    record_change(conn, "company", company_domain, "manual_event", "add-event")
    conn.commit()

    console.print(f"[green]Event added:[/green] {event_domain}/{event_type} on {event_date}")
    console.print(f"  Detail: {detail}")
    if discussion_id:
        console.print(f"  Assigned to discussion #{discussion_id}")
    else:
        console.print(f"  [dim]No discussion assigned — will be assigned on next update[/dim]")
    console.print(f"  ID: {evt_id}")
    conn.close()


@cli.command()
@click.option("--company", "-c", default=None, help="Company domain or name (required if no --discussion)")
@click.option("--discussion", "-d", "discussion_id", type=int, default=None, help="Discussion to update with debrief")
@click.argument("text", nargs=-1)
@click.pass_context
def debrief(ctx: click.Context, company: str | None, discussion_id: int | None, text: tuple[str, ...]) -> None:
    """Record an out-of-platform interaction via freeform text.

    Sends your text + discussion context to the LLM, which extracts events,
    updates discussion state/summary/milestones, and proposes actions.

    Text can be passed as arguments or piped via stdin.

    \b
    Example:
      email-analyser debrief --company acme.com "Met with Sarah, they accepted terms"
      email-analyser debrief --discussion 42 "Call went well, pilot starts Monday"
      echo "Had a call with Bob" | email-analyser debrief --company acme.com
    """
    import sys
    from email_manager.analysis.quick_update import (
        _build_discussions_context,
        _save_quick_update_results,
        QUICK_UPDATE_SYSTEM,
    )
    from email_manager.analysis.events import load_category_config, _build_domains_block
    from email_manager.ai.factory import get_backend
    from email_manager.change_journal import record_change
    from datetime import datetime, timezone

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    # Get debrief text
    debrief_text = " ".join(text) if text else ""
    if not debrief_text:
        if not sys.stdin.isatty():
            debrief_text = sys.stdin.read().strip()
        else:
            console.print("Enter debrief text (Ctrl-D to finish):")
            lines = []
            try:
                while True:
                    lines.append(input())
            except EOFError:
                pass
            debrief_text = "\n".join(lines).strip()

    if not debrief_text:
        console.print("[red]No debrief text provided.[/red]")
        conn.close()
        return

    # Resolve company
    if discussion_id and not company:
        disc = fetchone(conn, """SELECT d.id, c.id as company_id, c.domain, c.name as company_name
                                  FROM discussions d JOIN companies c ON d.company_id = c.id
                                  WHERE d.id = ?""", (discussion_id,))
        if not disc:
            console.print(f"[red]Discussion {discussion_id} not found[/red]")
            conn.close()
            return
        company_id = disc["company_id"]
        company_domain = disc["domain"]
        company_name = disc["company_name"]
    elif company:
        row = fetchone(conn, "SELECT id, domain, name FROM companies WHERE domain = ? COLLATE NOCASE OR name = ? COLLATE NOCASE", (company, company))
        if not row:
            console.print(f"[red]Company not found: {company}[/red]")
            conn.close()
            return
        company_id = row["id"]
        company_domain = row["domain"]
        company_name = row["name"]
    else:
        console.print("[red]Provide --company or --discussion.[/red]")
        conn.close()
        return

    categories_config = load_category_config(getattr(config, "discussion_categories_path", None))
    backend = get_backend(config)
    console.print(f"Using AI backend: [bold]{backend.model_name}[/bold]")

    # Build context
    domains_block = _build_domains_block(categories_config)
    discussions_context = _build_discussions_context(conn, company_id, categories_config)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Build prompt — reuses quick_update structure but with debrief text instead of emails
    from email_manager.analysis.quick_update import _build_quick_update_prompt
    from email_manager.analysis.events import _detect_account_owner

    account_owner = _detect_account_owner(conn)

    # Replace the "new emails" section with debrief text
    debrief_emails_text = f"""--- Debrief notes (recorded by user, not from email) ---
[Debrief 0] [{today}] From: {account_owner or 'user'} <{account_owner or 'user'}>
Subject: Manual debrief
{debrief_text}
--- End debrief ---"""

    user_prompt = _build_quick_update_prompt(
        company_name, company_domain,
        debrief_emails_text, discussions_context, domains_block,
        account_owner, today,
    )

    console.print(f"Processing debrief for [bold]{company_name}[/bold] ({company_domain})...")

    try:
        result = backend.complete_json(QUICK_UPDATE_SYSTEM, user_prompt)
    except Exception as e:
        console.print(f"[red]LLM call failed: {e}[/red]")
        conn.close()
        return

    # Save results — create a fake "emails" list for source resolution
    fake_emails = [{"message_id": None, "thread_id": None}]
    counts = _save_quick_update_results(
        conn, result, company_id, fake_emails, backend.model_name, categories_config,
    )

    # Update source_type for events we just created to 'debrief'
    conn.execute(
        """UPDATE event_ledger SET source_type = 'debrief', source_id = NULL
           WHERE source_type = 'email' AND source_email_id IS NULL
           AND model_version = ? AND created_at >= ?""",
        (backend.model_name, today),
    )

    # Record in change journal
    record_change(conn, "company", company_domain, "debrief", "debrief")
    conn.commit()

    if counts["events"] == 0 and counts["new_discussions"] == 0:
        console.print("[dim]No events extracted from debrief text.[/dim]")
    else:
        console.print(
            f"[green]{counts['events']} events, "
            f"{counts['new_discussions']} new discussions, "
            f"{counts['updates']} updated, "
            f"{counts['actions']} actions[/green]"
        )

    conn.close()


@cli.command(name="update-discussion")
@click.argument("discussion_id", type=int)
@click.option("--state", default=None, help="Set workflow state")
@click.option("--title", default=None, help="Rename the discussion")
@click.option("--company", "-c", default=None, help="Reassign to a different company (domain or name)")
@click.option("--reason", default=None, help="Reason for the change (recorded in feedback)")
@click.pass_context
def update_discussion(ctx: click.Context, discussion_id: int, state: str | None, title: str | None, company: str | None, reason: str | None) -> None:
    """Manually update a discussion's state, title, or company.

    All changes are recorded in the feedback table (for AI learning) and
    discussion_state_history (for audit), and insert a change journal entry.

    \b
    Example:
      email-analyser update-discussion 42 --state signed --reason "Signed at meeting"
      email-analyser update-discussion 42 --title "Acme Series B"
      email-analyser update-discussion 42 --company newco.com
    """
    from datetime import datetime, timezone
    from email_manager.change_journal import record_change

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    disc = fetchone(conn, """SELECT d.*, c.domain as company_domain, c.name as company_name
                              FROM discussions d JOIN companies c ON d.company_id = c.id
                              WHERE d.id = ?""", (discussion_id,))
    if not disc:
        console.print(f"[red]Discussion {discussion_id} not found[/red]")
        conn.close()
        return

    if not state and not title and not company:
        console.print("[red]Provide at least one of --state, --title, or --company.[/red]")
        conn.close()
        return

    now = datetime.now(timezone.utc).isoformat()
    changes: list[str] = []

    # State change
    if state:
        old_state = disc["current_state"]
        conn.execute("UPDATE discussions SET current_state = ?, updated_at = ? WHERE id = ?", (state, now, discussion_id))
        conn.execute(
            """INSERT INTO discussion_state_history
               (discussion_id, state, entered_at, reasoning, model_used, detected_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (discussion_id, state, now, reason or "Manual state change", "manual", now),
        )
        conn.execute(
            """INSERT INTO feedback (layer, target_type, target_id, action, old_value, new_value, reason, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("discussion", "discussion", str(discussion_id), "state_change", old_state, state, reason, now),
        )
        changes.append(f"state: {old_state} → {state}")

    # Title change
    if title:
        old_title = disc["title"]
        conn.execute("UPDATE discussions SET title = ?, updated_at = ? WHERE id = ?", (title, now, discussion_id))
        conn.execute(
            """INSERT INTO feedback (layer, target_type, target_id, action, old_value, new_value, reason, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("discussion", "discussion", str(discussion_id), "title_change", old_title, title, reason, now),
        )
        changes.append(f"title: \"{old_title}\" → \"{title}\"")

    # Company reassignment
    if company:
        new_co = fetchone(conn, "SELECT id, domain, name FROM companies WHERE domain = ? COLLATE NOCASE OR name = ? COLLATE NOCASE", (company, company))
        if not new_co:
            console.print(f"[red]Company not found: {company}[/red]")
            conn.close()
            return
        old_company = f"{disc['company_name']} ({disc['company_domain']})"
        conn.execute("UPDATE discussions SET company_id = ?, updated_at = ? WHERE id = ?", (new_co["id"], now, discussion_id))
        conn.execute(
            """INSERT INTO feedback (layer, target_type, target_id, action, old_value, new_value, reason, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("discussion", "discussion", str(discussion_id), "company_change", disc["company_domain"], new_co["domain"], reason, now),
        )
        changes.append(f"company: {old_company} → {new_co['name']} ({new_co['domain']})")
        record_change(conn, "company", new_co["domain"], "discussion_reassigned", "update-discussion")

    # Journal entry for the original company
    record_change(conn, "company", disc["company_domain"], "discussion_updated", "update-discussion")
    conn.commit()

    console.print(f"[green]Discussion #{discussion_id} updated:[/green]")
    for c in changes:
        console.print(f"  {c}")
    if reason:
        console.print(f"  Reason: {reason}")
    conn.close()


@cli.command(name="merge-discussions")
@click.argument("target_id", type=int)
@click.argument("source_id", type=int)
@click.option("--reason", default=None, help="Reason for merging")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def merge_discussions(ctx: click.Context, target_id: int, source_id: int, reason: str | None, yes: bool) -> None:
    """Merge two discussions: move all data from SOURCE into TARGET.

    Events, threads, actions, milestones, state history, and proposed actions
    from the source discussion are moved to the target. The source discussion
    is then deleted.

    \b
    Example:
      email-analyser merge-discussions 42 43
      email-analyser merge-discussions 42 43 --reason "Same deal, split by mistake"
    """
    from datetime import datetime, timezone
    from email_manager.change_journal import record_change

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    target = fetchone(conn, """SELECT d.*, c.domain as company_domain, c.name as company_name
                                FROM discussions d JOIN companies c ON d.company_id = c.id
                                WHERE d.id = ?""", (target_id,))
    source = fetchone(conn, """SELECT d.*, c.domain as company_domain, c.name as company_name
                                FROM discussions d JOIN companies c ON d.company_id = c.id
                                WHERE d.id = ?""", (source_id,))

    if not target:
        console.print(f"[red]Target discussion {target_id} not found[/red]")
        conn.close()
        return
    if not source:
        console.print(f"[red]Source discussion {source_id} not found[/red]")
        conn.close()
        return

    # Show what will happen
    console.print(f"\n[bold]Merge discussions:[/bold]")
    console.print(f"  Target (keep): #{target_id} \"{target['title']}\" [{target['category']}] — {target['company_name']}")
    console.print(f"  Source (delete): #{source_id} \"{source['title']}\" [{source['category']}] — {source['company_name']}")

    # Count items to move
    event_count = fetchone(conn, "SELECT COUNT(*) as cnt FROM event_ledger WHERE discussion_id = ?", (source_id,))["cnt"]
    thread_count = fetchone(conn, "SELECT COUNT(*) as cnt FROM discussion_threads WHERE discussion_id = ?", (source_id,))["cnt"]
    action_count = fetchone(conn, "SELECT COUNT(*) as cnt FROM actions WHERE discussion_id = ?", (source_id,))["cnt"]

    console.print(f"\n  Will move: {event_count} events, {thread_count} threads, {action_count} actions")

    if not yes:
        click.confirm("\nProceed with merge?", abort=True)

    now = datetime.now(timezone.utc).isoformat()

    # Move events
    conn.execute("UPDATE event_ledger SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    # Move threads (ignore conflicts if thread already linked to target)
    conn.execute("UPDATE OR IGNORE discussion_threads SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    conn.execute("DELETE FROM discussion_threads WHERE discussion_id = ?", (source_id,))
    # Move actions
    conn.execute("UPDATE actions SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    # Move proposed actions
    conn.execute("UPDATE proposed_actions SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    # Move milestones (ignore conflicts on duplicate names)
    conn.execute("UPDATE OR IGNORE milestones SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    conn.execute("DELETE FROM milestones WHERE discussion_id = ?", (source_id,))
    # Move state history
    conn.execute("UPDATE discussion_state_history SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    # Move calendar event links
    conn.execute("UPDATE OR IGNORE discussion_events SET discussion_id = ? WHERE discussion_id = ?", (target_id, source_id))
    conn.execute("DELETE FROM discussion_events WHERE discussion_id = ?", (source_id,))
    # Reparent sub-discussions
    conn.execute("UPDATE discussions SET parent_id = ? WHERE parent_id = ?", (target_id, source_id))

    # Update target date range
    conn.execute(
        """UPDATE discussions SET
           first_seen = MIN(first_seen, ?),
           last_seen = MAX(last_seen, ?),
           updated_at = ?
           WHERE id = ?""",
        (source["first_seen"], source["last_seen"], now, target_id),
    )

    # Delete source
    conn.execute("DELETE FROM discussions WHERE id = ?", (source_id,))

    # Record feedback
    conn.execute(
        """INSERT INTO feedback (layer, target_type, target_id, action, old_value, new_value, reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("discussion", "discussion", str(target_id), "merge",
         f"#{source_id}: {source['title']}", f"merged into #{target_id}",
         reason, now),
    )

    # Journal entry
    record_change(conn, "company", target["company_domain"], "discussion_merged", "merge-discussions")
    if source["company_domain"] != target["company_domain"]:
        record_change(conn, "company", source["company_domain"], "discussion_merged", "merge-discussions")
    conn.commit()

    console.print(f"\n[green]Merged #{source_id} into #{target_id}.[/green] Moved {event_count} events, {thread_count} threads, {action_count} actions.")
    conn.close()


@cli.command()
@click.option("--company", "-c", default=None, help="Scope to a specific company (domain or name)")
@click.option("--label", "-l", default=None, help="Scope to all companies with this label")
@click.option("--company-file", default=None, type=click.Path(exists=True), help="File with company domains/names (one per line)")
@click.option("--from-stage", "from_stage", default=None,
              type=click.Choice(["extract_events", "discover_discussions", "analyse_discussions", "propose_actions"]),
              help="Delete outputs from this stage onwards, keeping earlier stages intact")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def reset(ctx: click.Context, company: str | None, label: str | None, company_file: str | None, from_stage: str | None, yes: bool) -> None:
    """Delete analysis output for a scope, optionally from a specific stage onwards.

    Without --from-stage, removes everything generated by the analysis pipeline.
    With --from-stage, only removes outputs from that stage and later stages,
    keeping earlier work intact.

    \b
    Stage order and what each produces:
      1. extract_events       → event_ledger
      2. discover_discussions → discussions, discussion_threads
      3. analyse_discussions  → discussion state/summary, milestones, state_history
      4. propose_actions      → proposed_actions

    \b
    Tables always preserved:
      - emails, threads, contacts, companies, company_labels
      - calendar_events, contact_memories, feedback

    \b
    Example: Reset everything for one company:
      email-analyser reset --company acme.com

    Example: Keep events but redo discussions onwards:
      email-analyser reset --company acme.com --from-stage discover_discussions

    Example: Just redo proposed actions:
      email-analyser reset --company acme.com --from-stage propose_actions
    """
    from pathlib import Path

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()


    # Resolve company IDs in scope
    company_ids: list[int] | None = None
    scope_desc = "ALL companies"

    if company:
        rows = fetchall(conn, "SELECT id, name, domain FROM companies WHERE domain = ? COLLATE NOCASE OR name = ? COLLATE NOCASE", (company, company))
        if not rows:
            console.print(f"[red]Company not found: {company}[/red]")
            conn.close()
            return
        company_ids = [r["id"] for r in rows]
        scope_desc = f"company: {rows[0]['name']} ({rows[0]['domain']})"
    elif label:
        rows = fetchall(conn, "SELECT DISTINCT c.id, c.name, c.domain FROM companies c JOIN company_labels cl ON c.id = cl.company_id WHERE cl.label = ?", (label,))
        if not rows:
            console.print(f"[red]No companies found with label: {label}[/red]")
            conn.close()
            return
        company_ids = [r["id"] for r in rows]
        scope_desc = f"label '{label}' ({len(company_ids)} companies)"
    elif company_file:
        entries = _read_company_file(company_file)
        lowered = [v.lower() for v in entries]
        placeholders = ", ".join("?" for _ in lowered)
        rows = fetchall(conn, f"SELECT id, name, domain FROM companies WHERE LOWER(domain) IN ({placeholders}) OR LOWER(name) IN ({placeholders})", tuple(lowered + lowered))
        if not rows:
            console.print(f"[red]No matching companies found in {company_file}[/red]")
            conn.close()
            return
        company_ids = [r["id"] for r in rows]
        scope_desc = f"company file ({len(company_ids)} companies)"

    # Count what will be deleted
    if company_ids is not None:
        placeholders = ",".join("?" for _ in company_ids)
        params = tuple(company_ids)
        disc_ids = [r[0] for r in fetchall(conn, f"SELECT id FROM discussions WHERE company_id IN ({placeholders})", params)]
    else:
        disc_ids = [r[0] for r in fetchall(conn, "SELECT id FROM discussions")]

    disc_ph = ",".join("?" for _ in disc_ids) if disc_ids else "NULL"
    disc_params = tuple(disc_ids) if disc_ids else ()

    # Determine what to delete based on --from-stage
    # Stage order: extract_events → discover_discussions → analyse_discussions → propose_actions
    STAGE_ORDER = ["extract_events", "discover_discussions", "analyse_discussions", "propose_actions"]
    if from_stage:
        stage_idx = STAGE_ORDER.index(from_stage)
        stages_to_clear = set(STAGE_ORDER[stage_idx:])
    else:
        stages_to_clear = set(STAGE_ORDER)

    delete_events = "extract_events" in stages_to_clear
    delete_discussions = "discover_discussions" in stages_to_clear
    delete_analysis = "analyse_discussions" in stages_to_clear
    delete_actions = "propose_actions" in stages_to_clear

    # Build company LIKE clauses for orphan event cleanup
    like_clauses: list[str] = []
    like_params: list[str] = []
    if company_ids is not None:
        for cid in company_ids:
            domain_row = fetchone(conn, "SELECT domain FROM companies WHERE id = ?", (cid,))
            if domain_row:
                like = f"%@{domain_row[0]}%"
                like_clauses.append("(e.from_address LIKE ? OR e.to_addresses LIKE ?)")
                like_params.extend([like, like])

    # Count what will be deleted
    counts: dict[str, int] = {}

    if delete_events:
        event_count = fetchone(conn, f"SELECT COUNT(*) FROM event_ledger WHERE discussion_id IN ({disc_ph})" if disc_ids else "SELECT COUNT(*) FROM event_ledger", disc_params)[0]
        if like_clauses:
            orphan_count = fetchone(conn, f"""SELECT COUNT(*) FROM event_ledger el
                JOIN emails e ON el.source_email_id = e.message_id
                WHERE el.discussion_id IS NULL AND ({' OR '.join(like_clauses)})""", tuple(like_params))[0]
            event_count += orphan_count
        counts["events"] = event_count

    if delete_discussions:
        counts["discussions"] = len(disc_ids)

    if delete_analysis:
        counts["milestones"] = fetchone(conn, f"SELECT COUNT(*) FROM milestones WHERE discussion_id IN ({disc_ph})" if disc_ids else "SELECT COUNT(*) FROM milestones", disc_params)[0]
        counts["state_history"] = fetchone(conn, f"SELECT COUNT(*) FROM discussion_state_history WHERE discussion_id IN ({disc_ph})" if disc_ids else "SELECT COUNT(*) FROM discussion_state_history", disc_params)[0]

    if delete_actions:
        counts["proposed_actions"] = fetchone(conn, f"SELECT COUNT(*) FROM proposed_actions WHERE discussion_id IN ({disc_ph})" if disc_ids else "SELECT COUNT(*) FROM proposed_actions", disc_params)[0]
        counts["actions"] = fetchone(conn, f"SELECT COUNT(*) FROM actions WHERE discussion_id IN ({disc_ph})" if disc_ids else "SELECT COUNT(*) FROM actions", disc_params)[0]

    # Display
    stage_label = f" from {from_stage} onwards" if from_stage else ""
    console.print(f"\n[bold]Scope:[/bold] {scope_desc}{stage_label}")
    for name, count in counts.items():
        console.print(f"  {name}: [bold]{count}[/bold]")

    if all(v == 0 for v in counts.values()):
        console.print("\n[dim]Nothing to reset.[/dim]")
        conn.close()
        return

    if not yes:
        click.confirm("\nProceed with reset?", abort=True)

    # Delete in dependency order, respecting --from-stage
    if disc_ids:
        if delete_actions:
            conn.execute(f"DELETE FROM proposed_actions WHERE discussion_id IN ({disc_ph})", disc_params)
            conn.execute(f"DELETE FROM actions WHERE discussion_id IN ({disc_ph})", disc_params)

        if delete_analysis:
            conn.execute(f"DELETE FROM milestones WHERE discussion_id IN ({disc_ph})", disc_params)
            conn.execute(f"DELETE FROM discussion_state_history WHERE discussion_id IN ({disc_ph})", disc_params)
            # Reset discussion state/summary but keep the discussion itself
            if not delete_discussions:
                conn.execute(f"UPDATE discussions SET current_state = NULL, summary = NULL, updated_at = NULL WHERE id IN ({disc_ph})", disc_params)

        if delete_discussions:
            conn.execute(f"UPDATE discussions SET parent_id = NULL WHERE parent_id IN ({disc_ph})", disc_params)
            conn.execute(f"DELETE FROM discussion_threads WHERE discussion_id IN ({disc_ph})", disc_params)
            conn.execute(f"DELETE FROM discussion_events WHERE discussion_id IN ({disc_ph})", disc_params)
            # Unlink events from discussions (but don't delete events unless extract_events is also being cleared)
            if not delete_events:
                conn.execute(f"UPDATE event_ledger SET discussion_id = NULL WHERE discussion_id IN ({disc_ph})", disc_params)
            else:
                conn.execute(f"DELETE FROM event_ledger WHERE discussion_id IN ({disc_ph})", disc_params)
            conn.execute(f"DELETE FROM discussions WHERE id IN ({disc_ph})", disc_params)

    if delete_events:
        # Delete orphan events for scoped companies
        if company_ids is not None and like_clauses:
            conn.execute(f"""DELETE FROM event_ledger WHERE discussion_id IS NULL
                AND source_email_id IN (
                    SELECT e.message_id FROM emails e WHERE {' OR '.join(like_clauses)}
                )""", tuple(like_params))
        elif company_ids is None:
            conn.execute("DELETE FROM event_ledger")

    conn.commit()
    conn.close()

    parts = [f"{v} {k}" for k, v in counts.items() if v > 0]
    console.print(f"\n[green]Reset complete.{stage_label}[/green] Deleted: {', '.join(parts) or 'nothing'}.")


@cli.command(name="migrate-db")
@click.option("--target-url", required=True, help="PostgreSQL URL to migrate to")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def migrate_db(ctx: click.Context, target_url: str, yes: bool) -> None:
    """Migrate data from SQLite to PostgreSQL.

    Copies all tables from the current SQLite database to a PostgreSQL server.
    The target database schema is created automatically.

    \b
    Example:
      email-analyser migrate-db --target-url postgresql://user:pass@host:5432/email_manager
    """
    import sqlite3 as _sqlite3
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

    config: Config = ctx.obj["config"]
    console = Console()

    # Source: SQLite
    src_path = config.db_abs_path
    if not src_path.exists():
        console.print(f"[red]SQLite database not found: {src_path}[/red]")
        return

    console.print(f"[bold]Source:[/bold] {src_path}")
    console.print(f"[bold]Target:[/bold] {target_url.split('@')[0].split('//')[0]}//***@{target_url.split('@')[-1] if '@' in target_url else target_url}")

    src = _sqlite3.connect(str(src_path))
    src.row_factory = _sqlite3.Row

    # Count rows in each table
    tables = [r[0] for r in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()]

    table_counts: dict[str, int] = {}
    for table in tables:
        count = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        table_counts[table] = count

    total_rows = sum(table_counts.values())
    console.print(f"\n[bold]Tables to migrate:[/bold] {len(tables)} ({total_rows} rows total)")
    for table, count in sorted(table_counts.items()):
        if count > 0:
            console.print(f"  {table}: {count:,}")

    if not yes:
        click.confirm("\nProceed with migration?", abort=True)

    # Target: PostgreSQL
    from email_manager.db_postgres import get_postgres_connection
    from email_manager.db import _init_schema
    dst = get_postgres_connection(target_url)

    # Initialize schema on target
    console.print("\n[bold]Creating schema...[/bold]")
    _init_schema(dst)
    console.print("[green]Schema created[/green]")

    # Tables in dependency order (FKs)
    ordered_tables = [
        "schema_version",
        "emails", "sync_state", "contacts", "threads",
        "projects", "email_projects", "email_references",
        "companies", "company_contacts", "company_labels",
        "co_email_stats", "contact_memories",
        "pipeline_runs", "calendar_events",
        "processing_runs",
        "discussions", "discussion_threads", "discussion_state_history",
        "discussion_events",
        "event_ledger", "milestones",
        "actions", "proposed_actions",
        "feedback", "few_shot_examples", "learned_rules",
        "change_journal",
    ]

    # Only migrate tables that exist in source
    tables_to_migrate = [t for t in ordered_tables if t in table_counts]
    # Add any tables we missed
    for t in tables:
        if t not in tables_to_migrate:
            tables_to_migrate.append(t)

    BATCH = 1000
    migrated = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Migrating", total=total_rows)

        for table in tables_to_migrate:
            count = table_counts.get(table, 0)
            if count == 0:
                continue

            # Get column names from source and target, use intersection
            src_cols = [r[1] for r in src.execute(f"PRAGMA table_info({table})").fetchall()]
            pg_cur_tmp = dst._conn.cursor()
            pg_cur_tmp.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
                (table,),
            )
            dst_cols = {r[0] for r in pg_cur_tmp.fetchall()}
            col_names = [c for c in src_cols if c in dst_cols]
            if not col_names:
                continue
            cols_str = ", ".join(col_names)
            placeholders = ", ".join(["%s"] * len(col_names))

            progress.update(task, description=f"Migrating {table}")

            # Use raw psycopg2 cursor for bulk inserts (bypass translate_sql)
            pg_cur = dst._conn.cursor()

            # Clear target table first (in case of partial previous migration)
            try:
                pg_cur.execute(f"DELETE FROM {table}")
                dst._conn.commit()
            except Exception:
                dst._conn.rollback()

            offset = 0
            while offset < count:
                rows = src.execute(
                    f"SELECT {cols_str} FROM {table} LIMIT {BATCH} OFFSET {offset}"
                ).fetchall()
                if not rows:
                    break

                for row in rows:
                    values = tuple(row[c] for c in col_names)
                    try:
                        pg_cur.execute(
                            f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})",
                            values,
                        )
                    except Exception as e:
                        # Skip constraint violations (e.g. duplicates)
                        dst._conn.rollback()

                dst._conn.commit()
                offset += len(rows)
                migrated += len(rows)
                progress.advance(task, len(rows))

    # Fix sequences (PostgreSQL SERIAL columns need sequence reset)
    serial_tables = [
        "emails", "contacts", "threads", "projects", "companies",
        "pipeline_runs", "discussions", "discussion_state_history",
        "actions", "calendar_events", "milestones", "feedback",
        "few_shot_examples", "learned_rules", "proposed_actions",
        "processing_runs", "change_journal",
    ]
    for table in serial_tables:
        if table in table_counts and table_counts[table] > 0:
            try:
                dst.execute(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 0) + 1, false) FROM {table}"
                )
                dst.commit()
            except Exception:
                dst._conn.rollback()

    src.close()
    dst.close()

    console.print(f"\n[bold green]Migration complete.[/bold green] {migrated:,} rows migrated across {len(tables_to_migrate)} tables.")
    console.print(f"\nTo switch to PostgreSQL, set in .env:")
    console.print(f"  DB_BACKEND=postgres")
    console.print(f"  DB_URL={target_url}")


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
@click.option("--limit", "-n", default=30, help="Max companies to show (ignored with --csv)")
@click.option("--label", "-l", default=None, help="Filter by label (e.g. customer, vendor, partner)")
@click.option("--unlabelled", is_flag=True, help="Show only companies without labels")
@click.option("--updated-after", default=None, help="Only show companies analysed after this date (YYYY-MM-DD)")
@click.option("--updated-before", default=None, help="Only show companies not analysed since this date, or never analysed (YYYY-MM-DD)")
@click.option("--last-seen-after", default=None, help="Only show companies with email activity after this date (YYYY-MM-DD)")
@click.option("--last-seen-before", default=None, help="Only show companies with last email activity before this date (YYYY-MM-DD)")
@click.option("--csv", "csv_output", is_flag=True, help="Output all matching companies as CSV (no limit)")
@click.pass_context
def companies(ctx: click.Context, limit: int, label: str | None, unlabelled: bool, updated_after: str | None, updated_before: str | None, last_seen_after: str | None, last_seen_before: str | None, csv_output: bool) -> None:
    """List companies you interact with and their associated email addresses."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    conditions: list[str] = []
    params: list = []

    if label:
        conditions.append("c.id IN (SELECT company_id FROM company_labels WHERE label = ?)")
        params.append(label)
    elif unlabelled:
        conditions.append("c.id NOT IN (SELECT company_id FROM company_labels)")

    if updated_after:
        conditions.append("""c.id IN (
            SELECT d.company_id FROM discussions d
            JOIN milestones m ON m.discussion_id = d.id
            WHERE m.last_evaluated_at >= ?
        )""")
        params.append(updated_after)

    if updated_before:
        conditions.append("""(
            c.id NOT IN (
                SELECT d.company_id FROM discussions d
                JOIN milestones m ON m.discussion_id = d.id
                WHERE d.company_id IS NOT NULL
            )
            OR c.id IN (
                SELECT d.company_id FROM discussions d
                LEFT JOIN milestones m ON m.discussion_id = d.id
                GROUP BY d.company_id
                HAVING MAX(m.last_evaluated_at) < ? OR MAX(m.last_evaluated_at) IS NULL
            )
        )""")
        params.append(updated_before)

    if last_seen_after:
        conditions.append("c.last_seen >= ?")
        params.append(last_seen_after)

    if last_seen_before:
        conditions.append("(c.last_seen < ? OR c.last_seen IS NULL)")
        params.append(last_seen_before)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    if csv_output:
        # No limit for CSV — output all matching rows
        rows = fetchall(
            conn,
            f"""SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen
                FROM companies c
                {where}
                ORDER BY c.email_count DESC""",
            tuple(params),
        )
    else:
        params.append(limit)
        rows = fetchall(
            conn,
            f"""SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen
                FROM companies c
                {where}
                ORDER BY c.email_count DESC
                LIMIT ?""",
            tuple(params),
        )

    console = Console()
    if not rows:
        if csv_output:
            conn.close()
            return
        console.print("[dim]No companies found. Run 'email-manager analyse --stage extract_base' first.[/dim]")
        conn.close()
        return

    if csv_output:
        import csv
        import sys
        writer = csv.writer(sys.stdout)
        writer.writerow(["name", "domain", "email_count", "labels", "first_seen", "last_seen"])
        for row in rows:
            labels = fetchall(
                conn,
                "SELECT label FROM company_labels WHERE company_id = ? ORDER BY confidence DESC",
                (row["id"],),
            )
            label_str = ";".join(l["label"] for l in labels)
            writer.writerow([
                row["name"],
                row["domain"],
                row["email_count"],
                label_str,
                (row["first_seen"] or "")[:10],
                (row["last_seen"] or "")[:10],
            ])
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

    # Summary stats — scoped to active filters (excluding label/unlabelled which already narrow the view)
    if not label and not unlabelled:
        # Build the same date conditions used for the main query
        stats_conditions: list[str] = []
        stats_params: list = []

        if updated_after:
            stats_conditions.append("""c.id IN (
                SELECT d.company_id FROM discussions d
                JOIN milestones m ON m.discussion_id = d.id
                WHERE m.last_evaluated_at >= ?
            )""")
            stats_params.append(updated_after)

        if updated_before:
            stats_conditions.append("""(
                c.id NOT IN (
                    SELECT d.company_id FROM discussions d
                    JOIN milestones m ON m.discussion_id = d.id
                    WHERE d.company_id IS NOT NULL
                )
                OR c.id IN (
                    SELECT d.company_id FROM discussions d
                    LEFT JOIN milestones m ON m.discussion_id = d.id
                    GROUP BY d.company_id
                    HAVING MAX(m.last_evaluated_at) < ? OR MAX(m.last_evaluated_at) IS NULL
                )
            )""")
            stats_params.append(updated_before)

        if last_seen_after:
            stats_conditions.append("c.last_seen >= ?")
            stats_params.append(last_seen_after)

        if last_seen_before:
            stats_conditions.append("(c.last_seen < ? OR c.last_seen IS NULL)")
            stats_params.append(last_seen_before)

        stats_where = (" WHERE " + " AND ".join(stats_conditions)) if stats_conditions else ""
        stats_tuple = tuple(stats_params)

        total = fetchone(conn, f"SELECT COUNT(*) as cnt FROM companies c{stats_where}", stats_tuple)["cnt"]
        labelled_count = fetchone(
            conn,
            f"SELECT COUNT(DISTINCT cl.company_id) as cnt FROM company_labels cl JOIN companies c ON c.id = cl.company_id{stats_where}",
            stats_tuple,
        )["cnt"]
        unlabelled_count = total - labelled_count
        label_counts = fetchall(
            conn,
            f"SELECT cl.label, COUNT(*) as cnt FROM company_labels cl JOIN companies c ON c.id = cl.company_id{stats_where} GROUP BY cl.label ORDER BY cnt DESC",
            stats_tuple,
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
@click.option("--label", "-l", default=None, help="Filter by company label (e.g. investor)")
@click.option("--updated-after", default=None, help="Only show discussions analysed after this date (YYYY-MM-DD)")
@click.option("--updated-before", default=None, help="Only show discussions not analysed since this date, or never analysed (YYYY-MM-DD)")
@click.pass_context
def discussions(ctx: click.Context, limit: int, company: str | None, contact: str | None, category: str | None, state: str | None, label: str | None, updated_after: str | None, updated_before: str | None) -> None:
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

    if label:
        conditions.append("d.company_id IN (SELECT company_id FROM company_labels WHERE label = ?)")
        params.append(label)

    if updated_after:
        conditions.append("""d.id IN (
            SELECT m.discussion_id FROM milestones m WHERE m.last_evaluated_at >= ?
        )""")
        params.append(updated_after)

    if updated_before:
        conditions.append("""(
            NOT EXISTS (SELECT 1 FROM milestones m WHERE m.discussion_id = d.id)
            OR d.id IN (
                SELECT m.discussion_id FROM milestones m
                GROUP BY m.discussion_id
                HAVING MAX(m.last_evaluated_at) < ?
            )
        )""")
        params.append(updated_before)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = fetchall(
        conn,
        f"""SELECT d.id, d.title, d.category, d.current_state, d.summary,
                   d.first_seen, d.last_seen, d.updated_at, c.name as company_name, c.domain
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
    table.add_column("Company", width=25)
    table.add_column("Category", width=16)
    table.add_column("State", width=14)
    table.add_column("Title", width=35)
    table.add_column("Start", width=10)
    table.add_column("End", width=10)
    table.add_column("Analysed", width=10)

    for row in rows:
        company_display = f"{row['company_name']} ({row['domain']})"
        table.add_row(
            str(row["id"]),
            company_display[:25],
            row["category"],
            row["current_state"] or "",
            row["title"][:35],
            (row["first_seen"] or "")[:10],
            (row["last_seen"] or "")[:10],
            (row["updated_at"] or "")[:10],
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
@click.option("--updated-after", default=None, help="Only show contacts last seen after this date (YYYY-MM-DD)")
@click.option("--updated-before", default=None, help="Only show contacts last seen before this date (YYYY-MM-DD)")
@click.pass_context
def contacts(ctx: click.Context, limit: int, updated_after: str | None, updated_before: str | None) -> None:
    """List contacts ranked by frequency."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)

    conditions: list[str] = []
    params: list = []

    if updated_after:
        conditions.append("last_seen >= ?")
        params.append(updated_after)

    if updated_before:
        conditions.append("(last_seen IS NULL OR last_seen < ?)")
        params.append(updated_before)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = fetchall(
        conn,
        f"""SELECT email, name, company, email_count, sent_count, received_count,
                  first_seen, last_seen
           FROM contacts
           {where}
           ORDER BY email_count DESC
           LIMIT ?""",
        tuple(params),
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


# ── Review / Evaluation ──────────────────────────────────────────────────────

@cli.command()
@click.argument("run_id", required=False, type=int)
@click.option("--limit", "-n", default=20, help="Number of recent runs to show")
@click.option("--company", "-c", default=None, help="Filter by company domain")
@click.option("--mode", "-m", default=None, help="Filter by mode (staged:*, quick, agent)")
@click.option("--annotate", is_flag=True, help="Interactively annotate items in this run")
@click.pass_context
def review(ctx: click.Context, run_id: int | None, limit: int, company: str | None,
           mode: str | None, annotate: bool) -> None:
    """Review processing runs and their proposed changes.

    Without arguments: list recent runs.
    With RUN_ID: show the proposed changes for that run.
    With --annotate: interactively mark items as correct/incorrect/missing.
    """
    import json as _json

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    if run_id is None:
        # List recent runs
        conditions = ["1=1"]
        params: list = []
        if company:
            conditions.append("company_domain = ?")
            params.append(company)
        if mode:
            conditions.append("mode LIKE ?")
            params.append(f"%{mode}%")
        where = " AND ".join(conditions)

        from email_manager.db import fetchall
        runs = fetchall(
            conn,
            f"""SELECT id, company_domain, mode, model, started_at, completed_at,
                       events_created, discussions_created, discussions_updated,
                       actions_proposed, input_tokens, output_tokens,
                       proposed_changes_json IS NOT NULL as has_snapshot
                FROM processing_runs
                WHERE {where}
                ORDER BY id DESC
                LIMIT ?""",
            (*params, limit),
        )

        if not runs:
            console.print("[dim]No processing runs found.[/dim]")
            return

        from rich.table import Table
        table = Table(title="Processing Runs")
        table.add_column("ID", style="bold")
        table.add_column("Company")
        table.add_column("Mode")
        table.add_column("Model")
        table.add_column("Started")
        table.add_column("Events")
        table.add_column("Disc+")
        table.add_column("Disc~")
        table.add_column("Actions")
        table.add_column("Snapshot")

        for r in runs:
            started = (r["started_at"] or "")[:16]
            table.add_row(
                str(r["id"]),
                r["company_domain"] or "",
                r["mode"] or "",
                (r["model"] or "")[:20],
                started,
                str(r["events_created"] or 0),
                str(r["discussions_created"] or 0),
                str(r["discussions_updated"] or 0),
                str(r["actions_proposed"] or 0),
                "yes" if r["has_snapshot"] else "no",
            )

        console.print(table)
        return

    # Show specific run
    from email_manager.db import fetchone
    run = fetchone(
        conn,
        """SELECT * FROM processing_runs WHERE id = ?""",
        (run_id,),
    )
    if not run:
        console.print(f"[red]Run #{run_id} not found.[/red]")
        return

    console.print(f"\n[bold]Run #{run_id}[/bold]")
    console.print(f"  Company: {run['company_domain']}")
    console.print(f"  Mode: {run['mode']}")
    console.print(f"  Model: {run['model']}")
    console.print(f"  Started: {run['started_at']}")
    console.print(f"  Completed: {run['completed_at'] or 'in progress'}")
    if run.get("input_tokens"):
        console.print(f"  Tokens: {run['input_tokens']} in / {run['output_tokens']} out")

    snapshot_json = run.get("proposed_changes_json")
    if not snapshot_json:
        console.print("\n[dim]No ProposedChanges snapshot for this run.[/dim]")
        return

    snapshot = _json.loads(snapshot_json)
    from email_manager.ai.agent_backend import ProposedChanges
    proposed = ProposedChanges(snapshot)

    if proposed.is_empty:
        console.print("\n[dim]Empty changeset.[/dim]")
        return

    # Display numbered items
    item_idx = 0

    if proposed.events:
        console.print(f"\n[bold]Events ({len(proposed.events)}):[/bold]")
        for ev in proposed.events:
            disc = f" → disc #{ev.get('discussion_id', '?')}" if ev.get("discussion_id") else ""
            console.print(
                f"  [{item_idx}] {ev.get('event_date', '?')} "
                f"[cyan]{ev.get('domain', '?')}/{ev.get('type', '?')}[/cyan]: "
                f"{(ev.get('detail') or '')[:80]}{disc}"
            )
            item_idx += 1

    if proposed.new_discussions:
        console.print(f"\n[bold]New Discussions ({len(proposed.new_discussions)}):[/bold]")
        for d in proposed.new_discussions:
            parent = f" (sub of #{d.get('parent_id')})" if d.get("parent_id") else ""
            console.print(
                f"  [{item_idx}] [green]\"{d.get('title', '?')}\"[/green] "
                f"[{d.get('category', '?')}]{parent}"
            )
            item_idx += 1

    if proposed.discussion_updates:
        console.print(f"\n[bold]Discussion Updates ({len(proposed.discussion_updates)}):[/bold]")
        for u in proposed.discussion_updates:
            parts = []
            if u.get("state"):
                parts.append(f"state → {u['state']}")
            if u.get("summary"):
                parts.append(f"summary: {u['summary'][:60]}...")
            if u.get("milestones"):
                achieved = [m["name"] for m in u["milestones"] if m.get("achieved")]
                if achieved:
                    parts.append(f"milestones: {', '.join(achieved)}")
            if u.get("proposed_actions"):
                for a in u["proposed_actions"]:
                    parts.append(f"action [{a.get('priority', '?')}]: {a.get('action', '')[:60]}")
            console.print(f"  [{item_idx}] Discussion #{u.get('discussion_id', '?')}:")
            for p in parts:
                console.print(f"        {p}")
            item_idx += 1

    if proposed.event_assignments:
        console.print(f"\n[bold]Event Assignments ({len(proposed.event_assignments)}):[/bold]")
        for ea in proposed.event_assignments:
            console.print(
                f"  [{item_idx}] {ea.get('event_id', '?')} → discussion #{ea.get('discussion_id', '?')}"
            )
            item_idx += 1

    if proposed.label_updates:
        console.print(f"\n[bold]Label Updates ({len(proposed.label_updates)}):[/bold]")
        for lu in proposed.label_updates:
            labels = [f"{l['label']} ({l.get('confidence', '?'):.0%})" for l in lu.get("labels", [])]
            name_update = f" name → \"{lu['company_name']}\"" if lu.get("company_name") else ""
            console.print(
                f"  [{item_idx}] Company #{lu.get('company_id', '?')}:{name_update}"
                f" labels: [cyan]{', '.join(labels)}[/cyan]"
            )
            if lu.get("company_description"):
                console.print(f"        {lu['company_description'][:80]}")
            item_idx += 1

    # Load existing annotations for this run
    from email_manager.db import fetchall as _fetchall
    existing_fb = _fetchall(
        conn,
        "SELECT * FROM feedback WHERE target_type LIKE ? ORDER BY id",
        (f"run:{run_id}:%",),
    )
    if existing_fb:
        console.print(f"\n[bold]Existing Annotations ({len(existing_fb)}):[/bold]")
        for fb in existing_fb:
            parts = fb["target_type"].split(":")
            idx_str = parts[2] if len(parts) > 2 else "?"
            action_style = {"correct": "green", "incorrect": "red", "missing": "yellow"}.get(fb["action"], "white")
            console.print(
                f"  Item [{idx_str}]: [{action_style}]{fb['action']}[/{action_style}]"
                + (f" — {fb['reason']}" if fb.get("reason") else "")
            )

    if not annotate:
        return

    # Interactive annotation
    console.print("\n[bold]Annotate items[/bold]")
    console.print("  Enter: [item_number] [correct|incorrect|missing] [optional reason]")
    console.print("  Example: 0 incorrect wrong event type, should be meeting_held")
    console.print("  Type 'done' to finish.\n")

    from datetime import datetime as _dt, timezone as _tz
    while True:
        try:
            line = click.prompt("annotate", default="done", show_default=False)
        except (EOFError, click.Abort):
            break
        line = line.strip()
        if line.lower() in ("done", "quit", "q", ""):
            break

        parts = line.split(None, 2)
        if len(parts) < 2:
            console.print("[red]Usage: <item_number> <correct|incorrect|missing> [reason][/red]")
            continue

        try:
            idx = int(parts[0])
        except ValueError:
            console.print(f"[red]'{parts[0]}' is not a valid item number.[/red]")
            continue

        action = parts[1].lower()
        if action not in ("correct", "incorrect", "missing"):
            console.print(f"[red]Action must be correct, incorrect, or missing.[/red]")
            continue

        reason = parts[2] if len(parts) > 2 else None

        # Determine which layer this item belongs to
        layer = "unknown"
        total = 0
        for section, section_layer in [
            (proposed.events, "events"),
            (proposed.new_discussions, "discussions"),
            (proposed.discussion_updates, "discussion_updates"),
            (proposed.event_assignments, "event_assignments"),
        ]:
            if total <= idx < total + len(section):
                layer = section_layer
                break
            total += len(section)

        now_str = _dt.now(_tz.utc).isoformat()
        conn.execute(
            """INSERT INTO feedback (layer, target_type, target_id, action, reason, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (layer, f"run:{run_id}:{idx}", str(run_id), action, reason, now_str),
        )
        conn.commit()

        action_style = {"correct": "green", "incorrect": "red", "missing": "yellow"}[action]
        console.print(f"  [{action_style}]Saved: item [{idx}] = {action}[/{action_style}]")

    console.print("[dim]Annotation complete.[/dim]")


@cli.command()
@click.option("--company", "-c", default=None, help="Filter by company domain")
@click.option("--mode", "-m", default=None, help="Filter by mode")
@click.option("--since", default=None, help="Only runs after this date (YYYY-MM-DD)")
@click.pass_context
def eval(ctx: click.Context, company: str | None, mode: str | None, since: str | None) -> None:
    """Show evaluation metrics from review annotations."""
    import json as _json

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    from email_manager.db import fetchall

    # Get all annotated runs
    conditions = ["proposed_changes_json IS NOT NULL"]
    params: list = []
    if company:
        conditions.append("company_domain = ?")
        params.append(company)
    if mode:
        conditions.append("mode LIKE ?")
        params.append(f"%{mode}%")
    if since:
        conditions.append("started_at >= ?")
        params.append(since)

    where = " AND ".join(conditions)
    runs = fetchall(
        conn,
        f"""SELECT id, company_domain, mode, model, started_at
            FROM processing_runs WHERE {where} ORDER BY id DESC""",
        tuple(params),
    )

    if not runs:
        console.print("[dim]No runs with snapshots found.[/dim]")
        return

    # Gather annotations
    all_feedback = fetchall(
        conn,
        "SELECT * FROM feedback WHERE target_type LIKE 'run:%' ORDER BY id",
    )

    # Group by run
    fb_by_run: dict[int, list] = {}
    for fb in all_feedback:
        parts = fb["target_type"].split(":")
        if len(parts) >= 2:
            try:
                rid = int(parts[1])
                fb_by_run.setdefault(rid, []).append(fb)
            except ValueError:
                pass

    annotated_runs = [r for r in runs if r["id"] in fb_by_run]
    if not annotated_runs:
        console.print(f"[dim]{len(runs)} runs with snapshots, but none have annotations yet.[/dim]")
        console.print("[dim]Use 'review <run_id> --annotate' to add annotations.[/dim]")
        return

    # Compute metrics
    total_correct = 0
    total_incorrect = 0
    total_missing = 0
    by_layer: dict[str, dict[str, int]] = {}

    for fb in all_feedback:
        action = fb["action"]
        layer = fb["layer"]
        by_layer.setdefault(layer, {"correct": 0, "incorrect": 0, "missing": 0})
        if action == "correct":
            total_correct += 1
            by_layer[layer]["correct"] += 1
        elif action == "incorrect":
            total_incorrect += 1
            by_layer[layer]["incorrect"] += 1
        elif action == "missing":
            total_missing += 1
            by_layer[layer]["missing"] += 1

    total_judged = total_correct + total_incorrect
    precision = total_correct / total_judged if total_judged > 0 else 0.0

    console.print(f"\n[bold]Evaluation Summary[/bold]")
    console.print(f"  Annotated runs: {len(annotated_runs)} / {len(runs)}")
    console.print(f"  Total annotations: {total_correct + total_incorrect + total_missing}")
    console.print(f"  Correct: [green]{total_correct}[/green]")
    console.print(f"  Incorrect: [red]{total_incorrect}[/red]")
    console.print(f"  Missing: [yellow]{total_missing}[/yellow]")
    console.print(f"  Precision: [bold]{precision:.1%}[/bold] ({total_correct}/{total_judged} judged items)")

    if by_layer:
        console.print(f"\n[bold]By Layer:[/bold]")
        from rich.table import Table
        table = Table()
        table.add_column("Layer")
        table.add_column("Correct", style="green")
        table.add_column("Incorrect", style="red")
        table.add_column("Missing", style="yellow")
        table.add_column("Precision")

        for layer, counts in sorted(by_layer.items()):
            judged = counts["correct"] + counts["incorrect"]
            prec = counts["correct"] / judged if judged > 0 else 0.0
            table.add_row(
                layer,
                str(counts["correct"]),
                str(counts["incorrect"]),
                str(counts["missing"]),
                f"{prec:.0%}",
            )
        console.print(table)


@cli.command()
@click.argument("action", type=click.Choice(["add", "list", "remove"]))
@click.option("--layer", "-l", default=None,
              type=click.Choice(["events", "discussions", "discussion_updates", "actions", "labels", "quick_update", "agent"]),
              help="Which analysis layer this rule applies to")
@click.option("--category", default=None, help="Optional category (domain) this rule applies to")
@click.option("--rule", "-r", default=None, help="The rule text (for 'add')")
@click.option("--rule-id", type=int, default=None, help="Rule ID (for 'remove')")
@click.pass_context
def learn(ctx: click.Context, action: str, layer: str | None, category: str | None,
          rule: str | None, rule_id: int | None) -> None:
    """Manage learned rules that get injected into LLM prompts.

    \b
    Examples:
      learn add -l events -r "Meeting scheduling emails should use the scheduling domain"
      learn add -l actions --category investment -r "Always suggest a follow-up within 2 weeks"
      learn list
      learn remove --rule-id 3
    """
    from datetime import datetime as _dt, timezone as _tz

    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    if action == "list":
        from email_manager.db import fetchall
        conditions = ["1=1"]
        params: list = []
        if layer:
            conditions.append("layer = ?")
            params.append(layer)
        rows = fetchall(
            conn,
            f"SELECT * FROM learned_rules WHERE {' AND '.join(conditions)} ORDER BY layer, id",
            tuple(params),
        )
        if not rows:
            console.print("[dim]No learned rules.[/dim]")
            return

        from rich.table import Table
        table = Table(title="Learned Rules")
        table.add_column("ID", style="bold")
        table.add_column("Layer")
        table.add_column("Category")
        table.add_column("Rule")
        table.add_column("Active")

        for r in rows:
            table.add_row(
                str(r["id"]),
                r["layer"],
                r["category"] or "",
                r["rule_text"][:80],
                "yes" if r["active"] else "no",
            )
        console.print(table)

    elif action == "add":
        if not layer:
            console.print("[red]--layer is required for 'add'.[/red]")
            return
        if not rule:
            console.print("[red]--rule is required for 'add'.[/red]")
            return

        now = _dt.now(_tz.utc).isoformat()
        cursor = conn.execute(
            """INSERT INTO learned_rules (layer, category, rule_text, active, created_at)
               VALUES (?, ?, ?, 1, ?)""",
            (layer, category, rule, now),
        )
        conn.commit()
        console.print(f"[green]Rule #{cursor.lastrowid} added to layer '{layer}'.[/green]")

    elif action == "remove":
        if not rule_id:
            console.print("[red]--rule-id is required for 'remove'.[/red]")
            return

        conn.execute("UPDATE learned_rules SET active = 0 WHERE id = ?", (rule_id,))
        conn.commit()
        console.print(f"[yellow]Rule #{rule_id} deactivated.[/yellow]")


@cli.command()
@click.argument("run_id", type=int)
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without actually deleting")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def rollback(ctx: click.Context, run_id: int, dry_run: bool, yes: bool) -> None:
    """Roll back a processing run and all subsequent runs for that company+mode.

    Deletes all derived data (events, discussions, milestones, actions,
    state history) produced by the specified run and any later runs in the
    same company+mode chain. Raw emails are never touched.
    """
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    from email_manager.db import fetchone, fetchall

    run = fetchone(conn, "SELECT * FROM processing_runs WHERE id = ?", (run_id,))
    if not run:
        console.print(f"[red]Run #{run_id} not found.[/red]")
        return

    company = run["company_domain"]
    mode = run["mode"]

    # Find this run and all later runs in the same chain
    runs_to_rollback = fetchall(
        conn,
        """SELECT id, started_at, events_created, discussions_created,
                  discussions_updated, actions_proposed
           FROM processing_runs
           WHERE company_domain = ? AND mode = ? AND id >= ?
           ORDER BY id ASC""",
        (company, mode, run_id),
    )

    if not runs_to_rollback:
        console.print("[dim]No runs to roll back.[/dim]")
        return

    run_ids = [r["id"] for r in runs_to_rollback]
    placeholders = ",".join("?" for _ in run_ids)

    # Count what will be deleted
    event_count = fetchone(
        conn,
        f"SELECT COUNT(*) as cnt FROM event_ledger WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    disc_count = fetchone(
        conn,
        f"SELECT COUNT(*) as cnt FROM discussions WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    milestone_count = fetchone(
        conn,
        f"SELECT COUNT(*) as cnt FROM milestones WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    action_count = fetchone(
        conn,
        f"SELECT COUNT(*) as cnt FROM proposed_actions WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )

    console.print(f"\n[bold]Rollback plan for {company} ({mode})[/bold]")
    console.print(f"  Runs to remove: {len(runs_to_rollback)} (#{run_ids[0]} through #{run_ids[-1]})")
    console.print(f"  Events to delete: {event_count['cnt']}")
    console.print(f"  Discussions to delete: {disc_count['cnt']}")
    console.print(f"  Milestones to delete: {milestone_count['cnt']}")
    console.print(f"  Actions to delete: {action_count['cnt']}")

    if dry_run:
        console.print("\n[dim]Dry run — nothing deleted.[/dim]")
        return

    if not yes:
        click.confirm("Proceed with rollback?", abort=True)

    # Delete in dependency order
    # 1. Proposed actions
    conn.execute(
        f"DELETE FROM proposed_actions WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    # 2. Milestones
    conn.execute(
        f"DELETE FROM milestones WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    # 3. State history for discussions created by these runs
    disc_ids_rows = fetchall(
        conn,
        f"SELECT id FROM discussions WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    if disc_ids_rows:
        disc_ids = [r["id"] for r in disc_ids_rows]
        disc_ph = ",".join("?" for _ in disc_ids)
        conn.execute(
            f"DELETE FROM discussion_state_history WHERE discussion_id IN ({disc_ph})",
            tuple(disc_ids),
        )
        conn.execute(
            f"DELETE FROM discussion_threads WHERE discussion_id IN ({disc_ph})",
            tuple(disc_ids),
        )
    # 4. Unlink events from discussions being deleted
    if disc_ids_rows:
        conn.execute(
            f"UPDATE event_ledger SET discussion_id = NULL WHERE discussion_id IN ({disc_ph})",
            tuple(disc_ids),
        )
    # 5. Events
    conn.execute(
        f"DELETE FROM event_ledger WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    # 6. Discussions
    conn.execute(
        f"DELETE FROM discussions WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    # 7. LLM call records
    conn.execute(
        f"DELETE FROM llm_calls WHERE run_id IN ({placeholders})",
        tuple(run_ids),
    )
    # 8. The processing runs themselves
    conn.execute(
        f"DELETE FROM processing_runs WHERE id IN ({placeholders})",
        tuple(run_ids),
    )

    conn.commit()
    console.print(f"\n[green]Rolled back {len(runs_to_rollback)} runs for {company} ({mode}).[/green]")


@cli.command()
@click.argument("company_domain")
@click.option("--mode", "-m", default=None, help="Filter by mode")
@click.pass_context
def history(ctx: click.Context, company_domain: str, mode: str | None) -> None:
    """Show the processing run history (changeset chain) for a company."""
    config: Config = ctx.obj["config"]
    conn = get_db(config)
    console = Console()

    from email_manager.db import fetchall

    conditions = ["company_domain = ?"]
    params: list = [company_domain]
    if mode:
        conditions.append("mode LIKE ?")
        params.append(f"%{mode}%")

    runs = fetchall(
        conn,
        f"""SELECT id, mode, model, started_at, completed_at,
                   events_created, discussions_created, discussions_updated,
                   actions_proposed, parent_run_id, email_cutoff_date,
                   proposed_changes_json IS NOT NULL as has_snapshot,
                   prompt_hash
            FROM processing_runs
            WHERE {" AND ".join(conditions)}
            ORDER BY id ASC""",
        tuple(params),
    )

    if not runs:
        console.print(f"[dim]No processing runs for {company_domain}.[/dim]")
        return

    from rich.table import Table
    table = Table(title=f"History: {company_domain}")
    table.add_column("ID", style="bold")
    table.add_column("Parent")
    table.add_column("Mode")
    table.add_column("Model")
    table.add_column("Started")
    table.add_column("Email Cutoff")
    table.add_column("Evts")
    table.add_column("Disc+")
    table.add_column("Disc~")
    table.add_column("Acts")
    table.add_column("Prompt")

    for r in runs:
        table.add_row(
            str(r["id"]),
            str(r["parent_run_id"] or "—"),
            (r["mode"] or "").replace("staged:", ""),
            (r["model"] or "")[:20],
            (r["started_at"] or "")[:16],
            (r["email_cutoff_date"] or "")[:10],
            str(r["events_created"] or 0),
            str(r["discussions_created"] or 0),
            str(r["discussions_updated"] or 0),
            str(r["actions_proposed"] or 0),
            (r["prompt_hash"] or "—")[:8],
        )

    console.print(table)


if __name__ == "__main__":
    cli()
