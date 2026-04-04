"""Fetch company homepages and save as markdown files — no AI needed."""

from __future__ import annotations

import sqlite3
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import html2text

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.db import fetchall


HOMEPAGES_DIR = Path("data/homepages")
DEFAULT_MAX_WORKERS = 10


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    )


def homepage_path(domain: str, base_dir: Path = HOMEPAGES_DIR) -> Path:
    """Return the markdown file path for a given domain."""
    return base_dir / f"{domain}.md"


def fetch_homepages(
    conn: sqlite3.Connection,
    console: Console | None = None,
    limit: int | None = None,
    force: bool = False,
    output_dir: Path = HOMEPAGES_DIR,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> int:
    """Download the homepage for each company and save as a markdown file."""
    if console is None:
        console = Console()

    output_dir.mkdir(parents=True, exist_ok=True)

    if force:
        sql = "SELECT id, name, domain FROM companies ORDER BY email_count DESC"
    else:
        sql = "SELECT id, name, domain FROM companies WHERE homepage_fetched_at IS NULL ORDER BY email_count DESC"

    if limit:
        sql += f" LIMIT {int(limit)}"

    companies = fetchall(conn, sql)
    if not companies:
        console.print("  [dim]All company homepages already fetched.[/dim]")
        return 0

    now = datetime.now(timezone.utc).isoformat()
    fetched = 0
    failed = 0

    with _make_progress(console) as progress:
        task = progress.add_task("Fetching homepages", total=len(companies))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # Submit all downloads
            future_to_company = {
                pool.submit(_download_homepage, row["domain"]): row
                for row in companies
            }

            for future in as_completed(future_to_company):
                row = future_to_company[future]
                domain = row["domain"]
                company_id = row["id"]

                try:
                    html = future.result()
                except Exception:
                    html = None

                if html is not None:
                    try:
                        # html2text is not thread-safe, so create per-result
                        converter = html2text.HTML2Text()
                        converter.ignore_links = False
                        converter.ignore_images = True
                        converter.body_width = 0
                        md = converter.handle(html).strip()
                        md_path = homepage_path(domain, output_dir)
                        md_path.write_text(f"# {row['name']} ({domain})\n\n{md}", encoding="utf-8")
                        fetched += 1
                    except Exception:
                        failed += 1
                else:
                    failed += 1

                # Record that we attempted the fetch so we don't retry every run
                conn.execute(
                    "UPDATE companies SET homepage_fetched_at = ? WHERE id = ?",
                    (now, company_id),
                )
                conn.commit()
                progress.advance(task)

    if failed:
        console.print(f"  [yellow]{failed} homepage(s) could not be fetched[/yellow]")
    return fetched


def _download_homepage(domain: str, timeout: int = 8) -> str | None:
    """Try to download the homepage for a domain. Returns HTML text or None on failure.

    Tries the bare domain first (HTTPS+HTTP in parallel), then falls back to
    www.{domain} if the bare domain fails.
    """
    result = _try_host(domain, timeout)
    if result is not None:
        return result

    # Fall back to www. prefix (skip if domain already starts with www.)
    if not domain.startswith("www."):
        return _try_host(f"www.{domain}", timeout)

    return None


def _try_host(host: str, timeout: int = 8) -> str | None:
    """Try HTTPS and HTTP for a host in parallel, return first successful HTML."""
    import threading, queue

    def _fetch(url: str, result_q: queue.Queue) -> None:
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; EmailManager/1.0)",
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                content_type = resp.headers.get("Content-Type", "")
                if "text/html" not in content_type and "xhtml" not in content_type:
                    result_q.put(None)
                    return

                raw = resp.read(512_000)  # cap at 512KB
                charset = "utf-8"
                if "charset=" in content_type:
                    charset = content_type.split("charset=")[-1].split(";")[0].strip()
                try:
                    result_q.put(raw.decode(charset, errors="replace"))
                except (LookupError, UnicodeDecodeError):
                    result_q.put(raw.decode("utf-8", errors="replace"))
        except Exception:
            result_q.put(None)

    hard_deadline = timeout + 2
    q: queue.Queue = queue.Queue()

    # Launch both schemes in parallel
    for scheme in ("https", "http"):
        url = f"{scheme}://{host}/"
        t = threading.Thread(target=_fetch, args=(url, q), daemon=True)
        t.start()

    # Wait for up to hard_deadline — take the first successful result
    deadline = time.monotonic() + hard_deadline
    results_seen = 0
    while results_seen < 2:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            result = q.get(timeout=remaining)
            results_seen += 1
            if result is not None:
                return result
        except queue.Empty:
            break

    return None
