from __future__ import annotations

import hashlib
import sqlite3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.ai.base import LLMBackend
from email_manager.db import fetchall, fetchone
from email_manager.memory.base import ContactMemory, MemoryBackend, MemoryStrategy


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    )


def build_contact_memories(
    conn: sqlite3.Connection,
    ai_backend: LLMBackend,
    memory_backends: list[MemoryBackend],
    strategy: MemoryStrategy,
    email_address: str | None = None,
    company_domain: str | None = None,
    console: Console | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    if console is None:
        console = Console()

    if email_address:
        contacts = fetchall(
            conn, "SELECT email, name, email_count FROM contacts WHERE email = ?", (email_address,)
        )
    elif company_domain:
        contacts = fetchall(
            conn,
            """SELECT ct.email, ct.name, ct.email_count
               FROM contacts ct
               JOIN company_contacts cc ON cc.contact_email = ct.email
               JOIN companies c ON cc.company_id = c.id
               WHERE c.domain = ? COLLATE NOCASE AND ct.email_count > 0
               ORDER BY ct.email_count DESC""",
            (company_domain,),
        )
        if limit:
            contacts = contacts[:limit]
    else:
        sql = "SELECT email, name, email_count FROM contacts WHERE email_count > 0 ORDER BY email_count DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        contacts = fetchall(conn, sql)

    if not contacts:
        console.print("[dim]No contacts to process.[/dim]")
        return 0

    processed = 0

    with _make_progress(console) as progress:
        task = progress.add_task("Building contact memories", total=len(contacts))

        for contact in contacts:
            addr = contact["email"]

            # Incremental check: skip if emails haven't changed and strategy is the same
            if not force:
                current_hash = _compute_emails_hash(conn, addr)
                existing = memory_backends[0].load(addr) if memory_backends else None
                if existing and existing.emails_hash == current_hash and existing.strategy_used == strategy.name:
                    progress.advance(task)
                    continue

            try:
                memory = strategy.generate(conn, ai_backend, addr)
                memory.emails_hash = _compute_emails_hash(conn, addr)

                # Check if this is an update
                existing = memory_backends[0].load(addr) if memory_backends else None
                if existing:
                    memory.version = existing.version + 1

                for backend in memory_backends:
                    backend.store(memory)

                processed += 1
            except Exception as e:
                console.print(f"  [red]Failed for {addr}: {e}[/red]")

            progress.advance(task)

    return processed


def _compute_emails_hash(conn: sqlite3.Connection, email_address: str) -> str:
    rows = fetchall(
        conn,
        """SELECT id FROM emails
           WHERE from_address = ? OR to_addresses LIKE ? OR cc_addresses LIKE ?
           ORDER BY id""",
        (email_address, f'%"{email_address}"%', f'%"{email_address}"%'),
    )
    ids = ",".join(str(r["id"]) for r in rows)
    return hashlib.md5(ids.encode()).hexdigest()
