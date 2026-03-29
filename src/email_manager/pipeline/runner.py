from __future__ import annotations

import sqlite3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.ai.base import LLMBackend
from email_manager.ai.factory import get_backend
from email_manager.config import Config
from email_manager.db import get_db
from email_manager.pipeline.stages import ALL_STAGES


def run_pipeline(
    config: Config,
    stages: list[str] | None = None,
    console: Console | None = None,
) -> dict[str, int]:
    if console is None:
        console = Console()

    conn = get_db(config)
    backend = get_backend(config)

    stage_names = stages or list(ALL_STAGES.keys())
    results = {}

    console.print(f"Using AI backend: [bold]{backend.model_name}[/bold]")

    for stage_name in stage_names:
        if stage_name not in ALL_STAGES:
            console.print(f"[red]Unknown stage: {stage_name}[/red]")
            continue

        stage_fn = ALL_STAGES[stage_name]
        console.print(f"\n[bold]Running stage: {stage_name}[/bold]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=console,
        ) as progress:
            task = progress.add_task(stage_name, total=None)

            def on_progress(done: int, total: int) -> None:
                progress.update(task, completed=done, total=total)

            try:
                count = stage_fn(conn, backend, config, on_progress=on_progress)
                results[stage_name] = count
                console.print(f"  [green]{stage_name}: processed {count} items[/green]")
            except Exception as e:
                console.print(f"  [red]{stage_name} failed: {e}[/red]")
                results[stage_name] = -1

    conn.close()
    return results
