from __future__ import annotations

import inspect
import logging
import sqlite3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from email_manager.ai.base import LLMBackend
from email_manager.ai.factory import get_backend
from email_manager.config import Config
from email_manager.db import get_db
from email_manager.pipeline.stages import ALL_STAGES

logger = logging.getLogger("email_manager.pipeline")


def _setup_file_logging(config: Config) -> None:
    """Configure file logging for pipeline runs."""
    log_path = getattr(config, "log_file", None) or "email_manager.log"
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root = logging.getLogger("email_manager")
    # Avoid duplicate handlers on repeated calls
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == handler.baseFilename
               for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(logging.INFO)


def run_pipeline(
    config: Config,
    stages: list[str] | None = None,
    console: Console | None = None,
    limit: int | None = None,
    force: bool = False,
    clean: bool = False,
    company: str | None = None,
    label: str | None = None,
    exclude: list[str] | None = None,
    contact: str | None = None,
) -> dict[str, int]:
    if console is None:
        console = Console()

    _setup_file_logging(config)

    conn = get_db(config)

    stage_names = stages or list(ALL_STAGES.keys())
    results = {}

    # Only initialise AI backend if we need it (stages beyond extract_base)
    NO_AI_STAGES = {"extract_base", "fetch_homepages", "sync_calendar"}
    needs_ai = any(s not in NO_AI_STAGES for s in stage_names)
    backend = None
    if needs_ai:
        backend = get_backend(config)
        console.print(f"Using AI backend: [bold]{backend.model_name}[/bold]")
        logger.info("Using AI backend: %s", backend.model_name)
    else:
        console.print("Running non-AI stages only")
        logger.info("Running non-AI stages only")

    logger.info("Pipeline started — stages: %s", ", ".join(stage_names))

    for stage_name in stage_names:
        if stage_name not in ALL_STAGES:
            console.print(f"[red]Unknown stage: {stage_name}[/red]")
            logger.warning("Unknown stage: %s", stage_name)
            continue

        stage_fn = ALL_STAGES[stage_name]
        console.print(f"\n[bold]Running stage: {stage_name}[/bold]")
        logger.info("Starting stage: %s", stage_name)

        try:
            kwargs = dict(console=console, limit=limit, force=force)
            # Pass filtering options to stages that accept them
            sig = inspect.signature(stage_fn)
            if clean and "clean" in sig.parameters:
                kwargs["clean"] = True
            for opt_name, opt_val in [("company", company), ("label", label), ("exclude", exclude), ("contact", contact)]:
                if opt_name in sig.parameters and opt_val:
                    kwargs[opt_name] = opt_val
            count = stage_fn(conn, backend, config, **kwargs)
            results[stage_name] = count
            console.print(f"  [green]{stage_name}: processed {count} items[/green]")
            logger.info("Finished stage: %s — processed %d items", stage_name, count)
        except Exception as e:
            console.print(f"  [red]{stage_name} failed: {e}[/red]")
            logger.exception("Stage %s failed", stage_name)
            results[stage_name] = -1

    logger.info("Pipeline finished — results: %s", results)
    conn.close()
    return results
