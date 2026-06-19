"""Scheduler-specific configuration, separate from the core Config class."""
from dataclasses import dataclass


@dataclass
class SchedulerSettings:
    # Scheduling cadences (in minutes)
    ingest_interval_minutes: int = 10
    enrich_interval_minutes: int = 30
    ai_interval_minutes: int = 60

    # Prefect concurrency limit name for LLM-heavy tasks.
    # Create with: prefect concurrency-limit create ai-llm 3
    ai_concurrency_limit_name: str = "ai-llm"

    # Max companies processed per AI flow run.
    # Prevents one long run from blocking the next scheduled run.
    ai_company_batch_size: int = 50

    # Prefect work pool name (must match what the worker registers).
    work_pool: str = "email-manager-pool"

    # AI stages to run per-company in ai_flow.
    ai_stages: tuple[str, ...] = (
        "extract_events",
        "discover_discussions",
        "analyse_discussions",
        "propose_actions",
        "contact_memory",
    )


SETTINGS = SchedulerSettings()
