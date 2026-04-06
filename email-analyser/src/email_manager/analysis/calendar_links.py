"""Link calendar events to discussions by attendee overlap and time proximity."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    # Handle both ISO datetime and date-only formats
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # Try fromisoformat as fallback
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _attendee_score(event_emails: set[str], disc_emails: set[str]) -> float:
    """Jaccard similarity between event attendees and discussion participants."""
    if not event_emails or not disc_emails:
        return 0.0
    intersection = event_emails & disc_emails
    union = event_emails | disc_emails
    return len(intersection) / len(union) if union else 0.0


def _time_score(event_start: datetime | None, disc_first: datetime | None, disc_last: datetime | None) -> float:
    """Score based on whether the event falls within or near the discussion's date range."""
    if not event_start or not (disc_first or disc_last):
        return 0.0

    # Strip timezone info for comparison if mixed
    evt = event_start.replace(tzinfo=None) if event_start.tzinfo else event_start
    first = disc_first.replace(tzinfo=None) if disc_first and disc_first.tzinfo else disc_first
    last = disc_last.replace(tzinfo=None) if disc_last and disc_last.tzinfo else disc_last

    if first and last:
        if first <= evt <= last:
            return 1.0
        # How far outside the range?
        if evt < first:
            days_away = (first - evt).days
        else:
            days_away = (evt - last).days
    elif first:
        days_away = abs((evt - first).days)
    else:
        days_away = abs((evt - last).days)

    if days_away <= 7:
        return 1.0 - (days_away / 14.0)
    elif days_away <= 30:
        return 0.3 - (0.3 * (days_away - 7) / 23.0)
    return 0.0


def link_calendar_events(
    conn: sqlite3.Connection,
    console: Console | None = None,
    limit: int | None = None,
) -> int:
    """Match calendar events to discussions. Returns number of links created."""
    if console is None:
        console = Console()

    # Load all calendar events
    events_sql = "SELECT id, event_id, start_time, attendees FROM calendar_events ORDER BY start_time DESC"
    if limit:
        events_sql += f" LIMIT {int(limit)}"
    events = conn.execute(events_sql).fetchall()

    if not events:
        console.print("  [dim]No calendar events to link.[/dim]")
        return 0

    # Load all discussions with their participants
    discussions = conn.execute(
        "SELECT id, participants, first_seen, last_seen FROM discussions"
    ).fetchall()

    if not discussions:
        console.print("  [dim]No discussions to link against.[/dim]")
        return 0

    # Pre-parse discussion data
    disc_data = []
    for d in discussions:
        try:
            participants = set(json.loads(d["participants"])) if d["participants"] else set()
        except (json.JSONDecodeError, TypeError):
            participants = set()
        disc_data.append({
            "id": d["id"],
            "participants": {e.lower() for e in participants},
            "first_seen": _parse_date(d["first_seen"]),
            "last_seen": _parse_date(d["last_seen"]),
        })

    links_created = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Linking calendar events", total=len(events))

        for evt in events:
            try:
                attendees_raw = json.loads(evt["attendees"]) if evt["attendees"] else []
            except (json.JSONDecodeError, TypeError):
                attendees_raw = []

            event_emails = {a["email"].lower() for a in attendees_raw if a.get("email")}
            event_start = _parse_date(evt["start_time"])

            best_match = None
            best_score = 0.0
            best_reason_parts = []

            for disc in disc_data:
                a_score = _attendee_score(event_emails, disc["participants"])
                t_score = _time_score(event_start, disc["first_seen"], disc["last_seen"])

                # Must have at least some attendee overlap
                if a_score == 0:
                    continue

                combined = 0.7 * a_score + 0.3 * t_score

                if combined > best_score and combined >= 0.3:
                    overlap_count = len(event_emails & disc["participants"])
                    best_score = combined
                    best_match = disc["id"]
                    best_reason_parts = [
                        f"{overlap_count} attendee overlap (Jaccard={a_score:.2f})",
                        f"time_score={t_score:.2f}",
                    ]

            if best_match is not None:
                conn.execute(
                    """INSERT OR REPLACE INTO discussion_events
                       (discussion_id, event_id, match_score, match_reason)
                       VALUES (?, ?, ?, ?)""",
                    (best_match, evt["id"], round(best_score, 3), ", ".join(best_reason_parts)),
                )
                links_created += 1

            progress.advance(task)

    conn.commit()
    console.print(f"  Linked {links_created} event(s) to discussions")
    return links_created
