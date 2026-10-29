"""Zoom integration — syncs meetings, AI summaries, and recordings.

The proxy swaps the placeholder Bearer token (ZOOM_TOKEN) for the real OAuth
token, so no local credentials are needed.

Confirmed working scopes:
  meeting:read:list_meetings                   — list scheduled/previous meetings (23 most recent non-recurring)
  meeting:read:meeting                         — get individual meeting details / past meeting stats
  cloud_recording:read:list_user_recordings    — list cloud recordings
  (user-scoped path for summaries works too)  — /users/me/meeting_summaries (no extra scope needed)

Additional scopes needed for full history:
  report:read:user     — full meeting history via date-range report (300+ meetings)
  meeting:read:summary — full AI summary content (overview, details, next_steps)

Tables created:
  zoom_meeting_notes  — one row per past meeting / AI summary
  zoom_recordings     — one row per cloud recording file
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterator

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

ZOOM_API = "https://api.zoom.us/v2"
DEFAULT_BEARER = "ZOOM_TOKEN"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS zoom_meeting_notes (
    id                  SERIAL PRIMARY KEY,
    meeting_uuid        TEXT UNIQUE NOT NULL,
    meeting_id          TEXT,
    host_email          TEXT,
    topic               TEXT,
    start_time          TIMESTAMPTZ,
    end_time            TIMESTAMPTZ,
    duration_mins       INTEGER,
    total_person_mins   INTEGER,
    participants_count  INTEGER,
    has_summary         BOOLEAN DEFAULT FALSE,
    notes_content       TEXT,
    summary_created_at  TIMESTAMPTZ,
    summary_title       TEXT,
    summary_overview    TEXT,
    summary_details     TEXT,
    next_steps          TEXT,
    audio_vtt_url       TEXT,
    join_url            TEXT,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS zoom_recordings (
    id              SERIAL PRIMARY KEY,
    meeting_uuid    TEXT NOT NULL,
    recording_id    TEXT UNIQUE NOT NULL,
    meeting_id      TEXT,
    host_email      TEXT,
    topic           TEXT,
    start_time      TIMESTAMPTZ,
    duration_mins   INTEGER,
    file_type       TEXT,
    file_size       BIGINT,
    status          TEXT,
    download_url    TEXT,
    play_url        TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class ZoomClient:
    def __init__(self, bearer_token: str = DEFAULT_BEARER):
        self.bearer = bearer_token

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = ZOOM_API + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.bearer}",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"Zoom API {exc.code} {path}: {body}") from exc

    def _paginate(self, path: str, key: str, params: dict | None = None) -> Iterator[dict]:
        params = dict(params or {})
        params.setdefault("page_size", 100)
        while True:
            data = self._get(path, params)
            for item in data.get(key, []):
                yield item
            token = data.get("next_page_token", "")
            if not token:
                break
            params["next_page_token"] = token
            time.sleep(0.1)

    def list_previous_meetings(self, user_id: str = "me") -> Iterator[dict]:
        """List past non-recurring scheduled meetings (up to ~30 most recent)."""
        yield from self._paginate(f"/users/{user_id}/meetings", "meetings", {"type": "previous_meetings"})

    def list_report_meetings(self, user_id: str = "me", from_date: str | None = None, to_date: str | None = None) -> Iterator[dict]:
        """Full meeting history via report API (requires report:read:user scope).

        Yields one dict per meeting instance across all time. Use date windows
        of ≤1 month to stay within Zoom's report API limits.
        """
        params: dict = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        yield from self._paginate(f"/report/users/{user_id}/meetings", "meetings", params)

    def get_past_meeting(self, meeting_id: int | str) -> dict | None:
        """Fetch actual start/end/participants for a past meeting occurrence."""
        try:
            return self._get(f"/past_meetings/{meeting_id}")
        except RuntimeError:
            return None

    def get_meeting_summary_by_uuid(self, uuid: str) -> dict | None:
        """Fetch full AI summary content for a meeting (requires meeting:read:summary scope).

        Zoom requires double URL-encoding for UUIDs that contain / or ==.
        """
        enc = urllib.parse.quote(urllib.parse.quote(uuid, safe=""), safe="")
        try:
            return self._get(f"/meetings/{enc}/meeting_summary")
        except RuntimeError:
            return None

    def list_user_meeting_summaries(self, user_id: str = "me") -> Iterator[dict]:
        """List all AI summary metadata for a user (no extra scope needed beyond meeting:read:list_meetings).

        Note: only returns metadata (topic, dates, title). Full content requires
        meeting:read:summary scope and get_meeting_summary_by_uuid().
        """
        yield from self._paginate(f"/users/{user_id}/meeting_summaries", "summaries")

    def list_recordings(self, user_id: str = "me", from_date: str | None = None, to_date: str | None = None) -> Iterator[dict]:
        """List cloud recordings; yields individual recording file objects."""
        params: dict = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        for meeting in self._paginate(f"/users/{user_id}/recordings", "meetings", params):
            for file in meeting.get("recording_files", []):
                file["_meeting_uuid"] = meeting.get("uuid", "")
                file["_meeting_id"] = str(meeting.get("id", ""))
                file["_host_email"] = meeting.get("host_email", "")
                file["_topic"] = meeting.get("topic", "")
                file["_start_time"] = meeting.get("start_time", "")
                file["_duration"] = meeting.get("duration", 0)
                yield file


def _ensure_tables(conn: Any) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def _iso(s: str | None) -> str | None:
    return s if s else None


def _upsert_meeting(conn: Any, uuid: str, mid: str, host: str, topic: str,
                    start_time: str | None, end_time: str | None,
                    duration_mins: int | None, total_person_mins: int | None,
                    participants_count: int | None, has_summary: bool,
                    notes_content: str | None, join_url: str | None,
                    summary_created_at: str | None, summary_title: str | None,
                    summary_overview: str | None, summary_details: str | None,
                    next_steps: str | None, audio_vtt_url: str | None) -> None:
    conn.execute(
        """
        INSERT INTO zoom_meeting_notes
            (meeting_uuid, meeting_id, host_email, topic,
             start_time, end_time, duration_mins, total_person_mins,
             participants_count, has_summary, notes_content,
             summary_created_at, summary_title, summary_overview,
             summary_details, next_steps, audio_vtt_url, join_url,
             fetched_at)
        VALUES
            (%(uuid)s, %(mid)s, %(host)s, %(topic)s,
             %(start)s, %(end)s, %(dur)s, %(tpm)s,
             %(pcount)s, %(has_sum)s, %(notes)s,
             %(sum_created)s, %(sum_title)s, %(sum_overview)s,
             %(sum_details)s, %(steps)s, %(vtt)s, %(join)s,
             NOW())
        ON CONFLICT (meeting_uuid) DO UPDATE SET
            topic               = EXCLUDED.topic,
            start_time          = EXCLUDED.start_time,
            end_time            = EXCLUDED.end_time,
            duration_mins       = EXCLUDED.duration_mins,
            total_person_mins   = EXCLUDED.total_person_mins,
            participants_count  = EXCLUDED.participants_count,
            has_summary         = EXCLUDED.has_summary,
            notes_content       = COALESCE(EXCLUDED.notes_content, zoom_meeting_notes.notes_content),
            summary_created_at  = EXCLUDED.summary_created_at,
            summary_title       = EXCLUDED.summary_title,
            summary_overview    = EXCLUDED.summary_overview,
            summary_details     = EXCLUDED.summary_details,
            next_steps          = EXCLUDED.next_steps,
            audio_vtt_url       = EXCLUDED.audio_vtt_url,
            join_url            = EXCLUDED.join_url,
            fetched_at          = NOW()
        """,
        {
            "uuid": uuid, "mid": mid, "host": host, "topic": topic,
            "start": _iso(start_time), "end": _iso(end_time),
            "dur": duration_mins, "tpm": total_person_mins,
            "pcount": participants_count, "has_sum": has_summary,
            "notes": notes_content,
            "sum_created": summary_created_at, "sum_title": summary_title,
            "sum_overview": summary_overview, "sum_details": summary_details,
            "steps": next_steps, "vtt": audio_vtt_url, "join": join_url,
        },
    )


def _fetch_notes_for_meeting(client: "ZoomClient", uuid: str) -> str | None:
    """Fetch inline notes for a meeting instance via /my_notes/notes."""
    try:
        data = client._get("/my_notes/notes", {"meeting_id": uuid})
        notes = data.get("notes", [])
        if not notes:
            return None
        # Concatenate all note blocks as plain text
        parts = []
        for note in notes:
            content = note.get("content") or note.get("body") or note.get("text") or ""
            if isinstance(content, list):
                content = "\n".join(str(c) for c in content)
            if content:
                parts.append(str(content))
        return "\n\n".join(parts) if parts else None
    except RuntimeError:
        return None


def sync_meetings(
    conn: Any,
    client: ZoomClient,
    from_date: str | None = None,
    to_date: str | None = None,
    console: Console | None = None,
) -> int:
    """Sync past meetings with notes, timing, and participant data.

    Strategy:
    1. If report:read:user:admin scope is available, uses the report API to get the
       full meeting history across all date windows (to/from), fetching notes for
       every instance. This gets the 300+ meetings.
    2. Falls back to the previous_meetings endpoint (last ~23 non-recurring meetings)
       when the report scope is absent.

    For each meeting, also fetches:
    - Inline notes via /my_notes/notes?meeting_id={uuid}
    - AI summary content if meeting:read:summary scope is available

    Returns count of upserted rows.
    """
    import datetime as dt

    _ensure_tables(conn)
    # Ensure notes_content column exists (added after initial schema creation)
    try:
        conn.execute("ALTER TABLE zoom_meeting_notes ADD COLUMN IF NOT EXISTS notes_content TEXT")
        conn.commit()
    except Exception:
        conn.commit()

    c = console or Console()

    # ── Try report API first (full history) ───────────────────────────────
    use_report = False
    if not from_date:
        from_date = "2020-01-01"
    if not to_date:
        to_date = dt.date.today().isoformat()

    # Probe whether report scope works
    try:
        # Use a narrow 1-day window just to test
        probe_to = dt.date.today().isoformat()
        probe_from = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        next(iter(client.list_report_meetings(from_date=probe_from, to_date=probe_to)), None)
        use_report = True
    except RuntimeError as exc:
        if "4711" in str(exc):
            c.print("  [yellow]report:read:user:admin scope not available — falling back to previous_meetings[/yellow]")
        else:
            raise

    upserted = 0

    if use_report:
        # Iterate month-by-month windows (Zoom report API max 1 month per request)
        start = dt.date.fromisoformat(from_date)
        end = dt.date.fromisoformat(to_date)
        windows: list[tuple[str, str]] = []
        cur = start
        while cur <= end:
            win_end = min(cur + dt.timedelta(days=30), end)
            windows.append((cur.isoformat(), win_end.isoformat()))
            cur = win_end + dt.timedelta(days=1)

        c.print(f"  Using report API: {len(windows)} monthly windows from {from_date} → {to_date}")

        with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                      BarColumn(), TextColumn("{task.completed} meetings"), console=c, transient=True) as progress:
            task = progress.add_task("Syncing meeting history…", total=None)

            for win_from, win_to in windows:
                for meeting in client.list_report_meetings(from_date=win_from, to_date=win_to):
                    uuid = meeting.get("uuid", "")
                    mid = str(meeting.get("id", ""))
                    if not uuid:
                        continue

                    notes_content = _fetch_notes_for_meeting(client, uuid)

                    _upsert_meeting(
                        conn, uuid, mid,
                        host=meeting.get("host_email", ""),
                        topic=meeting.get("topic", ""),
                        start_time=meeting.get("start_time"),
                        end_time=meeting.get("end_time"),
                        duration_mins=meeting.get("duration"),
                        total_person_mins=meeting.get("total_minutes"),
                        participants_count=meeting.get("participants_count"),
                        has_summary=bool(meeting.get("has_meeting_summary", False)),
                        notes_content=notes_content,
                        join_url=None,
                        summary_created_at=None, summary_title=None,
                        summary_overview=None, summary_details=None,
                        next_steps=None, audio_vtt_url=None,
                    )
                    upserted += 1
                    progress.update(task, completed=upserted)
                    time.sleep(0.05)

                conn.commit()

    else:
        # Fallback: previous_meetings endpoint
        meetings = list(client.list_previous_meetings())
        c.print(f"  Found {len(meetings)} past meetings (limited — add report:read:user:admin for full history)")

        with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                      BarColumn(), TextColumn("{task.completed}/{task.total}"), console=c, transient=True) as progress:
            task = progress.add_task("Fetching meeting details…", total=len(meetings))

            for sched in meetings:
                mid = sched.get("id")
                if not mid:
                    progress.advance(task)
                    continue

                past = client.get_past_meeting(mid) or {}
                uuid = past.get("uuid") or sched.get("uuid", "")
                if not uuid:
                    progress.advance(task)
                    continue

                notes_content = _fetch_notes_for_meeting(client, uuid)
                has_summary = bool(past.get("has_meeting_summary", False))

                summary_title = summary_overview = summary_details = next_steps = summary_created_at = audio_vtt_url = None
                if has_summary:
                    s = client.get_meeting_summary_by_uuid(uuid) or {}
                    if s and not s.get("code"):
                        edited = s.get("edited_summary") or {}
                        summary_title = edited.get("summary_title") or s.get("summary_title")
                        summary_overview = edited.get("summary_overview") or s.get("summary_overview")
                        details = s.get("summary_details") or []
                        steps = s.get("next_steps") or []
                        summary_details = json.dumps(details) if details else None
                        next_steps = json.dumps(steps) if steps else None
                        summary_created_at = _iso(s.get("summary_created_time"))
                        audio_vtt_url = s.get("audio_vtt_url")

                _upsert_meeting(
                    conn, uuid, str(mid),
                    host=past.get("user_email") or "",
                    topic=sched.get("topic", ""),
                    start_time=past.get("start_time") or sched.get("start_time"),
                    end_time=past.get("end_time"),
                    duration_mins=past.get("duration") or sched.get("duration"),
                    total_person_mins=past.get("total_minutes"),
                    participants_count=past.get("participants_count"),
                    has_summary=has_summary,
                    notes_content=notes_content,
                    join_url=sched.get("join_url"),
                    summary_created_at=summary_created_at, summary_title=summary_title,
                    summary_overview=summary_overview, summary_details=summary_details,
                    next_steps=next_steps, audio_vtt_url=audio_vtt_url,
                )
                upserted += 1
                progress.update(task, completed=upserted)
                time.sleep(0.05)

            conn.commit()

    # ── Also sync all AI summaries (metadata + content where available) ──
    c.print("  Syncing AI meeting summaries…")
    summary_upserted = 0
    for s in client.list_user_meeting_summaries():
        uuid = s.get("meeting_uuid", "")
        mid = str(s.get("meeting_id", ""))
        if not uuid:
            continue

        # Try to get full content
        full = client.get_meeting_summary_by_uuid(uuid) or {}
        edited = full.get("edited_summary") or {}
        summary_title = edited.get("summary_title") or full.get("summary_title") or s.get("summary_title")
        summary_overview = edited.get("summary_overview") or full.get("summary_overview")
        details = full.get("summary_details") or []
        steps = full.get("next_steps") or []

        _upsert_meeting(
            conn, uuid, mid,
            host=s.get("meeting_host_email", ""),
            topic=s.get("meeting_topic", ""),
            start_time=s.get("meeting_start_time"),
            end_time=s.get("meeting_end_time"),
            duration_mins=None, total_person_mins=None, participants_count=None,
            has_summary=True,
            notes_content=None,
            join_url=None,
            summary_created_at=_iso(s.get("summary_created_time")),
            summary_title=summary_title,
            summary_overview=summary_overview,
            summary_details=json.dumps(details) if details else None,
            next_steps=json.dumps(steps) if steps else None,
            audio_vtt_url=full.get("audio_vtt_url"),
        )
        summary_upserted += 1
        time.sleep(0.05)

    conn.commit()
    if summary_upserted:
        c.print(f"  AI summaries: {summary_upserted} upserted")

    c.print(f"[green]✓[/green] Meetings: {upserted} synced")
    return upserted


def sync_recordings(
    conn: Any,
    client: ZoomClient,
    from_date: str | None = None,
    to_date: str | None = None,
    console: Console | None = None,
) -> int:
    """Sync Zoom cloud recording metadata. Returns count of upserted rows."""
    _ensure_tables(conn)
    c = console or Console()

    upserted = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed} synced"),
        console=c,
        transient=True,
    ) as progress:
        task = progress.add_task("Fetching recordings…", total=None)

        for file in client.list_recordings(from_date=from_date, to_date=to_date):
            rid = file.get("id", "")
            if not rid:
                continue

            conn.execute(
                """
                INSERT INTO zoom_recordings
                    (meeting_uuid, recording_id, meeting_id, host_email, topic,
                     start_time, duration_mins, file_type, file_size, status,
                     download_url, play_url, fetched_at)
                VALUES
                    (%(uuid)s, %(rid)s, %(mid)s, %(host)s, %(topic)s,
                     %(start)s, %(dur)s, %(ftype)s, %(fsize)s, %(status)s,
                     %(dl)s, %(play)s, NOW())
                ON CONFLICT (recording_id) DO UPDATE SET
                    status       = EXCLUDED.status,
                    file_size    = EXCLUDED.file_size,
                    download_url = EXCLUDED.download_url,
                    play_url     = EXCLUDED.play_url,
                    fetched_at   = NOW()
                """,
                {
                    "uuid": file.get("_meeting_uuid", ""),
                    "rid": rid,
                    "mid": file.get("_meeting_id", ""),
                    "host": file.get("_host_email", ""),
                    "topic": file.get("_topic", ""),
                    "start": _iso(file.get("_start_time")),
                    "dur": file.get("_duration"),
                    "ftype": file.get("file_type", ""),
                    "fsize": file.get("file_size"),
                    "status": file.get("status", ""),
                    "dl": file.get("download_url", ""),
                    "play": file.get("play_url", ""),
                },
            )
            upserted += 1
            progress.update(task, completed=upserted)

        conn.commit()

    c.print(f"[green]✓[/green] Recordings: {upserted} synced")
    return upserted
