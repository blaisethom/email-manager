"""HubSpot CRM integration.

Pulls companies and contacts from HubSpot into the local DB. The proxy swaps
the placeholder Bearer token (`HUBSPOT_TOKEN`) for the real OAuth token, so
no local credentials are needed.

Data lands in dedicated `hubspot_*` tables rather than merging into the
existing `companies`/`contacts` tables — HubSpot is a separate source of
truth and we want to preserve provenance. Linking to email-derived
companies/contacts can be layered on top later via domain/email keys.
"""

from __future__ import annotations

import http.client
import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

HUBSPOT_API = "https://api.hubapi.com"

# Default placeholder — the proxy intercepts this and substitutes the real token.
DEFAULT_BEARER = "HUBSPOT_TOKEN"

# Properties we materialise as columns. Everything else lands in properties_json.
COMPANY_PROPERTIES = [
    "name", "domain", "website", "industry", "description", "about_us",
    "city", "state", "country", "phone", "numberofemployees", "annualrevenue",
    "lifecyclestage", "type", "hubspot_owner_id", "founded_year",
    "linkedin_company_page", "twitterhandle",
    "createdate", "hs_lastmodifieddate",
]

CONTACT_PROPERTIES = [
    "email", "firstname", "lastname", "company", "jobtitle", "phone",
    "city", "state", "country", "address",
    "lifecyclestage", "hs_lead_status", "hubspot_owner_id",
    "twitterhandle", "linkedin", "website", "industry", "salutation",
    "createdate", "lastmodifieddate",
]

TASK_PROPERTIES = [
    "hs_task_subject", "hs_task_body", "hs_task_status", "hs_task_type",
    "hs_task_priority", "hs_timestamp", "hs_task_completion_date",
    "hubspot_owner_id", "hs_queue_membership_ids",
    "createdate", "hs_lastmodifieddate",
]


def _hubspot_url(portal_id: int | str, object_type: str, record_id: str, ui_domain: str = "app.hubspot.com") -> str:
    """Construct a deep-link URL to a HubSpot record.

    `object_type` is HubSpot's object-type code: `0-2` for companies, `0-1`
    for contacts. The portal ID is account-specific and constant per portal.
    """
    return f"https://{ui_domain}/contacts/{portal_id}/record/{object_type}/{record_id}"


class HubSpotClient:
    def __init__(self, bearer_token: str = DEFAULT_BEARER):
        self.bearer = bearer_token

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = HUBSPOT_API + path
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.bearer}",
                "Accept": "application/json",
            },
        )
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                # Retry on rate-limit and transient server errors
                if e.code in (429, 500, 502, 503, 504) and attempt < 4:
                    delay = 2 ** attempt
                    time.sleep(delay)
                    continue
                body = e.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"HubSpot {e.code} {e.reason}: {body}") from e
            except (urllib.error.URLError, http.client.IncompleteRead, http.client.RemoteDisconnected) as e:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                raise
        raise RuntimeError("HubSpot request failed after retries")

    def _post(self, path: str, body: dict) -> dict:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            HUBSPOT_API + path,
            data=data,
            headers={
                "Authorization": f"Bearer {self.bearer}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504) and attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                body_txt = e.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"HubSpot {e.code} {e.reason}: {body_txt}") from e
            except (urllib.error.URLError, http.client.IncompleteRead, http.client.RemoteDisconnected) as e:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                raise
        raise RuntimeError("HubSpot request failed after retries")

    def account_info(self) -> dict:
        """Fetch portal info: portalId, uiDomain, etc."""
        return self._get("/account-info/v3/details")

    def get_owner_id(self, email: str) -> str | None:
        """Look up a HubSpot owner ID by email address."""
        data = self._get("/crm/v3/owners", {"email": email, "limit": 1})
        results = data.get("results", [])
        return str(results[0]["id"]) if results else None

    def search_tasks(
        self,
        owner_id: str,
        status: str | None = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Search tasks assigned to an owner, newest-due first."""
        filters: list[dict] = [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id}
        ]
        if status:
            filters.append({"propertyName": "hs_task_status", "operator": "EQ", "value": status})

        after: str | None = None
        while True:
            body: dict[str, Any] = {
                "filterGroups": [{"filters": filters}],
                "properties": TASK_PROPERTIES,
                "associations": ["contacts", "companies"],
                "limit": page_size,
                "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
            }
            if after:
                body["after"] = after
            data = self._post("/crm/v3/objects/tasks/search", body)
            for r in data.get("results", []):
                yield r
            nxt = (data.get("paging") or {}).get("next") or {}
            after = nxt.get("after")
            if not after:
                return

    def batch_get_associations(
        self,
        from_type: str,
        to_type: str,
        ids: list[str],
        chunk_size: int = 100,
    ) -> dict[str, list[str]]:
        """Return {from_id: [to_id, ...]} for all given IDs using the v4 batch API."""
        result: dict[str, list[str]] = {i: [] for i in ids}
        for offset in range(0, len(ids), chunk_size):
            chunk = ids[offset : offset + chunk_size]
            data = self._post(
                f"/crm/v4/associations/{from_type}/{to_type}/batch/read",
                {"inputs": [{"id": i} for i in chunk]},
            )
            for item in data.get("results", []):
                from_id = str(item["from"]["id"])
                to_ids = [str(t["toObjectId"]) for t in item.get("to", [])]
                result[from_id] = to_ids
        return result

    def batch_read(
        self,
        object_type: str,
        ids: list[str],
        properties: list[str],
        chunk_size: int = 100,
    ) -> list[dict]:
        """Batch-read objects by ID. Returns list of raw object dicts."""
        results = []
        for offset in range(0, len(ids), chunk_size):
            chunk = ids[offset : offset + chunk_size]
            data = self._post(
                f"/crm/v3/objects/{object_type}/batch/read",
                {"inputs": [{"id": i} for i in chunk], "properties": properties},
            )
            results.extend(data.get("results", []))
        return results

    def iter_objects(
        self,
        object_type: str,
        properties: list[str],
        associations: list[str] | None = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Yield CRM objects of the given type, transparently paginating."""
        after: str | None = None
        while True:
            params: dict[str, Any] = {
                "limit": page_size,
                "properties": ",".join(properties),
                "archived": "false",
            }
            if associations:
                params["associations"] = ",".join(associations)
            if after:
                params["after"] = after
            data = self._get(f"/crm/v3/objects/{object_type}", params)
            for r in data.get("results", []):
                yield r
            nxt = (data.get("paging") or {}).get("next") or {}
            after = nxt.get("after")
            if not after:
                return


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ms_to_iso(ms_str: str | None) -> str | None:
    """Convert a HubSpot millisecond-epoch timestamp string to ISO-8601."""
    if not ms_str:
        return None
    try:
        ms = int(float(ms_str))
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, OSError):
        return None


def _to_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm(s: str | None) -> str | None:
    if not s:
        return None
    return s.strip().lower() or None


def _upsert_company(conn: sqlite3.Connection, obj: dict, fetched_at: str) -> str:
    p = obj.get("properties") or {}
    conn.execute(
        """INSERT INTO hubspot_companies (
            id, name, domain, website, industry, description, about_us,
            city, state, country, phone, num_employees, annual_revenue,
            lifecycle_stage, type, owner_id, founded_year,
            linkedin_url, twitter_handle, hs_created_at, hs_updated_at,
            hs_url, properties_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name, domain=excluded.domain, website=excluded.website,
            industry=excluded.industry, description=excluded.description,
            about_us=excluded.about_us, city=excluded.city, state=excluded.state,
            country=excluded.country, phone=excluded.phone,
            num_employees=excluded.num_employees, annual_revenue=excluded.annual_revenue,
            lifecycle_stage=excluded.lifecycle_stage, type=excluded.type,
            owner_id=excluded.owner_id, founded_year=excluded.founded_year,
            linkedin_url=excluded.linkedin_url, twitter_handle=excluded.twitter_handle,
            hs_created_at=excluded.hs_created_at, hs_updated_at=excluded.hs_updated_at,
            hs_url=excluded.hs_url,
            properties_json=excluded.properties_json, fetched_at=excluded.fetched_at
        """,
        (
            obj["id"],
            p.get("name"),
            _norm(p.get("domain")),
            p.get("website"),
            p.get("industry"),
            p.get("description"),
            p.get("about_us"),
            p.get("city"),
            p.get("state"),
            p.get("country"),
            p.get("phone"),
            _to_int(p.get("numberofemployees")),
            _to_float(p.get("annualrevenue")),
            p.get("lifecyclestage"),
            p.get("type"),
            p.get("hubspot_owner_id"),
            p.get("founded_year"),
            p.get("linkedin_company_page"),
            p.get("twitterhandle"),
            p.get("createdate"),
            p.get("hs_lastmodifieddate"),
            obj.get("url"),
            json.dumps(p),
            fetched_at,
        ),
    )
    return obj["id"]


def _upsert_contact(conn: sqlite3.Connection, obj: dict, fetched_at: str) -> str:
    p = obj.get("properties") or {}
    conn.execute(
        """INSERT INTO hubspot_contacts (
            id, email, firstname, lastname, company_name, job_title, phone,
            city, state, country, address,
            lifecycle_stage, lead_status, owner_id,
            twitter_handle, linkedin_url, website, industry, salutation,
            hs_created_at, hs_updated_at, hs_url, properties_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            email=excluded.email, firstname=excluded.firstname, lastname=excluded.lastname,
            company_name=excluded.company_name, job_title=excluded.job_title, phone=excluded.phone,
            city=excluded.city, state=excluded.state, country=excluded.country, address=excluded.address,
            lifecycle_stage=excluded.lifecycle_stage, lead_status=excluded.lead_status,
            owner_id=excluded.owner_id, twitter_handle=excluded.twitter_handle,
            linkedin_url=excluded.linkedin_url, website=excluded.website,
            industry=excluded.industry, salutation=excluded.salutation,
            hs_created_at=excluded.hs_created_at, hs_updated_at=excluded.hs_updated_at,
            hs_url=excluded.hs_url,
            properties_json=excluded.properties_json, fetched_at=excluded.fetched_at
        """,
        (
            obj["id"],
            _norm(p.get("email")),
            p.get("firstname"),
            p.get("lastname"),
            p.get("company"),
            p.get("jobtitle"),
            p.get("phone"),
            p.get("city"),
            p.get("state"),
            p.get("country"),
            p.get("address"),
            p.get("lifecyclestage"),
            p.get("hs_lead_status"),
            p.get("hubspot_owner_id"),
            p.get("twitterhandle"),
            p.get("linkedin"),
            p.get("website"),
            p.get("industry"),
            p.get("salutation"),
            p.get("createdate"),
            p.get("lastmodifieddate"),
            obj.get("url"),
            json.dumps(p),
            fetched_at,
        ),
    )
    return obj["id"]


def _record_company_associations(conn: sqlite3.Connection, company_id: str, obj: dict) -> int:
    """Persist company→contact associations. Returns number of links written."""
    assocs = ((obj.get("associations") or {}).get("contacts") or {}).get("results") or []
    # Dedupe — HubSpot returns labeled + unlabeled rows for the same pairing.
    seen: set[str] = set()
    rows = []
    for a in assocs:
        cid = a.get("id")
        if cid and cid not in seen:
            seen.add(cid)
            rows.append((company_id, cid))
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO hubspot_company_contacts (company_id, contact_id) VALUES (?, ?)",
            rows,
        )
    return len(rows)


def _update_sync_state(conn: sqlite3.Connection, object_type: str, count: int) -> None:
    now = _now_iso()
    conn.execute(
        """INSERT INTO hubspot_sync_state (object_type, last_sync_at, last_full_sync_at, record_count)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(object_type) DO UPDATE SET
               last_sync_at=excluded.last_sync_at,
               last_full_sync_at=excluded.last_full_sync_at,
               record_count=excluded.record_count
        """,
        (object_type, now, now, count),
    )


def backfill_urls(
    conn: sqlite3.Connection,
    client: HubSpotClient,
    *,
    console: Console | None = None,
) -> tuple[int, int]:
    """Fill `hs_url` for existing rows using portal info from /account-info.

    HubSpot returns a `url` field on each object during sync, but rows
    inserted before v32 schema migration won't have it. This backfill
    constructs URLs using the portal ID and HubSpot's object-type codes
    (`0-2` companies, `0-1` contacts). Idempotent — only writes where
    `hs_url IS NULL`.
    """
    console = console or Console()
    info = client.account_info()
    portal_id = info.get("portalId")
    ui_domain = info.get("uiDomain") or "app.hubspot.com"
    if not portal_id:
        raise RuntimeError(f"HubSpot account-info did not return portalId: {info!r}")

    # Build URLs for everything missing one. Cheap UPDATE — single statement.
    co_cur = conn.execute(
        f"""UPDATE hubspot_companies
            SET hs_url = 'https://{ui_domain}/contacts/{portal_id}/record/0-2/' || id
            WHERE hs_url IS NULL"""
    )
    co_n = co_cur.rowcount if hasattr(co_cur, "rowcount") else 0
    ct_cur = conn.execute(
        f"""UPDATE hubspot_contacts
            SET hs_url = 'https://{ui_domain}/contacts/{portal_id}/record/0-1/' || id
            WHERE hs_url IS NULL"""
    )
    ct_n = ct_cur.rowcount if hasattr(ct_cur, "rowcount") else 0
    conn.commit()
    console.print(f"  Portal: [bold]{portal_id}[/bold] @ {ui_domain}")
    console.print(f"  Backfilled URLs: [green]{co_n}[/green] companies, [green]{ct_n}[/green] contacts")
    return int(co_n), int(ct_n)


def sync_companies(
    conn: sqlite3.Connection,
    client: HubSpotClient,
    *,
    limit: int | None = None,
    console: Console | None = None,
) -> tuple[int, int]:
    """Sync companies (and their contact associations). Returns (companies, links)."""
    from email_manager.entities.reconcile import SOURCE_HUBSPOT, link_or_create_org

    console = console or Console()
    fetched_at = _now_iso()
    n_companies = 0
    n_links = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed} fetched"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("HubSpot companies", total=None)
        for obj in client.iter_objects(
            "companies", COMPANY_PROPERTIES, associations=["contacts"]
        ):
            cid = _upsert_company(conn, obj, fetched_at)
            n_links += _record_company_associations(conn, cid, obj)
            props = obj.get("properties") or {}
            link_or_create_org(
                conn,
                source=SOURCE_HUBSPOT,
                source_id=cid,
                domain=props.get("domain"),
                name=props.get("name"),
            )
            # Propagate HubSpot name to companies.name unless human has set it
            hs_name = props.get("name")
            hs_domain = props.get("domain")
            if hs_name and hs_domain:
                conn.execute(
                    """UPDATE companies SET name = ?, name_source = 'hubspot'
                       WHERE LOWER(domain) = LOWER(?)
                         AND (name_source IS NULL OR name_source != 'human')""",
                    (hs_name, hs_domain),
                )
            n_companies += 1
            if n_companies % 50 == 0:
                conn.commit()
            progress.update(task, advance=1)
            if limit and n_companies >= limit:
                break
    _update_sync_state(conn, "companies", n_companies)
    conn.commit()
    return n_companies, n_links


def sync_contacts(
    conn: sqlite3.Connection,
    client: HubSpotClient,
    *,
    limit: int | None = None,
    console: Console | None = None,
) -> int:
    """Sync contacts. Returns contact count."""
    from email_manager.entities.reconcile import SOURCE_HUBSPOT, link_or_create_person

    console = console or Console()
    fetched_at = _now_iso()
    n = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed} fetched"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("HubSpot contacts", total=None)
        for obj in client.iter_objects("contacts", CONTACT_PROPERTIES):
            _upsert_contact(conn, obj, fetched_at)
            props = obj.get("properties") or {}
            full_name = " ".join(
                x for x in (props.get("firstname"), props.get("lastname")) if x
            ).strip() or None
            link_or_create_person(
                conn,
                source=SOURCE_HUBSPOT,
                source_id=obj["id"],
                email=props.get("email"),
                name=full_name,
            )
            n += 1
            if n % 100 == 0:
                conn.commit()
            progress.update(task, advance=1)
            if limit and n >= limit:
                break
    _update_sync_state(conn, "contacts", n)
    conn.commit()
    return n


def _upsert_task(
    conn: sqlite3.Connection,
    obj: dict,
    fetched_at: str,
    portal_id: str | None = None,
    ui_domain: str = "app.hubspot.com",
) -> str:
    p = obj.get("properties") or {}
    hs_url = obj.get("url")
    if not hs_url and portal_id:
        # HubSpot tasks use object-type code 0-27
        hs_url = f"https://{ui_domain}/contacts/{portal_id}/record/0-27/{obj['id']}"

    assocs = obj.get("associations") or {}
    contact_ids = list({
        a["id"] for a in (assocs.get("contacts") or {}).get("results", [])
    })
    company_ids = list({
        a["id"] for a in (assocs.get("companies") or {}).get("results", [])
    })

    conn.execute(
        """INSERT INTO hubspot_tasks (
            id, subject, body, status, type, priority,
            due_date, completed_at, owner_id,
            associated_contact_ids, associated_company_ids,
            hs_url, properties_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            subject=excluded.subject, body=excluded.body,
            status=excluded.status, type=excluded.type, priority=excluded.priority,
            due_date=excluded.due_date, completed_at=excluded.completed_at,
            owner_id=excluded.owner_id,
            associated_contact_ids=excluded.associated_contact_ids,
            associated_company_ids=excluded.associated_company_ids,
            hs_url=excluded.hs_url,
            properties_json=excluded.properties_json, fetched_at=excluded.fetched_at
        """,
        (
            obj["id"],
            p.get("hs_task_subject"),
            p.get("hs_task_body"),
            p.get("hs_task_status"),
            p.get("hs_task_type"),
            p.get("hs_task_priority"),
            _ms_to_iso(p.get("hs_timestamp")),
            _ms_to_iso(p.get("hs_task_completion_date")),
            p.get("hubspot_owner_id"),
            json.dumps(contact_ids),
            json.dumps(company_ids),
            hs_url,
            json.dumps(p),
            fetched_at,
        ),
    )
    return obj["id"]


def enrich_tasks_with_threads(
    conn: sqlite3.Connection,
    *,
    force: bool = False,
    console: Console | None = None,
) -> int:
    """Link HubSpot tasks to email threads via associated contact emails.

    For each task, resolve contact emails from hubspot_contacts and find
    matching threads in the emails table. Results land in hubspot_task_threads.
    Returns the number of (task_id, thread_id) links written.
    """
    from email_manager.db import fetchall, fetchone

    console = console or Console()

    tasks = fetchall(
        conn,
        """SELECT id, associated_contact_ids
           FROM hubspot_tasks
           WHERE associated_contact_ids IS NOT NULL AND associated_contact_ids != '[]'""",
    )
    if not tasks:
        console.print("  [dim]No tasks with associated contacts to enrich.[/dim]")
        return 0

    if force:
        conn.execute("DELETE FROM hubspot_task_threads")
        conn.commit()

    n_links = 0
    n_tasks = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} tasks"),
        console=console,
        transient=True,
    ) as progress:
        bar = progress.add_task("Enriching tasks", total=len(tasks))
        for task in tasks:
            try:
                contact_ids: list[str] = json.loads(task["associated_contact_ids"] or "[]")
            except (ValueError, TypeError):
                contact_ids = []

            task_links = 0
            for contact_id in contact_ids:
                contact = fetchone(
                    conn, "SELECT email FROM hubspot_contacts WHERE id = ?", (contact_id,)
                )
                if not contact or not contact["email"]:
                    continue
                email = contact["email"].lower().strip()
                like = f"%{email}%"
                threads = fetchall(
                    conn,
                    """SELECT DISTINCT thread_id FROM emails
                       WHERE LOWER(from_address) = ?
                          OR LOWER(to_addresses) LIKE ?
                          OR LOWER(cc_addresses) LIKE ?
                       LIMIT 200""",
                    (email, like, like),
                )
                for thread in threads:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO hubspot_task_threads (task_id, thread_id, contact_email) VALUES (?, ?, ?)",
                            (task["id"], thread["thread_id"], email),
                        )
                        task_links += 1
                    except Exception:
                        pass

            n_links += task_links
            n_tasks += 1
            if n_tasks % 20 == 0:
                conn.commit()
            progress.update(bar, advance=1)

    conn.commit()
    console.print(f"  [green]Linked {n_links} thread(s) across {n_tasks} task(s)[/green]")
    return n_links


def sync_tasks(
    conn: sqlite3.Connection,
    client: HubSpotClient,
    owner_email: str,
    *,
    open_only: bool = False,
    limit: int | None = None,
    console: Console | None = None,
) -> int:
    """Sync HubSpot tasks assigned to the given owner email. Returns task count."""
    console = console or Console()

    owner_id = client.get_owner_id(owner_email)
    if not owner_id:
        raise RuntimeError(f"No HubSpot owner found for email: {owner_email!r}")
    console.print(f"  HubSpot owner: [bold]{owner_email}[/bold] (id {owner_id})")

    try:
        info = client.account_info()
        portal_id = str(info.get("portalId") or "")
        ui_domain = info.get("uiDomain") or "app.hubspot.com"
    except Exception:
        portal_id = ""
        ui_domain = "app.hubspot.com"

    fetched_at = _now_iso()
    n = 0
    task_ids: list[str] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed} fetched"),
        console=console,
        transient=True,
    ) as progress:
        bar = progress.add_task("HubSpot tasks", total=None)
        for obj in client.search_tasks(owner_id):
            _upsert_task(conn, obj, fetched_at, portal_id=portal_id or None, ui_domain=ui_domain)
            task_ids.append(obj["id"])
            n += 1
            if n % 50 == 0:
                conn.commit()
            progress.update(bar, advance=1)
            if limit and n >= limit:
                break

    # The CRM search API silently drops associations from results; fetch them
    # separately via the v4 batch associations endpoint.
    if task_ids:
        console.print(f"  Fetching associations for {len(task_ids)} tasks…")
        contact_assocs = client.batch_get_associations("tasks", "contacts", task_ids)
        company_assocs = client.batch_get_associations("tasks", "companies", task_ids)
        for tid in task_ids:
            conn.execute(
                "UPDATE hubspot_tasks SET associated_contact_ids=?, associated_company_ids=? WHERE id=?",
                (
                    json.dumps(contact_assocs.get(tid, [])),
                    json.dumps(company_assocs.get(tid, [])),
                    tid,
                ),
            )
        conn.commit()

    _update_sync_state(conn, "tasks", n)
    conn.commit()
    return n


EMAIL_ENGAGEMENT_PROPERTIES = [
    "hs_email_subject",
    "hs_email_text",
    "hs_email_html",
    "hs_email_direction",
    "hs_timestamp",
    "hs_email_from_email",
    "hs_email_from_firstname",
    "hs_email_from_lastname",
    "hs_email_to_email",
    "hs_email_cc_email",
    "hs_email_status",
    "hs_email_message_id",
    "hs_email_headers",
    "createdate",
    "hs_lastmodifieddate",
]


def _parse_hs_message_id(raw: str | None) -> str | None:
    """Normalise a HubSpot Message-ID to RFC822 <id@domain> format."""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if not raw.startswith("<"):
        raw = f"<{raw}"
    if not raw.endswith(">"):
        raw = f"{raw}>"
    return raw


def _upsert_hubspot_email(
    conn: sqlite3.Connection,
    obj: dict,
    fetched_at: str,
) -> bool:
    """Insert a HubSpot email engagement into the main emails table.

    Returns True if a new row was inserted, False if it already existed.
    Uses the real RFC822 Message-ID when HubSpot provides it (hs_email_message_id),
    falling back to 'hs:<id>' only when absent. Using real IDs lets HubSpot emails
    thread with Gmail/IMAP emails via References chains, and deduplicates emails
    that exist in both sources via INSERT OR IGNORE.
    """
    from email_manager.ingestion.threading import normalise_subject, insert_email_references

    p = obj.get("properties") or {}
    hs_id = obj["id"]

    date = _ms_to_iso(p.get("hs_timestamp")) or _ms_to_iso(p.get("createdate"))
    if not date:
        return False

    from_address = (p.get("hs_email_from_email") or "").strip().lower()
    if not from_address:
        return False

    # Prefer the real RFC822 Message-ID so HubSpot emails join existing threads
    message_id = _parse_hs_message_id(p.get("hs_email_message_id")) or f"hs:{hs_id}"

    from_name = " ".join(
        x for x in [p.get("hs_email_from_firstname"), p.get("hs_email_from_lastname")] if x
    ) or None

    direction = (p.get("hs_email_direction") or "").upper()
    folder = "HUBSPOT_SENT" if "OUTGOING" in direction else "HUBSPOT"

    def _split_addrs(raw: str | None) -> str:
        if not raw:
            return "[]"
        parts = [a.strip().lower() for a in raw.replace(",", ";").split(";") if a.strip()]
        return json.dumps(parts)

    subject = p.get("hs_email_subject") or ""
    to_addresses = _split_addrs(p.get("hs_email_to_email"))
    cc_addresses = _split_addrs(p.get("hs_email_cc_email"))

    from_domain = from_address.split("@", 1)[1].lower() if "@" in from_address else None

    # Extract In-Reply-To from hs_email_headers JSON (contains replyToId field)
    hs_ref_headers: dict[str, str] = {}
    hs_headers_raw = p.get("hs_email_headers") or ""
    if hs_headers_raw:
        try:
            hs_headers = json.loads(hs_headers_raw)
            reply_to_id = _parse_hs_message_id(hs_headers.get("replyToId"))
            if reply_to_id:
                hs_ref_headers["in_reply_to"] = reply_to_id
        except (json.JSONDecodeError, TypeError):
            pass

    cursor = conn.execute(
        """INSERT OR IGNORE INTO emails
           (message_id, thread_id, subject, normalised_subject,
            from_address, from_name, to_addresses, cc_addresses,
            date, body_text, body_html, raw_headers,
            folder, size_bytes, has_attachments, fetched_at,
            gmail_id, account_name, from_domain)
           VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, 0, ?, NULL, 'hubspot', ?)""",
        (
            message_id,
            subject,
            normalise_subject(subject),
            from_address,
            from_name,
            to_addresses,
            cc_addresses,
            date,
            p.get("hs_email_text") or "",
            p.get("hs_email_html") or "",
            folder,
            fetched_at,
            from_domain,
        ),
    )

    if cursor.rowcount > 0 and hs_ref_headers:
        insert_email_references(conn, cursor.lastrowid, hs_ref_headers)

    # Always record hs_id → message_id mapping (even for existing emails)
    conn.execute(
        "INSERT OR IGNORE INTO hubspot_email_id_map (hs_id, message_id) VALUES (?, ?)",
        (hs_id, message_id),
    )

    # Record deal associations if present in the object
    deal_assocs = (obj.get("associations") or {}).get("deals", {}).get("results") or []
    for assoc in deal_assocs:
        deal_id = str(assoc.get("id", "")).strip()
        if deal_id:
            conn.execute(
                "INSERT OR IGNORE INTO hubspot_email_deal_links (hs_id, deal_id) VALUES (?, ?)",
                (hs_id, deal_id),
            )

    return cursor.rowcount > 0


def sync_email_engagements(
    conn: sqlite3.Connection,
    client: HubSpotClient,
    *,
    limit: int | None = None,
    console: Console | None = None,
) -> int:
    """Sync HubSpot email engagements into the main emails table.

    Emails land as account_name='hubspot' with message_id='hs:<id>'.
    Thread assignment is run at the end so they integrate with existing threads.
    Returns the number of newly inserted emails.
    """
    from email_manager.ingestion.threading import compute_threads

    console = console or Console()
    fetched_at = _now_iso()
    n_new = 0
    n_seen = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed} fetched"),
        console=console,
        transient=True,
    ) as progress:
        bar = progress.add_task("HubSpot email engagements", total=None)
        for obj in client.iter_objects("emails", EMAIL_ENGAGEMENT_PROPERTIES, associations=["deals"]):
            inserted = _upsert_hubspot_email(conn, obj, fetched_at)
            if inserted:
                n_new += 1
            n_seen += 1
            if n_seen % 200 == 0:
                conn.commit()
            progress.update(bar, advance=1)
            if limit and n_seen >= limit:
                break

    conn.commit()

    console.print(f"  {n_seen} processed, {n_new} new email(s) added")
    if n_new > 0:
        console.print(f"  Threading {n_new} new email(s)…")
        compute_threads(conn, console=console)

    _update_sync_state(conn, "email_engagements", n_seen)
    conn.commit()
    return n_new


# ---------------------------------------------------------------------------
# Deals
# ---------------------------------------------------------------------------

DEAL_PROPERTIES = [
    "dealname",
    "dealstage",
    "pipeline",
    "amount",
    "closedate",
    "description",
    "hs_deal_stage_probability",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
]

# Map HubSpot deal stage identifiers to canonical current_state values used
# in the discussions table. Only terminal states need explicit mapping — all
# non-terminal stages default to "active".
_DEAL_STAGE_STATE: dict[str, str] = {
    "closedwon": "won",
    "closedlost": "lost",
}


def _deal_state(stage: str | None) -> str:
    if not stage:
        return "active"
    return _DEAL_STAGE_STATE.get(stage.lower(), "active")


def _upsert_deal(
    conn: sqlite3.Connection,
    obj: dict,
    fetched_at: str,
    portal_id: int | str | None = None,
) -> str:
    p = obj.get("properties") or {}
    deal_id = obj["id"]
    hs_url = _hubspot_url(portal_id, "0-3", deal_id) if portal_id else None
    close_raw = p.get("closedate") or ""
    close_date = close_raw[:10] if close_raw else None

    conn.execute(
        """INSERT INTO hubspot_deals (
               id, name, stage, pipeline, amount, close_date,
               hs_created_at, hs_updated_at, hs_url, properties_json, fetched_at
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET
               name=excluded.name, stage=excluded.stage, pipeline=excluded.pipeline,
               amount=excluded.amount, close_date=excluded.close_date,
               hs_created_at=excluded.hs_created_at, hs_updated_at=excluded.hs_updated_at,
               hs_url=excluded.hs_url,
               properties_json=excluded.properties_json, fetched_at=excluded.fetched_at""",
        (
            deal_id,
            p.get("dealname"),
            p.get("dealstage"),
            p.get("pipeline"),
            _to_float(p.get("amount")),
            close_date,
            p.get("createdate"),
            p.get("hs_lastmodifieddate"),
            hs_url,
            json.dumps(p),
            fetched_at,
        ),
    )

    # Record company associations
    assoc_list = (obj.get("associations") or {}).get("companies", {}).get("results", [])
    for assoc in assoc_list:
        cid = str(assoc.get("id", ""))
        if cid:
            conn.execute(
                "INSERT OR IGNORE INTO hubspot_deal_companies (deal_id, company_id) VALUES (?, ?)",
                (deal_id, cid),
            )

    return deal_id


def sync_deals(
    conn: sqlite3.Connection,
    client: HubSpotClient,
    *,
    limit: int | None = None,
    console: Console | None = None,
) -> int:
    """Sync HubSpot deals into hubspot_deals. Returns the number of deals synced."""
    console = console or Console()
    fetched_at = _now_iso()

    portal_info = client._get("/account-info/v1/details")
    portal_id = portal_info.get("portalId")

    n = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed} fetched"),
        console=console,
        transient=True,
    ) as progress:
        bar = progress.add_task("HubSpot deals", total=None)
        for obj in client.iter_objects("deals", DEAL_PROPERTIES, associations=["companies"]):
            _upsert_deal(conn, obj, fetched_at, portal_id)
            n += 1
            if n % 100 == 0:
                conn.commit()
            progress.update(bar, advance=1)
            if limit and n >= limit:
                break

    _update_sync_state(conn, "deals", n)
    conn.commit()
    console.print(f"  {n} deal(s) synced")
    return n


def sync_deal_discussions(
    conn: sqlite3.Connection,
    *,
    console: Console | None = None,
) -> int:
    """Create or update discussions from HubSpot deals.

    Each deal becomes a discussion on its associated local company.  Existing
    deal-sourced discussions are updated (title, state) when the deal changes;
    new ones are created with source_type='hubspot_deal'.

    Returns the number of discussions created or updated.
    """
    console = console or Console()
    now = _now_iso()
    n_created = 0
    n_updated = 0

    # Join deals → hubspot_deal_companies → hubspot_companies → local companies
    deals = conn.execute(
        """SELECT
               d.id            AS deal_id,
               d.name          AS deal_name,
               d.stage         AS deal_stage,
               d.close_date,
               d.hs_url,
               d.hs_created_at,
               d.hs_updated_at,
               hc.domain       AS hs_domain,
               c.id            AS local_company_id
           FROM hubspot_deals d
           JOIN hubspot_deal_companies hdc ON hdc.deal_id = d.id
           JOIN hubspot_companies hc ON hc.id = hdc.company_id
           JOIN companies c ON LOWER(c.domain) = LOWER(hc.domain)
           WHERE hc.domain IS NOT NULL AND hc.domain != ''
        """
    ).fetchall()

    for row in deals:
        deal_id = str(row["deal_id"])
        name = row["deal_name"] or f"Deal {deal_id}"
        state = _deal_state(row["deal_stage"])
        company_id = row["local_company_id"]
        first_seen = (row["hs_created_at"] or now)[:10]
        last_seen = (row["hs_updated_at"] or row["close_date"] or now)[:10]

        existing = conn.execute(
            "SELECT id, current_state FROM discussions WHERE source_type = 'hubspot_deal' AND source_id = ?",
            (deal_id,),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE discussions SET
                       title = ?, current_state = ?, last_seen = ?, updated_at = ?
                   WHERE id = ?""",
                (name, state, last_seen, now, existing["id"]),
            )
            n_updated += 1
        else:
            conn.execute(
                """INSERT INTO discussions
                       (title, category, current_state, company_id,
                        first_seen, last_seen, updated_at,
                        source_type, source_id)
                   VALUES (?, 'deal', ?, ?, ?, ?, ?, 'hubspot_deal', ?)""",
                (name, state, company_id, first_seen, last_seen, now, deal_id),
            )
            n_created += 1

    conn.commit()
    console.print(f"  {n_created} deal discussion(s) created, {n_updated} updated")
    return n_created + n_updated


def sync_deal_email_links(
    conn: sqlite3.Connection,
    client: "HubSpotClient",
    *,
    console: Console | None = None,
) -> int:
    """Populate hubspot_email_id_map and hubspot_email_deal_links from deal associations.

    Goes deals → email associations → fetches message IDs for those emails.
    Much faster than re-syncing all email engagements (only needs the deals we have).
    Returns the number of email-deal links stored.
    """
    console = console or Console()

    # Get all deal IDs we know about
    deal_rows = conn.execute("SELECT id FROM hubspot_deals").fetchall()
    deal_ids = [str(r["id"]) for r in deal_rows]
    if not deal_ids:
        console.print("  No deals to link")
        return 0

    console.print(f"  Fetching email associations for {len(deal_ids)} deals…")
    deal_to_emails = client.batch_get_associations("deals", "emails", deal_ids)

    # Collect unique email IDs that have deal associations
    email_id_set: set[str] = set()
    for email_ids in deal_to_emails.values():
        email_id_set.update(email_ids)

    if not email_id_set:
        console.print("  No email associations found for any deal")
        return 0

    email_ids_list = list(email_id_set)
    console.print(f"  Fetching message IDs for {len(email_ids_list)} associated emails…")
    email_objs = client.batch_read("emails", email_ids_list, ["hs_email_message_id"])

    # Build hs_id → message_id map
    hs_to_msg: dict[str, str] = {}
    for obj in email_objs:
        hs_id = str(obj["id"])
        raw_mid = (obj.get("properties") or {}).get("hs_email_message_id")
        message_id = _parse_hs_message_id(raw_mid) or f"hs:{hs_id}"
        hs_to_msg[hs_id] = message_id

    # Store mappings
    n_links = 0
    for deal_id, email_ids in deal_to_emails.items():
        for hs_id in email_ids:
            message_id = hs_to_msg.get(hs_id)
            if not message_id:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO hubspot_email_id_map (hs_id, message_id) VALUES (?, ?)",
                (hs_id, message_id),
            )
            conn.execute(
                "INSERT OR IGNORE INTO hubspot_email_deal_links (hs_id, deal_id) VALUES (?, ?)",
                (hs_id, deal_id),
            )
            n_links += 1

    conn.commit()
    console.print(f"  {n_links} email-deal link(s) stored")
    return n_links


def sync_deal_email_events(
    conn: sqlite3.Connection,
    *,
    console: Console | None = None,
) -> int:
    """Assign event_ledger entries to deal discussions via HubSpot email-deal links.

    For emails that HubSpot associates with a specific deal, assigns their extracted
    events directly to the corresponding deal discussion — no LLM needed.
    Also populates discussion_threads so the emails are visible in the UI.

    Returns the number of events assigned.
    """
    console = console or Console()
    now = _now_iso()

    rows = conn.execute(
        """SELECT el.id AS event_id, el.event_date, el.thread_id,
                  e.thread_id AS email_thread_id, d.id AS discussion_id
           FROM event_ledger el
           JOIN emails e ON el.source_email_id = e.message_id
           JOIN hubspot_email_id_map heim ON heim.message_id = e.message_id
           JOIN hubspot_email_deal_links hedl ON hedl.hs_id = heim.hs_id
           JOIN discussions d ON d.source_id = hedl.deal_id
               AND d.source_type = 'hubspot_deal'
           WHERE el.discussion_id IS NULL"""
    ).fetchall()

    if not rows:
        console.print("  0 deal-linked events to assign")
        return 0

    from collections import defaultdict
    events_by_disc: dict = defaultdict(list)
    for row in rows:
        events_by_disc[row["discussion_id"]].append(row)

    n_assigned = 0
    for disc_id, events in events_by_disc.items():
        event_ids = [e["event_id"] for e in events]
        dates = [e["event_date"] for e in events if e["event_date"]]
        placeholders = ",".join("?" * len(event_ids))
        conn.execute(
            f"UPDATE event_ledger SET discussion_id = ? WHERE id IN ({placeholders})",
            (disc_id, *event_ids),
        )
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            conn.execute(
                """UPDATE discussions SET
                       first_seen = CASE WHEN first_seen > ? OR first_seen IS NULL THEN ? ELSE first_seen END,
                       last_seen  = CASE WHEN last_seen  < ? OR last_seen  IS NULL THEN ? ELSE last_seen  END,
                       updated_at = ?
                   WHERE id = ?""",
                (min_date, min_date, max_date, max_date, now, disc_id),
            )

        # Link threads so emails are visible in the UI
        thread_ids = {
            e["email_thread_id"] for e in events if e["email_thread_id"]
        }
        for tid in thread_ids:
            conn.execute(
                "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
                (disc_id, tid),
            )

        n_assigned += len(event_ids)

    conn.commit()
    console.print(f"  {n_assigned} event(s) assigned to deal discussions")
    return n_assigned


def repair_deal_discussion_threads(
    conn: sqlite3.Connection,
    *,
    console: Console | None = None,
) -> int:
    """Backfill discussion_threads for existing deal discussions.

    Finds all hubspot_deal discussions that have events in event_ledger but
    are missing the corresponding thread links. Safe to run repeatedly.

    Returns the number of thread links added.
    """
    console = console or Console()

    rows = conn.execute(
        """SELECT DISTINCT el.discussion_id, e.thread_id
           FROM event_ledger el
           JOIN emails e ON el.source_email_id = e.message_id
           JOIN discussions d ON d.id = el.discussion_id
               AND d.source_type = 'hubspot_deal'
           WHERE e.thread_id IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM discussion_threads dt
               WHERE dt.discussion_id = el.discussion_id
                 AND dt.thread_id = e.thread_id
             )"""
    ).fetchall()

    if not rows:
        console.print("  0 missing thread links to repair")
        return 0

    for row in rows:
        conn.execute(
            "INSERT OR IGNORE INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)",
            (row["discussion_id"], row["thread_id"]),
        )

    conn.commit()
    console.print(f"  [green]Repaired {len(rows)} missing discussion_threads link(s)[/green]")
    return len(rows)


# ── Notes ─────────────────────────────────────────────────────────────────────

NOTE_PROPERTIES = [
    "hs_note_body",
    "hs_timestamp",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
]


def _upsert_note(
    conn: sqlite3.Connection,
    obj: dict,
    fetched_at: str,
    portal_id: str | None = None,
    ui_domain: str = "app.hubspot.com",
) -> str:
    """Insert or update a HubSpot note. Returns the note ID."""
    p = obj.get("properties") or {}

    hs_url = obj.get("url")
    if not hs_url and portal_id:
        hs_url = f"https://{ui_domain}/contacts/{portal_id}/record/0-46/{obj['id']}"

    assocs = obj.get("associations") or {}
    contact_ids = list({
        a["id"] for a in (assocs.get("contacts") or {}).get("results", [])
    })
    company_ids = list({
        a["id"] for a in (assocs.get("companies") or {}).get("results", [])
    })
    deal_ids = list({
        a["id"] for a in (assocs.get("deals") or {}).get("results", [])
    })

    conn.execute(
        """INSERT INTO hubspot_notes (
            id, body, created_at, updated_at, owner_id,
            associated_contact_ids, associated_company_ids, associated_deal_ids,
            hs_url, properties_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            body=excluded.body,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at,
            owner_id=excluded.owner_id,
            associated_contact_ids=excluded.associated_contact_ids,
            associated_company_ids=excluded.associated_company_ids,
            associated_deal_ids=excluded.associated_deal_ids,
            hs_url=excluded.hs_url,
            properties_json=excluded.properties_json,
            fetched_at=excluded.fetched_at""",
        (
            obj["id"],
            p.get("hs_note_body"),
            _ms_to_iso(p.get("hs_timestamp") or p.get("createdate")),
            _ms_to_iso(p.get("hs_lastmodifieddate")),
            p.get("hubspot_owner_id"),
            json.dumps(contact_ids),
            json.dumps(company_ids),
            json.dumps(deal_ids),
            hs_url,
            json.dumps(p),
            fetched_at,
        ),
    )
    return obj["id"]


def sync_notes(
    conn: sqlite3.Connection,
    client: "HubSpotClient",
    *,
    limit: int | None = None,
    console: Console | None = None,
) -> int:
    """Fetch HubSpot notes and store them. Returns the number of notes synced."""
    from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn

    console = console or Console()
    fetched_at = _now_iso()

    info = client.account_info()
    portal_id = str(info.get("portalId") or "")
    ui_domain = info.get("uiDomain") or "app.hubspot.com"

    n = 0
    with Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn(), console=console) as progress:
        bar = progress.add_task("HubSpot notes", total=None)
        for obj in client.iter_objects(
            "notes",
            NOTE_PROPERTIES,
            associations=["deals", "contacts", "companies"],
        ):
            _upsert_note(conn, obj, fetched_at, portal_id=portal_id, ui_domain=ui_domain)
            n += 1
            if n % 100 == 0:
                conn.commit()
            progress.update(bar, advance=1)
            if limit and n >= limit:
                break

    conn.commit()
    _update_sync_state(conn, "notes", n)
    conn.commit()
    console.print(f"  {n} note(s) synced")
    return n


def link_notes_to_deal_discussions(
    conn: sqlite3.Connection,
    *,
    console: Console | None = None,
) -> int:
    """Populate discussion_notes by joining hubspot_notes deal associations to deal discussions.

    For each note that is associated with a HubSpot deal that has a corresponding
    discussion, inserts a row into discussion_notes. Safe to run repeatedly.

    Returns the number of new links created.
    """
    console = console or Console()

    # Iterate over notes that have deal associations
    rows = conn.execute(
        """SELECT hn.id AS note_id, hn.associated_deal_ids
           FROM hubspot_notes hn
           WHERE hn.associated_deal_ids IS NOT NULL
             AND hn.associated_deal_ids != '[]'"""
    ).fetchall()

    n_linked = 0
    for row in rows:
        try:
            deal_ids = json.loads(row["associated_deal_ids"] or "[]")
        except (ValueError, TypeError):
            continue
        for deal_id in deal_ids:
            disc = conn.execute(
                "SELECT id FROM discussions WHERE source_type = 'hubspot_deal' AND source_id = ?",
                (deal_id,),
            ).fetchone()
            if not disc:
                continue
            result = conn.execute(
                "INSERT OR IGNORE INTO discussion_notes (discussion_id, note_id) VALUES (?, ?)",
                (disc["id"], row["note_id"]),
            )
            n_linked += result.rowcount

    conn.commit()
    console.print(f"  {n_linked} note-discussion link(s) created")
    return n_linked
