"""Unified query layer for the entity model.

Stitches per-source rows (email, homepage, hubspot) into a single dict per
organization or person. Field precedence is hardcoded for v1 — see the
`_ORG_PRECEDENCE` / `_PERSON_PRECEDENCE` tables below. Per-entity field
overrides (from `field_overrides`) apply last and always win.

The list functions are intentionally written as straight SQL rather than a
DB view — keeps the SQL dialect translator (SQLite -> Postgres) on its
narrow happy path and makes pagination/filtering easy to extend.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

# ── Field precedence ──────────────────────────────────────────────────────
# Each entry says which source provides a field, in priority order.
# The merge layer walks the list and picks the first non-null value.

_ORG_PRECEDENCE: dict[str, tuple[str, ...]] = {
    "name":             ("hubspot", "email", "homepage"),
    "domain":           ("email", "hubspot", "homepage"),
    "description":      ("homepage", "hubspot"),
    # CRM-only fields — only HubSpot provides them.
    "industry":         ("hubspot",),
    "lifecycle_stage":  ("hubspot",),
    "type":             ("hubspot",),
    "owner_id":         ("hubspot",),
    "phone":            ("hubspot",),
    "city":             ("hubspot",),
    "state":            ("hubspot",),
    "country":          ("hubspot",),
    "num_employees":    ("hubspot",),
    "annual_revenue":   ("hubspot",),
    "linkedin_url":     ("hubspot",),
    "twitter_handle":   ("hubspot",),
    "founded_year":     ("hubspot",),
    "website":          ("hubspot",),
    # Activity fields — only email provides them.
    "email_count":      ("email",),
    "first_seen":       ("email",),
    "last_seen":        ("email",),
    # Homepage-only.
    "homepage_fetched_at": ("homepage",),
}


_PERSON_PRECEDENCE: dict[str, tuple[str, ...]] = {
    "name":             ("hubspot", "email"),
    "email":            ("email", "hubspot"),
    "company_name":     ("hubspot", "email"),
    "job_title":        ("hubspot",),
    "phone":            ("hubspot",),
    "city":             ("hubspot",),
    "state":            ("hubspot",),
    "country":          ("hubspot",),
    "address":          ("hubspot",),
    "lifecycle_stage":  ("hubspot",),
    "lead_status":      ("hubspot",),
    "owner_id":         ("hubspot",),
    "linkedin_url":     ("hubspot",),
    "twitter_handle":   ("hubspot",),
    "industry":         ("hubspot",),
    "salutation":       ("hubspot",),
    "website":          ("hubspot",),
    # Activity — email only.
    "email_count":      ("email",),
    "sent_count":       ("email",),
    "received_count":   ("email",),
    "first_seen":       ("email",),
    "last_seen":        ("email",),
}


def _row_dict(row: Any) -> dict[str, Any]:
    """Normalise a sqlite3.Row / PostgresRow to a plain dict."""
    if row is None:
        return {}
    if hasattr(row, "_data"):
        return dict(row._data)  # PostgresRow
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}  # sqlite3.Row
    return dict(row)


def _merge(precedence: dict[str, tuple[str, ...]], by_source: dict[str, dict]) -> dict:
    """Apply precedence rules to per-source dicts. First non-null wins."""
    out: dict[str, Any] = {}
    for field, sources in precedence.items():
        for src in sources:
            v = by_source.get(src, {}).get(field)
            if v is not None and v != "":
                out[field] = v
                break
        out.setdefault(field, None)
    return out


def _apply_overrides(
    conn: Any, entity_type: str, entity_id: int, merged: dict[str, Any]
) -> dict[str, Any]:
    """Replace fields with values from field_overrides."""
    rows = conn.execute(
        "SELECT field, value FROM field_overrides WHERE entity_type = ? AND entity_id = ?",
        (entity_type, entity_id),
    ).fetchall()
    if not rows:
        return merged
    for r in rows:
        d = _row_dict(r)
        merged[d["field"]] = d["value"]
    return merged


# ── Per-source extractors ─────────────────────────────────────────────────
# Each returns a flat dict keyed by the canonical field names used in
# _*_PRECEDENCE. Fields not provided by a source are simply absent.


def _extract_email_company(row: dict) -> dict:
    return {
        "name": row.get("name"),
        "domain": row.get("domain"),
        "email_count": row.get("email_count"),
        "first_seen": row.get("first_seen"),
        "last_seen": row.get("last_seen"),
    }


def _extract_homepage_company(row: dict) -> dict:
    return {
        "name": row.get("name"),
        "domain": row.get("domain"),
        "description": row.get("description"),
        "homepage_fetched_at": row.get("homepage_fetched_at"),
    }


def _extract_hubspot_company(row: dict) -> dict:
    return {
        "name": row.get("name"),
        "domain": row.get("domain"),
        "description": row.get("description") or row.get("about_us"),
        "website": row.get("website"),
        "industry": row.get("industry"),
        "lifecycle_stage": row.get("lifecycle_stage"),
        "type": row.get("type"),
        "owner_id": row.get("owner_id"),
        "phone": row.get("phone"),
        "city": row.get("city"),
        "state": row.get("state"),
        "country": row.get("country"),
        "num_employees": row.get("num_employees"),
        "annual_revenue": row.get("annual_revenue"),
        "linkedin_url": row.get("linkedin_url"),
        "twitter_handle": row.get("twitter_handle"),
        "founded_year": row.get("founded_year"),
    }


def _extract_email_contact(row: dict) -> dict:
    return {
        "name": row.get("name"),
        "email": row.get("email"),
        "company_name": row.get("company"),
        "email_count": row.get("email_count"),
        "sent_count": row.get("sent_count"),
        "received_count": row.get("received_count"),
        "first_seen": row.get("first_seen"),
        "last_seen": row.get("last_seen"),
    }


def _extract_hubspot_contact(row: dict) -> dict:
    full_name = " ".join(
        x for x in (row.get("firstname"), row.get("lastname")) if x
    ).strip() or None
    return {
        "name": full_name,
        "email": row.get("email"),
        "company_name": row.get("company_name"),
        "job_title": row.get("job_title"),
        "phone": row.get("phone"),
        "city": row.get("city"),
        "state": row.get("state"),
        "country": row.get("country"),
        "address": row.get("address"),
        "lifecycle_stage": row.get("lifecycle_stage"),
        "lead_status": row.get("lead_status"),
        "owner_id": row.get("owner_id"),
        "linkedin_url": row.get("linkedin_url"),
        "twitter_handle": row.get("twitter_handle"),
        "industry": row.get("industry"),
        "salutation": row.get("salutation"),
        "website": row.get("website"),
    }


# ── Per-org / per-person resolution ───────────────────────────────────────


def _load_org_sources(conn: Any, org_id: int) -> tuple[dict[str, dict], list[dict]]:
    """Return (per-source extracted dicts, raw identity rows) for one org.

    If an org has multiple identities of the same source (e.g. after a manual
    merge), the lowest source_id wins for the per-source dict. All identities
    are returned in the second element for UI display.
    """
    identity_rows = [
        _row_dict(r)
        for r in conn.execute(
            """SELECT source, source_id, match_key, confidence, is_manual, created_at
               FROM organization_identities
               WHERE organization_id = ?
               ORDER BY source, source_id""",
            (org_id,),
        ).fetchall()
    ]

    by_source: dict[str, dict] = {}
    seen_sources: set[str] = set()
    for ident in identity_rows:
        src = ident["source"]
        if src in seen_sources:
            continue  # already loaded the lowest source_id for this source
        seen_sources.add(src)
        sid = ident["source_id"]
        if src in ("email", "homepage"):
            row = conn.execute(
                "SELECT id, name, domain, email_count, first_seen, last_seen, "
                "       homepage_fetched_at, description "
                "FROM companies WHERE id = ?",
                (int(sid),),
            ).fetchone()
            if row is None:
                continue
            d = _row_dict(row)
            by_source[src] = (
                _extract_email_company(d) if src == "email" else _extract_homepage_company(d)
            )
        elif src == "hubspot":
            row = conn.execute(
                "SELECT id, name, domain, website, industry, description, about_us, "
                "       city, state, country, phone, num_employees, annual_revenue, "
                "       lifecycle_stage, type, owner_id, founded_year, "
                "       linkedin_url, twitter_handle "
                "FROM hubspot_companies WHERE id = ?",
                (sid,),
            ).fetchone()
            if row is None:
                continue
            by_source[src] = _extract_hubspot_company(_row_dict(row))
    return by_source, identity_rows


def _load_person_sources(conn: Any, person_id: int) -> tuple[dict[str, dict], list[dict]]:
    identity_rows = [
        _row_dict(r)
        for r in conn.execute(
            """SELECT source, source_id, match_key, confidence, is_manual, created_at
               FROM person_identities
               WHERE person_id = ?
               ORDER BY source, source_id""",
            (person_id,),
        ).fetchall()
    ]
    by_source: dict[str, dict] = {}
    seen: set[str] = set()
    for ident in identity_rows:
        src = ident["source"]
        if src in seen:
            continue
        seen.add(src)
        sid = ident["source_id"]
        if src == "email":
            row = conn.execute(
                "SELECT id, email, name, company, first_seen, last_seen, "
                "       email_count, sent_count, received_count "
                "FROM contacts WHERE id = ?",
                (int(sid),),
            ).fetchone()
            if row is None:
                continue
            by_source[src] = _extract_email_contact(_row_dict(row))
        elif src == "hubspot":
            row = conn.execute(
                "SELECT id, email, firstname, lastname, company_name, job_title, phone, "
                "       city, state, country, address, lifecycle_stage, lead_status, "
                "       owner_id, twitter_handle, linkedin_url, website, industry, salutation "
                "FROM hubspot_contacts WHERE id = ?",
                (sid,),
            ).fetchone()
            if row is None:
                continue
            by_source[src] = _extract_hubspot_contact(_row_dict(row))
    return by_source, identity_rows


def get_organization(conn: Any, org_id: int) -> dict | None:
    """Return a fully merged organization dict, or None if not found."""
    org_row = conn.execute(
        "SELECT id, canonical_name, canonical_domain, notes, created_at, updated_at "
        "FROM organizations WHERE id = ?",
        (org_id,),
    ).fetchone()
    if org_row is None:
        return None
    org = _row_dict(org_row)
    by_source, identities = _load_org_sources(conn, org_id)
    merged = _merge(_ORG_PRECEDENCE, by_source)
    # Canonical fields seed the merge — they're a hand-curated layer between
    # source data and overrides.
    if org.get("canonical_name"):
        merged["name"] = org["canonical_name"]
    if org.get("canonical_domain"):
        merged["domain"] = org["canonical_domain"]
    merged = _apply_overrides(conn, "organization", org_id, merged)

    return {
        "id": org_id,
        "notes": org.get("notes"),
        "created_at": org.get("created_at"),
        "updated_at": org.get("updated_at"),
        "sources": list(by_source.keys()),
        "identities": identities,
        **merged,
    }


def get_person(conn: Any, person_id: int) -> dict | None:
    person_row = conn.execute(
        "SELECT id, canonical_name, canonical_email, notes, created_at, updated_at "
        "FROM people WHERE id = ?",
        (person_id,),
    ).fetchone()
    if person_row is None:
        return None
    p = _row_dict(person_row)
    by_source, identities = _load_person_sources(conn, person_id)
    merged = _merge(_PERSON_PRECEDENCE, by_source)
    if p.get("canonical_name"):
        merged["name"] = p["canonical_name"]
    if p.get("canonical_email"):
        merged["email"] = p["canonical_email"]
    merged = _apply_overrides(conn, "person", person_id, merged)
    return {
        "id": person_id,
        "notes": p.get("notes"),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
        "sources": list(by_source.keys()),
        "identities": identities,
        **merged,
    }


# ── List queries ──────────────────────────────────────────────────────────
# These exist so the web list pages don't pay N+1 cost. They build a single
# joined query per page and merge per row in Python.


def _org_list_sql(where: str, order: str, with_limit: bool) -> str:
    # Pick the deterministic "primary" identity per source via a correlated
    # subquery. Same shape as get_organization's per-source resolution but
    # batched into one statement.
    limit_clause = "LIMIT ? OFFSET ?" if with_limit else ""
    return f"""
    WITH ranked_email AS (
        SELECT oi.organization_id, MIN(CAST(oi.source_id AS INTEGER)) AS company_id
        FROM organization_identities oi
        WHERE oi.source = 'email'
        GROUP BY oi.organization_id
    ),
    ranked_homepage AS (
        SELECT oi.organization_id, MIN(CAST(oi.source_id AS INTEGER)) AS company_id
        FROM organization_identities oi
        WHERE oi.source = 'homepage'
        GROUP BY oi.organization_id
    ),
    ranked_hubspot AS (
        SELECT oi.organization_id, MIN(oi.source_id) AS hubspot_id
        FROM organization_identities oi
        WHERE oi.source = 'hubspot'
        GROUP BY oi.organization_id
    )
    SELECT
        o.id AS org_id, o.canonical_name, o.canonical_domain,
        ec.id AS email_company_id, ec.name AS email_name, ec.domain AS email_domain,
        ec.email_count, ec.first_seen, ec.last_seen,
        ec.staleness_status,
        hp.description AS homepage_description, hp.homepage_fetched_at,
        hp.name AS homepage_name,
        hc.id AS hubspot_id, hc.name AS hubspot_name, hc.domain AS hubspot_domain,
        hc.industry, hc.lifecycle_stage, hc.country,
        hc.description AS hubspot_description,
        re.organization_id IS NOT NULL AS has_email,
        rh.organization_id IS NOT NULL AS has_homepage,
        rhs.organization_id IS NOT NULL AS has_hubspot
    FROM organizations o
    LEFT JOIN ranked_email re ON re.organization_id = o.id
    LEFT JOIN companies ec ON ec.id = re.company_id
    LEFT JOIN ranked_homepage rh ON rh.organization_id = o.id
    LEFT JOIN companies hp ON hp.id = rh.company_id
    LEFT JOIN ranked_hubspot rhs ON rhs.organization_id = o.id
    LEFT JOIN hubspot_companies hc ON hc.id = rhs.hubspot_id
    {where}
    {order}
    {limit_clause}
    """


def list_organizations(
    conn: Any,
    *,
    q: str = "",
    label: str = "",
    stale: str = "",
    sort: str = "email_count",
    order: str = "desc",
    page: int = 1,
    limit: int = 25,
) -> dict:
    """Paginated unified-org list. Mirrors the existing /api/companies shape."""
    allowed_sorts = {
        "email_count": "ec.email_count",
        "name": "COALESCE(o.canonical_name, hc.name, ec.name, hp.name)",
        "last_seen": "ec.last_seen",
    }
    sort_col = allowed_sorts.get(sort, "ec.email_count")
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    where_parts: list[str] = []
    params: list[Any] = []

    if q:
        where_parts.append(
            "(COALESCE(o.canonical_name, '') LIKE ? OR COALESCE(o.canonical_domain, '') LIKE ? "
            " OR COALESCE(ec.name, '') LIKE ? OR COALESCE(hc.name, '') LIKE ? "
            " OR COALESCE(ec.domain, '') LIKE ? OR COALESCE(hc.domain, '') LIKE ?)"
        )
        like = f"%{q}%"
        params.extend([like, like, like, like, like, like])

    if label:
        where_parts.append(
            "ec.id IN (SELECT company_id FROM company_labels WHERE label = ?)"
        )
        params.append(label)

    if stale == "1":
        where_parts.append("ec.staleness_status = 'stale'")
    elif stale == "0":
        where_parts.append("ec.staleness_status = 'up_to_date'")
    elif stale == "never":
        where_parts.append(
            "(ec.staleness_status = 'never' OR ec.staleness_status IS NULL)"
        )

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    order_clause = f"ORDER BY {sort_col} {order_dir} NULLS LAST"

    # Postgres supports NULLS LAST; SQLite does too as of 3.30. Both fine.
    offset = max(0, (page - 1) * limit)
    sql = _org_list_sql(where_clause, order_clause, with_limit=True)

    rows = conn.execute(sql, tuple(params + [limit, offset])).fetchall()
    items = [_row_dict(r) for r in rows]

    # Total count uses the same WHERE on the same join shape.
    count_sql = f"""
    WITH ranked_email AS (
        SELECT oi.organization_id, MIN(CAST(oi.source_id AS INTEGER)) AS company_id
        FROM organization_identities oi WHERE oi.source = 'email'
        GROUP BY oi.organization_id
    ),
    ranked_hubspot AS (
        SELECT oi.organization_id, MIN(oi.source_id) AS hubspot_id
        FROM organization_identities oi WHERE oi.source = 'hubspot'
        GROUP BY oi.organization_id
    )
    SELECT COUNT(*) AS cnt FROM organizations o
    LEFT JOIN ranked_email re ON re.organization_id = o.id
    LEFT JOIN companies ec ON ec.id = re.company_id
    LEFT JOIN ranked_hubspot rhs ON rhs.organization_id = o.id
    LEFT JOIN hubspot_companies hc ON hc.id = rhs.hubspot_id
    {where_clause}
    """
    total_row = conn.execute(count_sql, tuple(params)).fetchone()
    total = int(_row_dict(total_row).get("cnt", 0))

    org_ids = [r["org_id"] for r in items]
    labels_by_org = _labels_for_orgs(conn, org_ids)
    overrides_by_org = _overrides_for("organization", conn, org_ids)

    enriched = []
    for r in items:
        org_id = r["org_id"]
        sources_present: list[str] = []
        if r.get("has_email"):
            sources_present.append("email")
        if r.get("has_homepage"):
            sources_present.append("homepage")
        if r.get("has_hubspot"):
            sources_present.append("hubspot")

        # Field merge (mirrors _merge but inlined for the per-row column set).
        name = (
            r["canonical_name"]
            or r["hubspot_name"]
            or r["email_name"]
            or r["homepage_name"]
        )
        domain = r["canonical_domain"] or r["email_domain"] or r["hubspot_domain"]
        description = r["homepage_description"] or r["hubspot_description"]
        item = {
            "id": org_id,
            "name": name,
            "domain": domain,
            "description": description,
            "email_count": r["email_count"] or 0,
            "first_seen": r["first_seen"],
            "last_seen": r["last_seen"],
            "industry": r["industry"],
            "lifecycle_stage": r["lifecycle_stage"],
            "country": r["country"],
            "is_stale": r.get("staleness_status") == "stale",
            "staleness_status": r.get("staleness_status") or "never",
            "homepage_fetched_at": r["homepage_fetched_at"],
            "sources": sources_present,
            "labels": labels_by_org.get(org_id, []),
            # Pointers for follow-up queries.
            "email_company_id": r["email_company_id"],
            "hubspot_id": r["hubspot_id"],
        }
        for field, value in overrides_by_org.get(org_id, {}).items():
            item[field] = value
        enriched.append(item)

    all_labels = [
        _row_dict(r)["label"]
        for r in conn.execute(
            "SELECT DISTINCT label FROM company_labels ORDER BY label"
        ).fetchall()
    ]

    return {
        "items": enriched,
        "total": total,
        "labels": all_labels,
    }


def _labels_for_orgs(conn: Any, org_ids: Iterable[int]) -> dict[int, list[str]]:
    ids = list(org_ids)
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""SELECT oi.organization_id AS org_id, cl.label
            FROM organization_identities oi
            JOIN company_labels cl ON cl.company_id = CAST(oi.source_id AS INTEGER)
            WHERE oi.source = 'email' AND oi.organization_id IN ({placeholders})""",
        tuple(ids),
    ).fetchall()
    out: dict[int, list[str]] = {}
    for r in rows:
        d = _row_dict(r)
        out.setdefault(d["org_id"], []).append(d["label"])
    # Dedupe per org (a manual merge could attach two email companies, each with overlapping labels).
    return {k: sorted(set(v)) for k, v in out.items()}


def _overrides_for(
    entity_type: str, conn: Any, ids: Iterable[int]
) -> dict[int, dict[str, Any]]:
    id_list = list(ids)
    if not id_list:
        return {}
    placeholders = ",".join("?" for _ in id_list)
    rows = conn.execute(
        f"""SELECT entity_id, field, value FROM field_overrides
            WHERE entity_type = ? AND entity_id IN ({placeholders})""",
        tuple([entity_type, *id_list]),
    ).fetchall()
    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        d = _row_dict(r)
        out.setdefault(d["entity_id"], {})[d["field"]] = d["value"]
    return out


def list_people(
    conn: Any,
    *,
    q: str = "",
    company: str = "",
    sort: str = "email_count",
    order: str = "desc",
    page: int = 1,
    limit: int = 25,
) -> dict:
    """Paginated unified-people list. Mirrors /api/contacts shape."""
    allowed_sorts = {
        "email_count": "ec.email_count",
        "name": "COALESCE(p.canonical_name, hc.firstname || ' ' || hc.lastname, ec.name)",
        "last_seen": "ec.last_seen",
    }
    sort_col = allowed_sorts.get(sort, "ec.email_count")
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    where_parts: list[str] = []
    params: list[Any] = []

    if q:
        like = f"%{q}%"
        where_parts.append(
            "(COALESCE(p.canonical_name, '') LIKE ? OR COALESCE(p.canonical_email, '') LIKE ? "
            " OR COALESCE(ec.name, '') LIKE ? OR COALESCE(ec.email, '') LIKE ? "
            " OR COALESCE(hc.firstname, '') LIKE ? OR COALESCE(hc.lastname, '') LIKE ? "
            " OR COALESCE(hc.email, '') LIKE ?)"
        )
        params.extend([like, like, like, like, like, like, like])

    if company:
        where_parts.append(
            "(COALESCE(ec.company, '') LIKE ? OR COALESCE(hc.company_name, '') LIKE ?)"
        )
        params.extend([f"%{company}%", f"%{company}%"])

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    offset = max(0, (page - 1) * limit)

    sql = f"""
    WITH ranked_email AS (
        SELECT pi.person_id, MIN(CAST(pi.source_id AS INTEGER)) AS contact_id
        FROM person_identities pi WHERE pi.source = 'email'
        GROUP BY pi.person_id
    ),
    ranked_hubspot AS (
        SELECT pi.person_id, MIN(pi.source_id) AS hubspot_id
        FROM person_identities pi WHERE pi.source = 'hubspot'
        GROUP BY pi.person_id
    )
    SELECT
        p.id AS person_id, p.canonical_name, p.canonical_email,
        ec.id AS email_contact_id, ec.email AS email_email, ec.name AS email_name,
        ec.company AS email_company, ec.first_seen, ec.last_seen,
        ec.email_count, ec.sent_count, ec.received_count,
        hc.id AS hubspot_id, hc.email AS hubspot_email,
        hc.firstname AS hubspot_firstname, hc.lastname AS hubspot_lastname,
        hc.company_name AS hubspot_company_name, hc.job_title, hc.lifecycle_stage,
        hc.lead_status, hc.country,
        re.person_id IS NOT NULL AS has_email,
        rhs.person_id IS NOT NULL AS has_hubspot
    FROM people p
    LEFT JOIN ranked_email re ON re.person_id = p.id
    LEFT JOIN contacts ec ON ec.id = re.contact_id
    LEFT JOIN ranked_hubspot rhs ON rhs.person_id = p.id
    LEFT JOIN hubspot_contacts hc ON hc.id = rhs.hubspot_id
    {where_clause}
    ORDER BY {sort_col} {order_dir} NULLS LAST
    LIMIT ? OFFSET ?
    """
    rows = conn.execute(sql, tuple(params + [limit, offset])).fetchall()
    items_raw = [_row_dict(r) for r in rows]

    count_sql = f"""
    WITH ranked_email AS (
        SELECT pi.person_id, MIN(CAST(pi.source_id AS INTEGER)) AS contact_id
        FROM person_identities pi WHERE pi.source = 'email'
        GROUP BY pi.person_id
    ),
    ranked_hubspot AS (
        SELECT pi.person_id, MIN(pi.source_id) AS hubspot_id
        FROM person_identities pi WHERE pi.source = 'hubspot'
        GROUP BY pi.person_id
    )
    SELECT COUNT(*) AS cnt FROM people p
    LEFT JOIN ranked_email re ON re.person_id = p.id
    LEFT JOIN contacts ec ON ec.id = re.contact_id
    LEFT JOIN ranked_hubspot rhs ON rhs.person_id = p.id
    LEFT JOIN hubspot_contacts hc ON hc.id = rhs.hubspot_id
    {where_clause}
    """
    total_row = conn.execute(count_sql, tuple(params)).fetchone()
    total = int(_row_dict(total_row).get("cnt", 0))

    person_ids = [r["person_id"] for r in items_raw]
    overrides = _overrides_for("person", conn, person_ids)

    enriched = []
    for r in items_raw:
        pid = r["person_id"]
        hubspot_full = " ".join(
            x for x in (r.get("hubspot_firstname"), r.get("hubspot_lastname")) if x
        ).strip() or None
        sources_present: list[str] = []
        if r.get("has_email"):
            sources_present.append("email")
        if r.get("has_hubspot"):
            sources_present.append("hubspot")

        name = r["canonical_name"] or hubspot_full or r["email_name"]
        email = r["canonical_email"] or r["email_email"] or r["hubspot_email"]
        company_name = r["hubspot_company_name"] or r["email_company"]

        item = {
            "id": pid,
            "name": name,
            "email": email,
            "company_name": company_name,
            "email_count": r["email_count"] or 0,
            "sent_count": r["sent_count"] or 0,
            "received_count": r["received_count"] or 0,
            "first_seen": r["first_seen"],
            "last_seen": r["last_seen"],
            "job_title": r["job_title"],
            "lifecycle_stage": r["lifecycle_stage"],
            "lead_status": r["lead_status"],
            "country": r["country"],
            "sources": sources_present,
            "email_contact_id": r["email_contact_id"],
            "hubspot_id": r["hubspot_id"],
        }
        for field, value in overrides.get(pid, {}).items():
            item[field] = value
        enriched.append(item)

    distinct_companies = [
        _row_dict(r).get("company")
        for r in conn.execute(
            "SELECT DISTINCT company FROM contacts WHERE company IS NOT NULL ORDER BY company"
        ).fetchall()
    ]

    return {
        "items": enriched,
        "total": total,
        "companies": [c for c in distinct_companies if c],
    }
