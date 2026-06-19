"""Entity reconciliation: link source rows to abstract organizations/people.

The entity model treats `organizations` and `people` as the canonical handles.
Each row in a source table (email-derived `companies`, `hubspot_companies`,
the homepage subset of `companies`, future LinkedIn, ...) gets a row in
`organization_identities` (or `person_identities`) pointing to its parent
entity.

v1 matching strategy: **strict exact match only**.
  - companies: `match_key` = lowercase domain
  - people:    `match_key` = lowercase email

Anything that doesn't match by exact key gets its own entity. Bridging
across variants (`acme.com` vs `acme.io`, `alice@acme.com` vs
`alice.smith@acme.com`) is the job of `merge_orgs` / `merge_people`,
typically driven from the UI.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

# Source identifiers. Free strings — adding a new source only requires choosing
# a new value here and writing a backfill path.
SOURCE_EMAIL = "email"
SOURCE_HOMEPAGE = "homepage"
SOURCE_HUBSPOT = "hubspot"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm_domain(domain: str | None) -> str | None:
    if not domain:
        return None
    d = domain.strip().lower()
    return d or None


def _norm_email(email: str | None) -> str | None:
    if not email:
        return None
    e = email.strip().lower()
    return e or None


# ── Organizations ─────────────────────────────────────────────────────────


def link_or_create_org(
    conn: Any,
    *,
    source: str,
    source_id: str,
    domain: str | None,
    name: str | None = None,
) -> int:
    """Link a source row to an organization, creating one if needed.

    Returns the organization_id. Idempotent on (source, source_id): re-running
    refreshes match_key/canonical fields but doesn't duplicate identities.
    """
    domain_key = _norm_domain(domain)
    now = _now()

    # 1. If this exact identity already exists, return its org.
    row = conn.execute(
        "SELECT organization_id FROM organization_identities WHERE source = ? AND source_id = ?",
        (source, source_id),
    ).fetchone()
    if row is not None:
        org_id = int(row[0] if not hasattr(row, "keys") else row["organization_id"])
        # Refresh match_key in case the domain changed in the source row.
        conn.execute(
            "UPDATE organization_identities SET match_key = ? WHERE source = ? AND source_id = ?",
            (domain_key, source, source_id),
        )
        _maybe_promote_org_canonical(conn, org_id, name=name, domain=domain_key)
        return org_id

    # 2. Try to attach to an existing org via match_key (exact domain).
    if domain_key:
        row = conn.execute(
            "SELECT organization_id FROM organization_identities "
            "WHERE match_key = ? ORDER BY organization_id LIMIT 1",
            (domain_key,),
        ).fetchone()
        if row is not None:
            org_id = int(row[0] if not hasattr(row, "keys") else row["organization_id"])
            conn.execute(
                """INSERT INTO organization_identities
                   (organization_id, source, source_id, match_key, confidence, is_manual, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (org_id, source, source_id, domain_key, 1.0, 0, now),
            )
            _maybe_promote_org_canonical(conn, org_id, name=name, domain=domain_key)
            return org_id

    # 3. Create a new org.
    cur = conn.execute(
        """INSERT INTO organizations (canonical_name, canonical_domain, created_at, updated_at)
           VALUES (?, ?, ?, ?)""",
        (name, domain_key, now, now),
    )
    org_id = int(cur.lastrowid) if cur.lastrowid is not None else _last_org_id(conn)
    conn.execute(
        """INSERT INTO organization_identities
           (organization_id, source, source_id, match_key, confidence, is_manual, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (org_id, source, source_id, domain_key, 1.0, 0, now),
    )
    return org_id


def _last_org_id(conn: Any) -> int:
    """Fallback for backends where lastrowid isn't reliable."""
    row = conn.execute("SELECT MAX(id) AS id FROM organizations").fetchone()
    return int(row["id"] if hasattr(row, "keys") else row[0])


def _maybe_promote_org_canonical(
    conn: Any, org_id: int, *, name: str | None, domain: str | None
) -> None:
    """Backfill empty canonical_name / canonical_domain from a fresh source.

    Doesn't overwrite existing canonical fields — those are user-editable via
    field_overrides. Just fills in nulls so a freshly-created org gets a name
    once any source provides one.
    """
    updates: list[str] = []
    params: list[Any] = []
    if name:
        updates.append("canonical_name = COALESCE(canonical_name, ?)")
        params.append(name)
    if domain:
        updates.append("canonical_domain = COALESCE(canonical_domain, ?)")
        params.append(domain)
    if not updates:
        return
    updates.append("updated_at = ?")
    params.append(_now())
    params.append(org_id)
    conn.execute(
        f"UPDATE organizations SET {', '.join(updates)} WHERE id = ?",
        tuple(params),
    )


# ── People ────────────────────────────────────────────────────────────────


def link_or_create_person(
    conn: Any,
    *,
    source: str,
    source_id: str,
    email: str | None,
    name: str | None = None,
) -> int:
    """Link a source row to a person, creating one if needed."""
    email_key = _norm_email(email)
    now = _now()

    row = conn.execute(
        "SELECT person_id FROM person_identities WHERE source = ? AND source_id = ?",
        (source, source_id),
    ).fetchone()
    if row is not None:
        person_id = int(row[0] if not hasattr(row, "keys") else row["person_id"])
        conn.execute(
            "UPDATE person_identities SET match_key = ? WHERE source = ? AND source_id = ?",
            (email_key, source, source_id),
        )
        _maybe_promote_person_canonical(conn, person_id, name=name, email=email_key)
        return person_id

    if email_key:
        row = conn.execute(
            "SELECT person_id FROM person_identities "
            "WHERE match_key = ? ORDER BY person_id LIMIT 1",
            (email_key,),
        ).fetchone()
        if row is not None:
            person_id = int(row[0] if not hasattr(row, "keys") else row["person_id"])
            conn.execute(
                """INSERT INTO person_identities
                   (person_id, source, source_id, match_key, confidence, is_manual, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (person_id, source, source_id, email_key, 1.0, 0, now),
            )
            _maybe_promote_person_canonical(conn, person_id, name=name, email=email_key)
            return person_id

    cur = conn.execute(
        """INSERT INTO people (canonical_name, canonical_email, created_at, updated_at)
           VALUES (?, ?, ?, ?)""",
        (name, email_key, now, now),
    )
    person_id = int(cur.lastrowid) if cur.lastrowid is not None else _last_person_id(conn)
    conn.execute(
        """INSERT INTO person_identities
           (person_id, source, source_id, match_key, confidence, is_manual, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (person_id, source, source_id, email_key, 1.0, 0, now),
    )
    return person_id


def _last_person_id(conn: Any) -> int:
    row = conn.execute("SELECT MAX(id) AS id FROM people").fetchone()
    return int(row["id"] if hasattr(row, "keys") else row[0])


def _maybe_promote_person_canonical(
    conn: Any, person_id: int, *, name: str | None, email: str | None
) -> None:
    updates: list[str] = []
    params: list[Any] = []
    if name:
        updates.append("canonical_name = COALESCE(canonical_name, ?)")
        params.append(name)
    if email:
        updates.append("canonical_email = COALESCE(canonical_email, ?)")
        params.append(email)
    if not updates:
        return
    updates.append("updated_at = ?")
    params.append(_now())
    params.append(person_id)
    conn.execute(
        f"UPDATE people SET {', '.join(updates)} WHERE id = ?",
        tuple(params),
    )


# ── Manual merges ─────────────────────────────────────────────────────────


def merge_orgs(
    conn: Any,
    *,
    source_id: int,
    target_id: int,
    performed_by: str | None = None,
    notes: str | None = None,
) -> int:
    """Merge organization `source_id` into `target_id`.

    Re-points all identities, marks them is_manual=1, and records the merge in
    `entity_merges`. The source organization row is deleted.
    """
    if source_id == target_id:
        return 0
    now = _now()
    moved_cur = conn.execute(
        "UPDATE organization_identities SET organization_id = ?, is_manual = 1 WHERE organization_id = ?",
        (target_id, source_id),
    )
    moved = moved_cur.rowcount if hasattr(moved_cur, "rowcount") else 0
    conn.execute("DELETE FROM organizations WHERE id = ?", (source_id,))
    # Move any field overrides scoped to the source org.
    conn.execute(
        "UPDATE field_overrides SET entity_id = ? WHERE entity_type = 'organization' AND entity_id = ?",
        (target_id, source_id),
    )
    conn.execute(
        """INSERT INTO entity_merges
           (entity_type, source_id, target_id, performed_at, performed_by, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("organization", source_id, target_id, now, performed_by, notes),
    )
    conn.execute(
        "UPDATE organizations SET updated_at = ? WHERE id = ?", (now, target_id)
    )
    return int(moved)


def merge_people(
    conn: Any,
    *,
    source_id: int,
    target_id: int,
    performed_by: str | None = None,
    notes: str | None = None,
) -> int:
    """Merge person `source_id` into `target_id`."""
    if source_id == target_id:
        return 0
    now = _now()
    moved_cur = conn.execute(
        "UPDATE person_identities SET person_id = ?, is_manual = 1 WHERE person_id = ?",
        (target_id, source_id),
    )
    moved = moved_cur.rowcount if hasattr(moved_cur, "rowcount") else 0
    conn.execute("DELETE FROM people WHERE id = ?", (source_id,))
    conn.execute(
        "UPDATE field_overrides SET entity_id = ? WHERE entity_type = 'person' AND entity_id = ?",
        (target_id, source_id),
    )
    conn.execute(
        """INSERT INTO entity_merges
           (entity_type, source_id, target_id, performed_at, performed_by, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("person", source_id, target_id, now, performed_by, notes),
    )
    conn.execute(
        "UPDATE people SET updated_at = ? WHERE id = ?", (now, target_id)
    )
    return int(moved)


# ── Field overrides ───────────────────────────────────────────────────────


def set_field_override(
    conn: Any,
    *,
    entity_type: str,
    entity_id: int,
    field: str,
    value: str | None,
    set_by: str | None = None,
) -> None:
    """Write or replace a per-entity field override.

    The merge layer applies overrides last, so this always wins over any
    source-derived value. Pass `value=None` to clear an override (we keep
    the row so we can audit when it was cleared)."""
    conn.execute(
        """INSERT INTO field_overrides
           (entity_type, entity_id, field, value, set_by, set_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(entity_type, entity_id, field) DO UPDATE SET
               value = excluded.value,
               set_by = excluded.set_by,
               set_at = excluded.set_at""",
        (entity_type, entity_id, field, value, set_by, _now()),
    )


def clear_field_override(
    conn: Any, *, entity_type: str, entity_id: int, field: str
) -> None:
    conn.execute(
        "DELETE FROM field_overrides WHERE entity_type = ? AND entity_id = ? AND field = ?",
        (entity_type, entity_id, field),
    )


# ── Backfill ──────────────────────────────────────────────────────────────


def backfill_all(conn: Any) -> dict[str, int]:
    """Walk all source tables and reconcile into the entity model.

    Idempotent — safe to re-run. Returns counts per source.
    """
    counts: dict[str, int] = {}

    # Email-derived companies.
    n = 0
    for row in conn.execute(
        "SELECT id, name, domain FROM companies WHERE domain IS NOT NULL AND domain != ''"
    ).fetchall():
        link_or_create_org(
            conn,
            source=SOURCE_EMAIL,
            source_id=str(row["id"]),
            domain=row["domain"],
            name=row["name"],
        )
        n += 1
    counts["email_companies"] = n

    # Homepage subset of companies (only rows that have homepage data).
    n = 0
    for row in conn.execute(
        "SELECT id, name, domain FROM companies "
        "WHERE homepage_fetched_at IS NOT NULL AND domain IS NOT NULL AND domain != ''"
    ).fetchall():
        link_or_create_org(
            conn,
            source=SOURCE_HOMEPAGE,
            source_id=str(row["id"]),
            domain=row["domain"],
            name=row["name"],
        )
        n += 1
    counts["homepage_companies"] = n

    # HubSpot companies.
    n = 0
    for row in conn.execute(
        "SELECT id, name, domain FROM hubspot_companies"
    ).fetchall():
        link_or_create_org(
            conn,
            source=SOURCE_HUBSPOT,
            source_id=str(row["id"]),
            domain=row["domain"],
            name=row["name"],
        )
        n += 1
    counts["hubspot_companies"] = n

    # Email-derived contacts.
    n = 0
    for row in conn.execute(
        "SELECT id, name, email FROM contacts WHERE email IS NOT NULL AND email != ''"
    ).fetchall():
        link_or_create_person(
            conn,
            source=SOURCE_EMAIL,
            source_id=str(row["id"]),
            email=row["email"],
            name=row["name"],
        )
        n += 1
    counts["email_contacts"] = n

    # HubSpot contacts.
    n = 0
    for row in conn.execute(
        "SELECT id, firstname, lastname, email FROM hubspot_contacts"
    ).fetchall():
        full_name = " ".join(
            x for x in (row["firstname"], row["lastname"]) if x
        ).strip() or None
        link_or_create_person(
            conn,
            source=SOURCE_HUBSPOT,
            source_id=str(row["id"]),
            email=row["email"],
            name=full_name,
        )
        n += 1
    counts["hubspot_contacts"] = n

    conn.commit()
    return counts
