/**
 * Unified entity query layer.
 *
 * Mirrors `email-analyser/src/email_manager/entities/repository.py`. Stitches
 * email/homepage/hubspot source rows into a single row per organization or
 * person. Field precedence is hardcoded for v1 — see the per-source merge
 * blocks below.
 */

import type { Database, DbRow } from './db.js';

// ── Organizations ─────────────────────────────────────────────────────────

interface OrgListOptions {
  q?: string;
  label?: string;
  stale?: string;
  source?: string;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

interface OrgListItem {
  org_id: number;
  email_company_id: number | null;
  hubspot_id: string | null;
  name: string | null;
  domain: string | null;
  description: string | null;
  email_count: number;
  first_seen: string | null;
  last_seen: string | null;
  homepage_fetched_at: string | null;
  industry: string | null;
  lifecycle_stage: string | null;
  country: string | null;
  is_stale: boolean;
  staleness_status: string;
  last_analysed_at: string | null;
  sources: string[];
  labels: string[];
}

const ORG_LIST_CTE = `
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
`;

export async function listOrganizations(db: Database, opts: OrgListOptions): Promise<{
  items: OrgListItem[];
  total: number;
  labels: string[];
  stale_count: number;
  up_to_date_count: number;
  never_analysed_count: number;
}> {
  const q = opts.q ?? '';
  const label = opts.label ?? '';
  const stale = opts.stale ?? '';
  const source = opts.source ?? '';
  const sort = opts.sort ?? 'email_count';
  const order = (opts.order ?? 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, opts.page ?? 1);
  const limit = Math.min(100, Math.max(1, opts.limit ?? 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = {
    email_count: 'ec.email_count',
    name: 'COALESCE(o.canonical_name, hc.name, ec.name, hp.name)',
    last_seen: 'ec.last_seen',
  };
  const sortCol = allowedSorts[sort] ?? 'ec.email_count';

  const where: string[] = [];
  const params: unknown[] = [];

  if (q) {
    where.push(
      "(COALESCE(o.canonical_name, '') LIKE ? OR COALESCE(o.canonical_domain, '') LIKE ? "
      + "OR COALESCE(ec.name, '') LIKE ? OR COALESCE(hc.name, '') LIKE ? "
      + "OR COALESCE(ec.domain, '') LIKE ? OR COALESCE(hc.domain, '') LIKE ?)"
    );
    const like = `%${q}%`;
    params.push(like, like, like, like, like, like);
  }

  if (label) {
    where.push('ec.id IN (SELECT company_id FROM company_labels WHERE label = ?)');
    params.push(label);
  }

  if (stale === '1') where.push("ec.staleness_status = 'stale'");
  else if (stale === '0') where.push("ec.staleness_status = 'up_to_date'");
  else if (stale === 'never') where.push("(ec.staleness_status = 'never' OR ec.staleness_status IS NULL)");

  if (source === 'email_only') where.push('re.organization_id IS NOT NULL AND rhs.organization_id IS NULL');
  else if (source === 'hubspot_only') where.push('rhs.organization_id IS NOT NULL AND re.organization_id IS NULL');
  else if (source === 'both') where.push('re.organization_id IS NOT NULL AND rhs.organization_id IS NOT NULL');

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const baseFrom = `
    FROM organizations o
    LEFT JOIN ranked_email re ON re.organization_id = o.id
    LEFT JOIN companies ec ON ec.id = re.company_id
    LEFT JOIN ranked_homepage rh ON rh.organization_id = o.id
    LEFT JOIN companies hp ON hp.id = rh.company_id
    LEFT JOIN ranked_hubspot rhs ON rhs.organization_id = o.id
    LEFT JOIN hubspot_companies hc ON hc.id = rhs.hubspot_id
  `;

  const totalRow = await db.queryOne<{ cnt: number }>(
    `${ORG_LIST_CTE} SELECT COUNT(*) AS cnt ${baseFrom} ${whereClause}`,
    ...params
  );
  const total = Number(totalRow?.cnt ?? 0);

  const rows = await db.query<DbRow & {
    org_id: number;
    canonical_name: string | null;
    canonical_domain: string | null;
    email_company_id: number | null;
    email_name: string | null;
    email_domain: string | null;
    email_count: number | null;
    first_seen: string | null;
    last_seen: string | null;
    staleness_status: string | null;
    homepage_description: string | null;
    homepage_fetched_at: string | null;
    homepage_name: string | null;
    hubspot_id: string | null;
    hubspot_name: string | null;
    hubspot_domain: string | null;
    industry: string | null;
    lifecycle_stage: string | null;
    country: string | null;
    hubspot_description: string | null;
    has_email: number | boolean;
    has_homepage: number | boolean;
    has_hubspot: number | boolean;
  }>(
    `${ORG_LIST_CTE}
     SELECT
       o.id AS org_id, o.canonical_name, o.canonical_domain,
       ec.id AS email_company_id, ec.name AS email_name, ec.domain AS email_domain,
       ec.email_count, ec.first_seen, ec.last_seen, ec.staleness_status,
       hp.description AS homepage_description, hp.homepage_fetched_at,
       hp.name AS homepage_name,
       hc.id AS hubspot_id, hc.name AS hubspot_name, hc.domain AS hubspot_domain,
       hc.industry, hc.lifecycle_stage, hc.country,
       hc.description AS hubspot_description,
       (re.organization_id IS NOT NULL) AS has_email,
       (rh.organization_id IS NOT NULL) AS has_homepage,
       (rhs.organization_id IS NOT NULL) AS has_hubspot
     ${baseFrom}
     ${whereClause}
     ORDER BY ${sortCol} ${order} NULLS LAST
     LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const orgIds = rows.map(r => r.org_id);
  const labelsByOrg = await loadLabelsForOrgs(db, orgIds);
  const overrides = await loadOverrides(db, 'organization', orgIds);
  const lastAnalysedByDomain = await loadLastAnalysed(db, rows.map(r => r.canonical_domain || r.email_domain || r.hubspot_domain).filter(Boolean) as string[]);

  const items: OrgListItem[] = rows.map((r) => {
    const sources: string[] = [];
    if (r.has_email) sources.push('email');
    if (r.has_homepage) sources.push('homepage');
    if (r.has_hubspot) sources.push('hubspot');

    const name = r.canonical_name || r.hubspot_name || r.email_name || r.homepage_name;
    const domain = r.canonical_domain || r.email_domain || r.hubspot_domain;
    const description = r.homepage_description || r.hubspot_description;

    const item: OrgListItem = {
      org_id: r.org_id,
      email_company_id: r.email_company_id,
      hubspot_id: r.hubspot_id,
      name,
      domain,
      description,
      email_count: Number(r.email_count ?? 0),
      first_seen: r.first_seen,
      last_seen: r.last_seen,
      homepage_fetched_at: r.homepage_fetched_at,
      industry: r.industry,
      lifecycle_stage: r.lifecycle_stage,
      country: r.country,
      is_stale: r.staleness_status === 'stale',
      staleness_status: r.staleness_status ?? 'never',
      last_analysed_at: domain ? (lastAnalysedByDomain.get(domain.toLowerCase()) ?? null) : null,
      sources,
      labels: labelsByOrg.get(r.org_id) ?? [],
    };
    const ovr = overrides.get(r.org_id);
    if (ovr) Object.assign(item, ovr);
    return item;
  });

  // Counts for the status pills. Same WHERE shape but only counts staleness buckets.
  const staleCounts = await db.queryOne<{ stale: number; up_to_date: number; never_analysed: number }>(
    `${ORG_LIST_CTE}
     SELECT
       SUM(CASE WHEN ec.staleness_status = 'stale' THEN 1 ELSE 0 END) AS stale,
       SUM(CASE WHEN ec.staleness_status = 'up_to_date' THEN 1 ELSE 0 END) AS up_to_date,
       SUM(CASE WHEN ec.staleness_status = 'never' OR ec.staleness_status IS NULL THEN 1 ELSE 0 END) AS never_analysed
     ${baseFrom}
     ${whereClause}`,
    ...params
  );

  const allLabels = (
    await db.query<{ label: string }>('SELECT DISTINCT label FROM company_labels ORDER BY label')
  ).map(r => r.label);

  return {
    items,
    total,
    labels: allLabels,
    stale_count: Number(staleCounts?.stale ?? 0),
    up_to_date_count: Number(staleCounts?.up_to_date ?? 0),
    never_analysed_count: Number(staleCounts?.never_analysed ?? 0),
  };
}

async function loadLabelsForOrgs(db: Database, orgIds: number[]): Promise<Map<number, string[]>> {
  const out = new Map<number, string[]>();
  if (orgIds.length === 0) return out;
  const placeholders = orgIds.map(() => '?').join(',');
  const rows = await db.query<{ org_id: number; label: string }>(
    `SELECT oi.organization_id AS org_id, cl.label
     FROM organization_identities oi
     JOIN company_labels cl ON cl.company_id = CAST(oi.source_id AS INTEGER)
     WHERE oi.source = 'email' AND oi.organization_id IN (${placeholders})`,
    ...orgIds
  );
  for (const r of rows) {
    if (!out.has(r.org_id)) out.set(r.org_id, []);
    if (!out.get(r.org_id)!.includes(r.label)) out.get(r.org_id)!.push(r.label);
  }
  out.forEach((v, k) => { out.set(k, v.sort()); });
  return out;
}

async function loadOverrides(
  db: Database,
  entityType: 'organization' | 'person',
  entityIds: number[],
): Promise<Map<number, Record<string, unknown>>> {
  const out = new Map<number, Record<string, unknown>>();
  if (entityIds.length === 0) return out;
  const placeholders = entityIds.map(() => '?').join(',');
  const rows = await db.query<{ entity_id: number; field: string; value: string | null }>(
    `SELECT entity_id, field, value FROM field_overrides
     WHERE entity_type = ? AND entity_id IN (${placeholders})`,
    entityType, ...entityIds
  );
  for (const r of rows) {
    if (!out.has(r.entity_id)) out.set(r.entity_id, {});
    out.get(r.entity_id)![r.field] = r.value;
  }
  return out;
}

async function loadLastAnalysed(db: Database, domains: string[]): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  const lowered = domains.map(d => d.toLowerCase());
  const unique = Array.from(new Set(lowered));
  if (unique.length === 0) return out;
  const placeholders = unique.map(() => '?').join(',');
  const rows = await db.query<{ domain: string; started_at: string }>(
    `SELECT LOWER(company_domain) AS domain, MAX(started_at) AS started_at
     FROM processing_runs
     WHERE LOWER(company_domain) IN (${placeholders})
       AND mode LIKE 'staged:%' AND error IS NULL
     GROUP BY LOWER(company_domain)`,
    ...unique
  );
  for (const r of rows) {
    if (r.started_at) out.set(r.domain, r.started_at);
  }
  return out;
}

export async function getOrganization(db: Database, orgId: number): Promise<Record<string, unknown> | null> {
  const org = await db.queryOne<DbRow>(
    'SELECT id, canonical_name, canonical_domain, notes, created_at, updated_at FROM organizations WHERE id = ?',
    orgId
  );
  if (!org) return null;

  const identities = await db.query<DbRow>(
    `SELECT source, source_id, match_key, confidence, is_manual, created_at
     FROM organization_identities WHERE organization_id = ? ORDER BY source, source_id`,
    orgId
  );

  // Load source rows, picking the lowest source_id per source for the merge.
  const bySource: Record<string, DbRow> = {};
  for (const ident of identities) {
    const src = ident.source as string;
    if (bySource[src]) continue;
    const sid = ident.source_id as string;
    if (src === 'email' || src === 'homepage') {
      const row = await db.queryOne<DbRow>(
        'SELECT id, name, domain, email_count, first_seen, last_seen, homepage_fetched_at, description, staleness_status FROM companies WHERE id = ?',
        Number(sid)
      );
      if (row) bySource[src] = row;
    } else if (src === 'hubspot') {
      const row = await db.queryOne<DbRow>(
        `SELECT id, name, domain, website, industry, description, about_us,
                city, state, country, phone, num_employees, annual_revenue,
                lifecycle_stage, type, owner_id, founded_year,
                linkedin_url, twitter_handle, hs_url
         FROM hubspot_companies WHERE id = ?`,
        sid
      );
      if (row) bySource[src] = row;
    }
  }

  const overrides = await loadOverrides(db, 'organization', [orgId]);
  const ovr = overrides.get(orgId) ?? {};

  // Field precedence — first non-null wins per field.
  const pick = (...vals: unknown[]) => vals.find(v => v != null && v !== '') ?? null;
  const e = bySource.email ?? {};
  const h = bySource.homepage ?? {};
  const hs = bySource.hubspot ?? {};

  const merged: Record<string, unknown> = {
    id: orgId,
    org_id: orgId,
    email_company_id: e.id ?? null,
    hubspot_id: hs.id ?? null,
    hubspot_url: (hs.hs_url as string | undefined) ?? null,
    name: pick(org.canonical_name, hs.name, e.name, h.name),
    domain: pick(org.canonical_domain, e.domain, hs.domain, h.domain),
    description: pick(h.description, hs.description, hs.about_us),
    website: pick(hs.website),
    industry: pick(hs.industry),
    lifecycle_stage: pick(hs.lifecycle_stage),
    type: pick(hs.type),
    owner_id: pick(hs.owner_id),
    phone: pick(hs.phone),
    city: pick(hs.city),
    state: pick(hs.state),
    country: pick(hs.country),
    num_employees: pick(hs.num_employees),
    annual_revenue: pick(hs.annual_revenue),
    linkedin_url: pick(hs.linkedin_url),
    twitter_handle: pick(hs.twitter_handle),
    founded_year: pick(hs.founded_year),
    email_count: pick(e.email_count, 0),
    first_seen: pick(e.first_seen),
    last_seen: pick(e.last_seen),
    homepage_fetched_at: pick(h.homepage_fetched_at),
    is_stale: e.staleness_status === 'stale',
    staleness_status: pick(e.staleness_status, 'never'),
    notes: org.notes,
    created_at: org.created_at,
    updated_at: org.updated_at,
    sources: Object.keys(bySource),
    identities,
  };
  Object.assign(merged, ovr);
  return merged;
}

// ── People ────────────────────────────────────────────────────────────────

interface PersonListOptions {
  q?: string;
  company?: string;
  source?: string;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}

interface PersonListItem {
  person_id: number;
  email_contact_id: number | null;
  hubspot_id: string | null;
  name: string | null;
  email: string | null;
  company_name: string | null;
  email_count: number;
  sent_count: number;
  received_count: number;
  first_seen: string | null;
  last_seen: string | null;
  job_title: string | null;
  lifecycle_stage: string | null;
  lead_status: string | null;
  country: string | null;
  sources: string[];
}

const PERSON_LIST_CTE = `
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
`;

export async function listPeople(db: Database, opts: PersonListOptions): Promise<{
  items: PersonListItem[];
  total: number;
  companies: string[];
}> {
  const q = opts.q ?? '';
  const company = opts.company ?? '';
  const source = opts.source ?? '';
  const sort = opts.sort ?? 'email_count';
  const order = (opts.order ?? 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, opts.page ?? 1);
  const limit = Math.min(100, Math.max(1, opts.limit ?? 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = {
    email_count: 'ec.email_count',
    name: "COALESCE(p.canonical_name, hc.firstname || ' ' || hc.lastname, ec.name)",
    last_seen: 'ec.last_seen',
  };
  const sortCol = allowedSorts[sort] ?? 'ec.email_count';

  const where: string[] = [];
  const params: unknown[] = [];

  if (q) {
    const like = `%${q}%`;
    where.push(
      "(COALESCE(p.canonical_name, '') LIKE ? OR COALESCE(p.canonical_email, '') LIKE ? "
      + "OR COALESCE(ec.name, '') LIKE ? OR COALESCE(ec.email, '') LIKE ? "
      + "OR COALESCE(hc.firstname, '') LIKE ? OR COALESCE(hc.lastname, '') LIKE ? "
      + "OR COALESCE(hc.email, '') LIKE ?)"
    );
    params.push(like, like, like, like, like, like, like);
  }

  if (company) {
    where.push("(COALESCE(ec.company, '') LIKE ? OR COALESCE(hc.company_name, '') LIKE ?)");
    params.push(`%${company}%`, `%${company}%`);
  }

  if (source === 'email_only') where.push('re.person_id IS NOT NULL AND rhs.person_id IS NULL');
  else if (source === 'hubspot_only') where.push('rhs.person_id IS NOT NULL AND re.person_id IS NULL');
  else if (source === 'both') where.push('re.person_id IS NOT NULL AND rhs.person_id IS NOT NULL');

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const baseFrom = `
    FROM people p
    LEFT JOIN ranked_email re ON re.person_id = p.id
    LEFT JOIN contacts ec ON ec.id = re.contact_id
    LEFT JOIN ranked_hubspot rhs ON rhs.person_id = p.id
    LEFT JOIN hubspot_contacts hc ON hc.id = rhs.hubspot_id
  `;

  const totalRow = await db.queryOne<{ cnt: number }>(
    `${PERSON_LIST_CTE} SELECT COUNT(*) AS cnt ${baseFrom} ${whereClause}`,
    ...params
  );
  const total = Number(totalRow?.cnt ?? 0);

  const rows = await db.query<DbRow>(
    `${PERSON_LIST_CTE}
     SELECT
       p.id AS person_id, p.canonical_name, p.canonical_email,
       ec.id AS email_contact_id, ec.email AS email_email, ec.name AS email_name,
       ec.company AS email_company, ec.first_seen, ec.last_seen,
       ec.email_count, ec.sent_count, ec.received_count,
       hc.id AS hubspot_id, hc.email AS hubspot_email,
       hc.firstname AS hubspot_firstname, hc.lastname AS hubspot_lastname,
       hc.company_name AS hubspot_company_name, hc.job_title, hc.lifecycle_stage,
       hc.lead_status, hc.country,
       (re.person_id IS NOT NULL) AS has_email,
       (rhs.person_id IS NOT NULL) AS has_hubspot
     ${baseFrom}
     ${whereClause}
     ORDER BY ${sortCol} ${order} NULLS LAST
     LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const personIds = rows.map(r => r.person_id as number);
  const overrides = await loadOverrides(db, 'person', personIds);

  const items: PersonListItem[] = rows.map((r) => {
    const sources: string[] = [];
    if (r.has_email) sources.push('email');
    if (r.has_hubspot) sources.push('hubspot');

    const hubspotFull = [r.hubspot_firstname, r.hubspot_lastname]
      .filter(Boolean)
      .join(' ')
      .trim() || null;
    const name = (r.canonical_name as string | null) || hubspotFull || (r.email_name as string | null);
    const email = (r.canonical_email as string | null) || (r.email_email as string | null) || (r.hubspot_email as string | null);
    const companyName = (r.hubspot_company_name as string | null) || (r.email_company as string | null);

    const item: PersonListItem = {
      person_id: r.person_id as number,
      email_contact_id: (r.email_contact_id as number | null) ?? null,
      hubspot_id: (r.hubspot_id as string | null) ?? null,
      name,
      email,
      company_name: companyName,
      email_count: Number(r.email_count ?? 0),
      sent_count: Number(r.sent_count ?? 0),
      received_count: Number(r.received_count ?? 0),
      first_seen: (r.first_seen as string | null) ?? null,
      last_seen: (r.last_seen as string | null) ?? null,
      job_title: (r.job_title as string | null) ?? null,
      lifecycle_stage: (r.lifecycle_stage as string | null) ?? null,
      lead_status: (r.lead_status as string | null) ?? null,
      country: (r.country as string | null) ?? null,
      sources,
    };
    const ovr = overrides.get(item.person_id);
    if (ovr) Object.assign(item, ovr);
    return item;
  });

  const distinctCompanies = (
    await db.query<{ company: string }>(
      'SELECT DISTINCT company FROM contacts WHERE company IS NOT NULL ORDER BY company'
    )
  ).map(r => r.company).filter(Boolean);

  return { items, total, companies: distinctCompanies };
}

export async function getPerson(db: Database, personId: number): Promise<Record<string, unknown> | null> {
  const person = await db.queryOne<DbRow>(
    'SELECT id, canonical_name, canonical_email, notes, created_at, updated_at FROM people WHERE id = ?',
    personId
  );
  if (!person) return null;

  const identities = await db.query<DbRow>(
    `SELECT source, source_id, match_key, confidence, is_manual, created_at
     FROM person_identities WHERE person_id = ? ORDER BY source, source_id`,
    personId
  );

  const bySource: Record<string, DbRow> = {};
  for (const ident of identities) {
    const src = ident.source as string;
    if (bySource[src]) continue;
    const sid = ident.source_id as string;
    if (src === 'email') {
      const row = await db.queryOne<DbRow>(
        `SELECT id, email, name, company, first_seen, last_seen,
                email_count, sent_count, received_count
         FROM contacts WHERE id = ?`,
        Number(sid)
      );
      if (row) bySource[src] = row;
    } else if (src === 'hubspot') {
      const row = await db.queryOne<DbRow>(
        `SELECT id, email, firstname, lastname, company_name, job_title, phone,
                city, state, country, address, lifecycle_stage, lead_status,
                owner_id, twitter_handle, linkedin_url, website, industry, salutation, hs_url
         FROM hubspot_contacts WHERE id = ?`,
        sid
      );
      if (row) bySource[src] = row;
    }
  }

  const overrides = await loadOverrides(db, 'person', [personId]);
  const ovr = overrides.get(personId) ?? {};

  const pick = (...vals: unknown[]) => vals.find(v => v != null && v !== '') ?? null;
  const e = bySource.email ?? {};
  const hs = bySource.hubspot ?? {};
  const hubspotFull = [hs.firstname, hs.lastname].filter(Boolean).join(' ').trim() || null;

  const merged: Record<string, unknown> = {
    id: personId,
    person_id: personId,
    email_contact_id: e.id ?? null,
    hubspot_id: hs.id ?? null,
    hubspot_url: (hs.hs_url as string | undefined) ?? null,
    name: pick(person.canonical_name, hubspotFull, e.name),
    email: pick(person.canonical_email, e.email, hs.email),
    company_name: pick(hs.company_name, e.company),
    job_title: pick(hs.job_title),
    phone: pick(hs.phone),
    city: pick(hs.city),
    state: pick(hs.state),
    country: pick(hs.country),
    address: pick(hs.address),
    lifecycle_stage: pick(hs.lifecycle_stage),
    lead_status: pick(hs.lead_status),
    owner_id: pick(hs.owner_id),
    linkedin_url: pick(hs.linkedin_url),
    twitter_handle: pick(hs.twitter_handle),
    industry: pick(hs.industry),
    salutation: pick(hs.salutation),
    website: pick(hs.website),
    email_count: pick(e.email_count, 0),
    sent_count: pick(e.sent_count, 0),
    received_count: pick(e.received_count, 0),
    first_seen: pick(e.first_seen),
    last_seen: pick(e.last_seen),
    notes: person.notes,
    created_at: person.created_at,
    updated_at: person.updated_at,
    sources: Object.keys(bySource),
    identities,
  };
  Object.assign(merged, ovr);
  return merged;
}

// ── Manual merges ─────────────────────────────────────────────────────────

export async function mergeOrganizations(
  db: Database,
  sourceId: number,
  targetId: number,
  performedBy: string | null,
  notes: string | null,
): Promise<number> {
  if (sourceId === targetId) return 0;
  const now = new Date().toISOString().replace(/\.\d+Z$/, 'Z');
  const result = await db.query<{ cnt: number }>(
    'SELECT COUNT(*) AS cnt FROM organization_identities WHERE organization_id = ?',
    sourceId
  );
  const moved = Number(result[0]?.cnt ?? 0);
  await db.exec(
    `UPDATE organization_identities SET organization_id = ${targetId}, is_manual = 1 WHERE organization_id = ${sourceId}`
  );
  await db.exec(`DELETE FROM organizations WHERE id = ${sourceId}`);
  await db.exec(
    `UPDATE field_overrides SET entity_id = ${targetId} WHERE entity_type = 'organization' AND entity_id = ${sourceId}`
  );
  // Audit row
  await db.query(
    `INSERT INTO entity_merges (entity_type, source_id, target_id, performed_at, performed_by, notes)
     VALUES (?, ?, ?, ?, ?, ?)`,
    'organization', sourceId, targetId, now, performedBy, notes
  );
  await db.query(`UPDATE organizations SET updated_at = ? WHERE id = ?`, now, targetId);
  return moved;
}

export async function mergePersons(
  db: Database,
  sourceId: number,
  targetId: number,
  performedBy: string | null,
  notes: string | null,
): Promise<number> {
  if (sourceId === targetId) return 0;
  const now = new Date().toISOString().replace(/\.\d+Z$/, 'Z');
  const result = await db.query<{ cnt: number }>(
    'SELECT COUNT(*) AS cnt FROM person_identities WHERE person_id = ?',
    sourceId
  );
  const moved = Number(result[0]?.cnt ?? 0);
  await db.exec(
    `UPDATE person_identities SET person_id = ${targetId}, is_manual = 1 WHERE person_id = ${sourceId}`
  );
  await db.exec(`DELETE FROM people WHERE id = ${sourceId}`);
  await db.exec(
    `UPDATE field_overrides SET entity_id = ${targetId} WHERE entity_type = 'person' AND entity_id = ${sourceId}`
  );
  await db.query(
    `INSERT INTO entity_merges (entity_type, source_id, target_id, performed_at, performed_by, notes)
     VALUES (?, ?, ?, ?, ?, ?)`,
    'person', sourceId, targetId, now, performedBy, notes
  );
  await db.query(`UPDATE people SET updated_at = ? WHERE id = ?`, now, targetId);
  return moved;
}
