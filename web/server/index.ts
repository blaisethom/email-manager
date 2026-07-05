import 'dotenv/config';
import express, { Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import yaml from 'js-yaml';
import { createDb, type Database, type DbRow } from './db.js';
import {
  initJobs, createJob, listJobs, getJob, getActiveJob, cancelJob,
  subscribeToLogs, PIPELINE_STAGES, type JobConfig,
} from './jobs.js';
import { hybridSearch } from './search.js';
import {
  listOrganizations, getOrganization, listPeople, getPerson,
  mergeOrganizations, mergePersons,
} from './entities.js';
import { registerReviewRoutes } from './review.js';
import { registerConfigRoutes } from './config.js';
import {
  prefectEnabled, listDeployments, triggerDeployment, getDeploymentByName,
  listFlowRuns, getFlowRun, getFlowRunLogs, cancelFlowRun,
} from './prefect.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const db: Database = createDb();

// Ensure indexes exist for expensive queries (SQLite only)
if (db.backend === 'sqlite') {
  db.exec('CREATE INDEX IF NOT EXISTS idx_emails_folder ON emails(folder, from_address)');
}

// Ensure tables exist (may not yet if Python migration hasn't run)
if (db.backend === 'sqlite') {
  db.exec(`CREATE TABLE IF NOT EXISTS calendar_events (
      id              INTEGER PRIMARY KEY,
      event_id        TEXT UNIQUE NOT NULL,
      calendar_id     TEXT NOT NULL DEFAULT 'primary',
      account_name    TEXT, title TEXT, description TEXT, location TEXT,
      start_time      TEXT NOT NULL, end_time TEXT NOT NULL,
      all_day         INTEGER DEFAULT 0, status TEXT,
      organizer_email TEXT, attendees TEXT, html_link TEXT,
      recurring_event_id TEXT, created_at TEXT, updated_at TEXT,
      fetched_at      TEXT NOT NULL
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS discussion_events (
      discussion_id   INTEGER REFERENCES discussions(id),
      event_id        INTEGER REFERENCES calendar_events(id),
      match_score     REAL, match_reason TEXT,
      PRIMARY KEY (discussion_id, event_id)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS event_ledger (
      id TEXT PRIMARY KEY, thread_id TEXT, source_email_id TEXT,
      source_calendar_event_id TEXT, discussion_id INTEGER REFERENCES discussions(id),
      domain TEXT NOT NULL, type TEXT NOT NULL, actor TEXT, target TEXT,
      event_date TEXT, detail TEXT, confidence REAL,
      model_version TEXT, prompt_version TEXT, created_at TEXT NOT NULL
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS milestones (
      id INTEGER PRIMARY KEY, discussion_id INTEGER REFERENCES discussions(id),
      name TEXT NOT NULL, achieved INTEGER DEFAULT 0, achieved_date TEXT,
      evidence_event_ids TEXT, confidence REAL, last_evaluated_at TEXT,
      UNIQUE(discussion_id, name)
  )`);
}

// Ensure HubSpot task enrichment table exists (created by Python migration v35,
// but guard here so the server works even if migration hasn't run yet)
db.exec(`CREATE TABLE IF NOT EXISTS hubspot_task_threads (
    task_id       TEXT NOT NULL,
    thread_id     TEXT NOT NULL,
    contact_email TEXT NOT NULL,
    PRIMARY KEY (task_id, thread_id)
)`);

// Ensure human_state_override column exists (Python migration v39)
try { db.exec('ALTER TABLE discussions ADD COLUMN human_state_override TEXT'); } catch { /* already exists */ }

// Ensure v41 columns exist (Python migration v41)
try { db.exec("ALTER TABLE milestones ADD COLUMN source TEXT DEFAULT 'ai'"); } catch { /* already exists */ }
try { db.exec("ALTER TABLE proposed_actions ADD COLUMN status TEXT DEFAULT 'open'"); } catch { /* already exists */ }
try { db.exec("ALTER TABLE proposed_actions ADD COLUMN source TEXT DEFAULT 'ai'"); } catch { /* already exists */ }
try { db.exec("ALTER TABLE event_ledger ADD COLUMN human_deleted INTEGER DEFAULT 0"); } catch { /* already exists */ }

// Ensure notes tables exist (Python migration v38)
db.exec(`CREATE TABLE IF NOT EXISTS hubspot_notes (
    id                      TEXT PRIMARY KEY,
    body                    TEXT,
    created_at              TEXT,
    updated_at              TEXT,
    owner_id                TEXT,
    associated_contact_ids  TEXT,
    associated_company_ids  TEXT,
    associated_deal_ids     TEXT,
    hs_url                  TEXT,
    properties_json         TEXT,
    fetched_at              TEXT NOT NULL
)`);
db.exec(`CREATE TABLE IF NOT EXISTS discussion_notes (
    discussion_id  INTEGER NOT NULL,
    note_id        TEXT NOT NULL,
    PRIMARY KEY (discussion_id, note_id)
)`);

// Ensure search tables exist (PG-specific: tsvector column, pgvector extension)
if (db.backend === 'postgres') {
  db.exec(`CREATE EXTENSION IF NOT EXISTS vector`);
  db.exec(`CREATE TABLE IF NOT EXISTS thread_search_docs (
      thread_id       TEXT PRIMARY KEY,
      doc_text        TEXT NOT NULL,
      doc_tsv         TSVECTOR,
      company_domain  TEXT,
      is_important    BOOLEAN DEFAULT FALSE,
      outreach_score  DOUBLE PRECISION DEFAULT 0,
      doc_hash        TEXT,
      created_at      TEXT NOT NULL,
      updated_at      TEXT NOT NULL
  )`);
  db.exec(`ALTER TABLE thread_search_docs ADD COLUMN IF NOT EXISTS doc_tsv TSVECTOR`);
  db.exec(`ALTER TABLE thread_search_docs ADD COLUMN IF NOT EXISTS doc_tsv_simple TSVECTOR`);
  db.exec(`ALTER TABLE thread_search_docs ADD COLUMN IF NOT EXISTS outreach_score DOUBLE PRECISION DEFAULT 0`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_tsd_tsv ON thread_search_docs USING GIN(doc_tsv)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_tsd_tsv_simple ON thread_search_docs USING GIN(doc_tsv_simple)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_tsd_company ON thread_search_docs(company_domain)`);
  db.exec(`CREATE TABLE IF NOT EXISTS thread_embeddings (
      thread_id       TEXT NOT NULL,
      model_name      TEXT NOT NULL,
      embedding       vector NOT NULL,
      doc_hash        TEXT,
      created_at      TEXT NOT NULL,
      PRIMARY KEY (thread_id, model_name)
  )`);
  db.exec(`ALTER TABLE thread_embeddings ADD COLUMN IF NOT EXISTS doc_hash TEXT`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_te_model ON thread_embeddings(model_name)`);

  db.exec(`CREATE TABLE IF NOT EXISTS discussion_search_docs (
      discussion_id   INTEGER PRIMARY KEY,
      doc_text        TEXT NOT NULL,
      doc_tsv         TSVECTOR,
      doc_tsv_simple  TSVECTOR,
      doc_hash        TEXT,
      company_domain  TEXT,
      category        TEXT,
      current_state   TEXT,
      created_at      TEXT NOT NULL,
      updated_at      TEXT NOT NULL
  )`);
  db.exec(`ALTER TABLE discussion_search_docs ADD COLUMN IF NOT EXISTS doc_tsv TSVECTOR`);
  db.exec(`ALTER TABLE discussion_search_docs ADD COLUMN IF NOT EXISTS doc_tsv_simple TSVECTOR`);
  db.exec(`ALTER TABLE discussion_search_docs ADD COLUMN IF NOT EXISTS current_state TEXT`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dsd_tsv ON discussion_search_docs USING GIN(doc_tsv)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dsd_tsv_simple ON discussion_search_docs USING GIN(doc_tsv_simple)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dsd_company ON discussion_search_docs(company_domain)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dsd_category ON discussion_search_docs(category)`);
  db.exec(`CREATE TABLE IF NOT EXISTS discussion_embeddings (
      discussion_id   INTEGER NOT NULL,
      model_name      TEXT NOT NULL,
      embedding       vector NOT NULL,
      doc_hash        TEXT,
      created_at      TEXT NOT NULL,
      PRIMARY KEY (discussion_id, model_name)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_de_model ON discussion_embeddings(model_name)`);
}

console.log(`Database backend: ${db.backend}`);

// ── Load discussion category config ────────────────────────────────────────

interface CategoryConfig {
  name: string;
  description: string;
  states: string[];
  terminal_states: string[];
}

function loadCategoryConfig(): CategoryConfig[] {
  const candidates = [
    path.resolve(__dirname, '../../email-analyser/discussion_categories.yaml'),
    path.resolve(__dirname, '../../discussion_categories.yaml'),
    path.resolve(__dirname, '../../data/discussion_categories.yaml'),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const raw = yaml.load(fs.readFileSync(p, 'utf8')) as { categories?: CategoryConfig[] };
      if (raw?.categories) {
        return raw.categories.map((c: any) => ({
          name: c.name,
          description: c.description ?? '',
          states: c.states ?? c.workflow_states ?? [],
          terminal_states: c.terminal_states ?? [],
        }));
      }
    }
  }
  return [];
}

const categoryConfig = loadCategoryConfig();
console.log(`Loaded ${categoryConfig.length} discussion categories`);

// ── Load company label config ───────────────────────────────────────────────

interface LabelConfig { name: string; description: string; }

function loadLabelConfig(): LabelConfig[] {
  const candidates = [
    path.resolve(__dirname, '../../email-analyser/company_labels.yaml'),
    path.resolve(__dirname, '../../company_labels.yaml'),
    path.resolve(__dirname, '../../data/company_labels.yaml'),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const raw = yaml.load(fs.readFileSync(p, 'utf8')) as { labels?: Array<{ name: string; description: string }> } | null;
      if (raw?.labels && Array.isArray(raw.labels)) {
        return raw.labels.map((l: { name?: unknown; description?: unknown }) => ({
          name: String(l.name ?? ''),
          description: String(l.description ?? ''),
        }));
      }
    }
  }
  // Fallback to labels observed in the database — returned dynamically in /api/meta.
  return [];
}

const labelConfig = loadLabelConfig();
console.log(`Loaded ${labelConfig.length} company label definitions`);

function parseJsonField<T>(value: unknown): T | null {
  if (value == null) return null;
  if (typeof value === 'string') {
    try {
      return JSON.parse(value) as T;
    } catch {
      return null;
    }
  }
  return value as T;
}

const app = express();
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

app.use(express.json());

if (process.env.NODE_ENV === 'production') {
  const distPath = path.resolve(__dirname, '../dist');
  app.use(express.static(distPath));
}

// ── /api/meta ──────────────────────────────────────────────────────────────

app.get('/api/meta', async (_req: Request, res: Response) => {
  const labels = (await db.query<{ label: string }>('SELECT DISTINCT label FROM company_labels ORDER BY label')).map(r => r.label);
  const categories = (await db.query<{ category: string }>('SELECT DISTINCT category FROM discussions WHERE category IS NOT NULL ORDER BY category')).map(r => r.category);
  const states = (await db.query<{ current_state: string }>('SELECT DISTINCT current_state FROM discussions WHERE current_state IS NOT NULL ORDER BY current_state')).map(r => r.current_state);

  const stats = await db.queryOne<{ companies: number; contacts: number; discussions: number; actions: number; emails: number; calendar_events: number }>(
    `SELECT
      (SELECT COUNT(*) FROM companies) AS companies,
      (SELECT COUNT(*) FROM contacts) AS contacts,
      (SELECT COUNT(*) FROM discussions) AS discussions,
      (SELECT COUNT(*) FROM actions) AS actions,
      (SELECT COALESCE(SUM(email_count), 0) FROM companies) AS emails,
      (SELECT COUNT(*) FROM calendar_events) AS calendar_events`
  );

  const userEmailRows = await db.query<{ from_address: string; cnt: number }>(
    `SELECT from_address, COUNT(*) AS cnt FROM emails
     WHERE folder IN ('SENT', 'Sent', 'Sent Items', 'Sent Mail', '[Gmail]/Sent Mail')
       AND from_address IS NOT NULL
     GROUP BY from_address
     ORDER BY cnt DESC`
  );
  const userEmails = userEmailRows.map(r => r.from_address);

  // Read label and category config fresh from disk on each request so edits
  // via the Config page (or direct YAML edits) take effect without a restart.
  const freshLabelConfig = loadLabelConfig();
  const freshCategoryConfig = loadCategoryConfig();

  const effectiveLabelConfig: LabelConfig[] = freshLabelConfig.length > 0
    ? freshLabelConfig
    : labels.map(l => ({ name: l, description: '' }));

  res.json({ labels, categories, states, stats, userEmails, categoryConfig: freshCategoryConfig, labelConfig: effectiveLabelConfig });
});

// ── /api/organizations ─────────────────────────────────────────────────────
// Unified view of companies across email-derived, homepage, and HubSpot
// sources. Each row carries an `org_id` (entity-model PK), `email_company_id`
// (the legacy companies.id when present, used by the existing CompanyDetail
// page and downstream joins), and `hubspot_id` (when present).

app.get('/api/organizations', async (req: Request, res: Response) => {
  const result = await listOrganizations(db, {
    q: (req.query.q as string) ?? '',
    label: (req.query.label as string) ?? '',
    stale: (req.query.stale as string) ?? '',
    source: (req.query.source as string) ?? '',
    sort: (req.query.sort as string) ?? 'email_count',
    order: (req.query.order as string) ?? 'desc',
    page: parseInt(req.query.page as string) || 1,
    limit: parseInt(req.query.limit as string) || 25,
  });
  res.json(result);
});

app.get('/api/organizations/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid organization id' }); return; }
  const org = await getOrganization(db, id);
  if (!org) { res.status(404).json({ error: 'Organization not found' }); return; }
  res.json(org);
});

app.post('/api/organizations/:id/merge', async (req: Request, res: Response) => {
  const sourceId = parseInt(req.params.id, 10);
  const targetId = parseInt((req.body?.target_id ?? '').toString(), 10);
  if (isNaN(sourceId) || isNaN(targetId)) {
    res.status(400).json({ error: 'source_id and target_id must be integers' });
    return;
  }
  const moved = await mergeOrganizations(db, sourceId, targetId, 'web', req.body?.notes ?? null);
  res.json({ moved, source_id: sourceId, target_id: targetId });
});

// ── /api/people ────────────────────────────────────────────────────────────

app.get('/api/people', async (req: Request, res: Response) => {
  const result = await listPeople(db, {
    q: (req.query.q as string) ?? '',
    company: (req.query.company as string) ?? '',
    source: (req.query.source as string) ?? '',
    sort: (req.query.sort as string) ?? 'email_count',
    order: (req.query.order as string) ?? 'desc',
    page: parseInt(req.query.page as string) || 1,
    limit: parseInt(req.query.limit as string) || 25,
  });
  res.json(result);
});

app.get('/api/people/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid person id' }); return; }
  const person = await getPerson(db, id);
  if (!person) { res.status(404).json({ error: 'Person not found' }); return; }
  res.json(person);
});

app.post('/api/people/:id/merge', async (req: Request, res: Response) => {
  const sourceId = parseInt(req.params.id, 10);
  const targetId = parseInt((req.body?.target_id ?? '').toString(), 10);
  if (isNaN(sourceId) || isNaN(targetId)) {
    res.status(400).json({ error: 'source_id and target_id must be integers' });
    return;
  }
  const moved = await mergePersons(db, sourceId, targetId, 'web', req.body?.notes ?? null);
  res.json({ moved, source_id: sourceId, target_id: targetId });
});

// ── /api/companies ─────────────────────────────────────────────────────────

app.get('/api/companies', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const label = (req.query.label as string) ?? '';
  const staleFilter = (req.query.stale as string) ?? '';
  const sort = (req.query.sort as string) ?? 'email_count';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = { email_count: 'c.email_count', name: 'c.name', last_seen: 'c.last_seen' };
  const sortCol = allowedSorts[sort] ?? 'c.email_count';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) { where.push('(c.name LIKE ? OR c.domain LIKE ?)'); params.push(`%${q}%`, `%${q}%`); }
  if (label) { where.push('c.id IN (SELECT company_id FROM company_labels WHERE label = ?)'); params.push(label); }
  // Staleness filter uses pre-computed staleness_status column (refreshed by job manager)
  if (staleFilter === '1') { where.push("c.staleness_status = 'stale'"); }
  else if (staleFilter === '0') { where.push("c.staleness_status = 'up_to_date'"); }
  else if (staleFilter === 'never') { where.push("(c.staleness_status = 'never' OR c.staleness_status IS NULL)"); }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(`SELECT COUNT(*) AS cnt FROM companies c ${whereClause}`, ...params);
  const total = totalRow?.cnt ?? 0;

  const items = await db.query(
    `SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen,
            c.homepage_fetched_at, c.description, c.staleness_status,
            GROUP_CONCAT(cl.label, '||') AS labels_concat,
            (SELECT MAX(pr.started_at) FROM processing_runs pr
             WHERE LOWER(pr.company_domain) = LOWER(c.domain) AND pr.mode LIKE 'staged:%' AND pr.error IS NULL
            ) AS last_analysed_at
     FROM companies c
     LEFT JOIN company_labels cl ON cl.company_id = c.id
     ${whereClause}
     GROUP BY c.id
     ORDER BY ${sortCol} ${order}
     LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const enriched = items.map(({ labels_concat, last_analysed_at, staleness_status, ...rest }: any) => ({
    ...rest,
    labels: labels_concat ? [...new Set((labels_concat as string).split('||'))] : [],
    last_analysed_at: last_analysed_at ?? null,
    is_stale: staleness_status === 'stale',
  }));

  const allLabels = (await db.query<{ label: string }>('SELECT DISTINCT label FROM company_labels ORDER BY label')).map(r => r.label);

  // Staleness counts from pre-computed column (fast — no email table scan)
  const baseWhere: string[] = [];
  const baseParams: unknown[] = [];
  if (q) { baseWhere.push('(c.name LIKE ? OR c.domain LIKE ?)'); baseParams.push(`%${q}%`, `%${q}%`); }
  if (label) { baseWhere.push('c.id IN (SELECT company_id FROM company_labels WHERE label = ?)'); baseParams.push(label); }
  const baseWhereClause = baseWhere.length > 0 ? 'WHERE ' + baseWhere.join(' AND ') : '';

  const staleCountRow = await db.queryOne<{ stale: number; up_to_date: number; never_analysed: number }>(
    `SELECT
       SUM(CASE WHEN c.staleness_status = 'stale' THEN 1 ELSE 0 END) AS stale,
       SUM(CASE WHEN c.staleness_status = 'up_to_date' THEN 1 ELSE 0 END) AS up_to_date,
       SUM(CASE WHEN c.staleness_status = 'never' OR c.staleness_status IS NULL THEN 1 ELSE 0 END) AS never_analysed
     FROM companies c ${baseWhereClause}`,
    ...baseParams
  );

  res.json({
    items: enriched, total, labels: allLabels,
    stale_count: Number(staleCountRow?.stale ?? 0),
    up_to_date_count: Number(staleCountRow?.up_to_date ?? 0),
    never_analysed_count: Number(staleCountRow?.never_analysed ?? 0),
  });
});

// ── /api/companies/by-domain/:domain ───────────────────────────────────────

app.get('/api/companies/by-domain/:domain', async (req: Request, res: Response) => {
  const company = await db.queryOne<{ id: number; name: string; domain: string }>(
    'SELECT id, name, domain FROM companies WHERE LOWER(domain) = LOWER(?)', req.params.domain
  );
  if (!company) { res.status(404).json({ error: 'Company not found' }); return; }
  res.json(company);
});

// ── /api/companies/:id ─────────────────────────────────────────────────────

app.get('/api/companies/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid company id' }); return; }

  const company = await db.queryOne('SELECT * FROM companies WHERE id = ?', id);
  if (!company) { res.status(404).json({ error: 'Company not found' }); return; }

  const labels = await db.query(
    'SELECT label, confidence, reasoning, model_used, assigned_at FROM company_labels WHERE company_id = ?', id
  );

  const contacts = await db.query(
    `SELECT ct.id, ct.email, ct.name, ct.email_count, ct.sent_count, ct.received_count, ct.last_seen
     FROM contacts ct INNER JOIN company_contacts cc ON cc.contact_email = ct.email
     WHERE cc.company_id = ? ORDER BY ct.email_count DESC LIMIT 50`, id
  );

  const discussionsRaw = await db.query(
    `SELECT id, title, category, current_state, summary, participants, first_seen, last_seen
     FROM discussions WHERE company_id = ? AND parent_id IS NULL ORDER BY last_seen DESC`, id
  );
  const discussions = discussionsRaw.map((d: any) => ({ ...d, participants: parseJsonField<string[]>(d.participants) ?? [] }));

  // Email threads for this company (via contacts)
  const like = `%@${(company as any).domain}%`;
  const threadsRaw = await db.query<{
    thread_id: string; subject: string | null; email_count: number;
    first_date: string | null; last_date: string | null; summary: string | null;
  }>(
    `SELECT t.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.summary
     FROM threads t
     WHERE t.thread_id IN (
       SELECT DISTINCT e.thread_id FROM emails e
       WHERE e.thread_id IS NOT NULL AND (e.from_address LIKE ? OR e.to_addresses LIKE ? OR e.cc_addresses LIKE ?)
     )
     ORDER BY t.last_date DESC
     LIMIT 50`,
    like, like, like
  );

  // Fetch discussion memberships for these threads in one pass.
  const threadIdList = threadsRaw.map(t => t.thread_id);
  let threadDiscussions = new Map<string, Array<{ id: number; title: string; category: string | null; current_state: string | null }>>();
  if (threadIdList.length > 0) {
    const placeholders = threadIdList.map(() => '?').join(',');
    const rows = await db.query<{
      thread_id: string; id: number; title: string;
      category: string | null; current_state: string | null; last_seen: string | null;
    }>(
      `SELECT dt.thread_id, d.id, d.title, d.category, d.current_state, d.last_seen
       FROM discussion_threads dt
       JOIN discussions d ON d.id = dt.discussion_id
       WHERE dt.thread_id IN (${placeholders})
       ORDER BY d.last_seen DESC NULLS LAST`,
      ...threadIdList
    );
    for (const r of rows) {
      const list = threadDiscussions.get(r.thread_id) ?? [];
      list.push({ id: r.id, title: r.title, category: r.category, current_state: r.current_state });
      threadDiscussions.set(r.thread_id, list);
    }
  }
  const threadsEnriched = threadsRaw.map(t => ({
    ...t,
    discussions: threadDiscussions.get(t.thread_id) ?? [],
  }));

  // Staleness from pre-computed column + last analysed date
  const domain = (company as any).domain;
  const stalenessStatus = (company as any).staleness_status ?? 'never';

  const lastRunRow = domain ? await db.queryOne<{ started_at: string }>(
    `SELECT MAX(started_at) AS started_at
     FROM processing_runs WHERE LOWER(company_domain) = LOWER(?) AND mode LIKE 'staged:%' AND error IS NULL`,
    domain
  ) : null;
  const lastAnalysedAt = lastRunRow?.started_at ?? null;

  // For the detail page, get the exact new email count (single company — fast enough)
  let newEmailCount = 0;
  if (stalenessStatus === 'stale' && domain) {
    const cutoffRow = await db.queryOne<{ email_cutoff_date: string }>(
      `SELECT email_cutoff_date FROM processing_runs
       WHERE LOWER(company_domain) = LOWER(?) AND mode = 'staged:extract_events' AND error IS NULL
       ORDER BY id DESC LIMIT 1`,
      domain
    );
    if (cutoffRow?.email_cutoff_date) {
      const countRow = await db.queryOne<{ cnt: number }>(
        `SELECT COUNT(*) AS cnt FROM emails
         WHERE (from_address LIKE ? OR to_addresses LIKE ?) AND date > ?`,
        `%@${domain}%`, `%@${domain}%`, cutoffRow.email_cutoff_date
      );
      newEmailCount = countRow?.cnt ?? 0;
    }
  } else if (stalenessStatus === 'never' || !lastAnalysedAt) {
    newEmailCount = (company as any).email_count ?? 0;
  }

  // Look up the unified org and any HubSpot identity attached to this email-derived company.
  const orgRow = await db.queryOne<{ organization_id: number }>(
    `SELECT organization_id FROM organization_identities
     WHERE source = 'email' AND source_id = ? LIMIT 1`,
    String(id)
  );
  let hubspot: Record<string, unknown> | null = null;
  let sources: string[] = ['email'];
  if (orgRow) {
    const sourceRows = await db.query<{ source: string }>(
      `SELECT DISTINCT source FROM organization_identities WHERE organization_id = ?`,
      orgRow.organization_id
    );
    sources = sourceRows.map(r => r.source).sort();
    const hsIdent = await db.queryOne<{ source_id: string }>(
      `SELECT source_id FROM organization_identities
       WHERE organization_id = ? AND source = 'hubspot'
       ORDER BY source_id LIMIT 1`,
      orgRow.organization_id
    );
    if (hsIdent) {
      hubspot = await db.queryOne<DbRow>(
        `SELECT id, name, domain, website, industry, description, about_us,
                city, state, country, phone, num_employees, annual_revenue,
                lifecycle_stage, type, owner_id, founded_year,
                linkedin_url, twitter_handle, hs_updated_at, hs_url
         FROM hubspot_companies WHERE id = ?`,
        hsIdent.source_id
      ) ?? null;
    }
  }

  res.json({
    ...company, labels, contacts, discussions, threads: threadsEnriched,
    last_analysed_at: lastAnalysedAt,
    is_stale: stalenessStatus === 'stale',
    new_email_count: newEmailCount,
    org_id: orgRow?.organization_id ?? null,
    sources,
    hubspot,
  });
});

// ── /api/companies/:id/homepage ────────────────────────────────────────────

app.get('/api/companies/:id/homepage', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid company id' }); return; }

  const company = await db.queryOne<{ domain: string | null; homepage_fetched_at: string | null }>(
    'SELECT domain, homepage_fetched_at FROM companies WHERE id = ?', id
  );
  if (!company) { res.status(404).json({ error: 'Company not found' }); return; }
  if (!company.homepage_fetched_at || !company.domain) { res.status(404).json({ error: 'Homepage not fetched' }); return; }

  const candidates = [
    path.resolve(__dirname, '../../data/homepages', `${company.domain}.md`),
    path.resolve(__dirname, '../../email-analyser/data/homepages', `${company.domain}.md`),
  ];
  for (const filePath of candidates) {
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      res.json({ content, domain: company.domain, fetched_at: company.homepage_fetched_at });
      return;
    }
  }
  res.status(404).json({ error: 'Homepage file not found on disk' });
});

// ── /api/companies/:id/insights ────────────────────────────────────────────

app.get('/api/companies/:id/insights', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid company id' }); return; }

  const company = await db.queryOne<{ id: number; domain: string; name: string }>(
    'SELECT id, domain, name FROM companies WHERE id = ?', id
  );
  if (!company) { res.status(404).json({ error: 'Company not found' }); return; }

  // Processing runs for this company, with total LLM duration
  const runs = await db.query(
    `SELECT pr.id, pr.mode, pr.model, pr.started_at, pr.completed_at,
            pr.events_created, pr.discussions_created, pr.discussions_updated, pr.actions_proposed,
            pr.input_tokens, pr.output_tokens, pr.llm_calls, pr.error,
            (SELECT COALESCE(SUM(lc.duration_ms), 0) FROM llm_calls lc WHERE lc.run_id = pr.id) AS total_llm_ms
     FROM processing_runs pr WHERE LOWER(pr.company_domain) = LOWER(?)
     ORDER BY pr.started_at DESC LIMIT 20`,
    company.domain
  );

  // LLM call breakdown by stage for this company's runs
  const runIds = runs.map((r: any) => r.id).filter(Boolean);
  let llmCallsByStage: any[] = [];
  if (runIds.length > 0) {
    // Build query with individual params (can't use IN with dynamic list easily in the abstraction)
    llmCallsByStage = await db.query(
      `SELECT stage, COUNT(*) AS call_count,
              SUM(input_tokens) AS total_input, SUM(output_tokens) AS total_output
       FROM llm_calls WHERE run_id IN (${runIds.map((_: any, i: number) => '?').join(',')})
       GROUP BY stage ORDER BY total_input DESC`,
      ...runIds
    );
  }

  // Discussion health: state, last update, staleness
  const discussions = await db.query(
    `SELECT d.id, d.title, d.category, d.current_state, d.summary,
            d.first_seen, d.last_seen, d.updated_at, d.parent_id, d.run_id,
            (SELECT COUNT(*) FROM event_ledger el WHERE el.discussion_id = d.id) AS event_count,
            (SELECT MAX(el.created_at) FROM event_ledger el WHERE el.discussion_id = d.id) AS latest_event_created,
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id AND (pa.status IS NULL OR pa.status != 'rejected')) AS action_count,
            (SELECT COUNT(*) FROM milestones m WHERE m.discussion_id = d.id AND m.achieved = 1 AND (m.source IS NULL OR m.source != 'human_deleted')) AS milestones_achieved,
            (SELECT COUNT(*) FROM milestones m WHERE m.discussion_id = d.id AND (m.source IS NULL OR m.source != 'human_deleted')) AS milestones_total,
            pr.mode AS last_run_mode, pr.model AS last_run_model
     FROM discussions d
     LEFT JOIN processing_runs pr ON d.run_id = pr.id
     WHERE d.company_id = ?
     ORDER BY d.last_seen DESC`,
    id
  );

  // Events summary: count by domain, freshness
  const eventsByDomain = await db.query(
    `SELECT el.domain, COUNT(*) AS cnt,
            MAX(el.event_date) AS latest_event_date,
            MAX(el.created_at) AS latest_created
     FROM event_ledger el
     JOIN discussions d ON el.discussion_id = d.id
     WHERE d.company_id = ?
     GROUP BY el.domain`,
    id
  );

  // Unprocessed threads (threads with new emails not yet extracted)
  const like = `%@${company.domain}%`;
  const unprocessedThreads = await db.queryOne<{ cnt: number }>(
    `SELECT COUNT(DISTINCT e.thread_id) AS cnt
     FROM emails e
     WHERE e.thread_id IS NOT NULL
       AND (
           e.thread_id NOT IN (
               SELECT DISTINCT el.thread_id FROM event_ledger el
               WHERE el.thread_id IS NOT NULL
           )
           OR e.thread_id IN (
               SELECT el2.thread_id FROM event_ledger el2
               WHERE el2.thread_id IS NOT NULL
               GROUP BY el2.thread_id
               HAVING MAX(el2.created_at) < (
                   SELECT MAX(e2.date) FROM emails e2
                   WHERE e2.thread_id = el2.thread_id
               )
           )
       )
       AND e.thread_id IN (
           SELECT DISTINCT e2.thread_id FROM emails e2
           WHERE e2.from_address LIKE ? OR e2.to_addresses LIKE ? OR e2.cc_addresses LIKE ?
       )`,
    like, like, like
  );

  // Pending change journal entries
  const pendingChanges = await db.queryOne<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM change_journal
     WHERE processed_at IS NULL AND entity_type = 'company' AND entity_id = ?`,
    company.domain
  );

  // Proposed actions for all discussions
  const proposedActions = await db.query(
    `SELECT pa.id, pa.action, pa.reasoning, pa.priority, pa.wait_until, pa.assignee, pa.created_at,
            d.id AS discussion_id, d.title AS discussion_title
     FROM proposed_actions pa
     JOIN discussions d ON pa.discussion_id = d.id
     WHERE d.company_id = ?
     ORDER BY
       CASE pa.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,
       pa.created_at DESC`,
    id
  );

  res.json({
    company: { id: company.id, domain: company.domain, name: company.name },
    processing_runs: runs,
    llm_calls_by_stage: llmCallsByStage,
    discussions,
    events_by_domain: eventsByDomain,
    unprocessed_threads: unprocessedThreads?.cnt ?? 0,
    pending_changes: pendingChanges?.cnt ?? 0,
    proposed_actions: proposedActions,
  });
});

// ── /api/contacts ──────────────────────────────────────────────────────────

app.get('/api/contacts', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const company = (req.query.company as string) ?? '';
  const sort = (req.query.sort as string) ?? 'email_count';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = { email_count: 'ct.email_count', name: 'ct.name', last_seen: 'ct.last_seen' };
  const sortCol = allowedSorts[sort] ?? 'ct.email_count';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) { where.push('(ct.name LIKE ? OR ct.email LIKE ?)'); params.push(`%${q}%`, `%${q}%`); }
  if (company) { where.push('ct.company LIKE ?'); params.push(`%${company}%`); }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(`SELECT COUNT(*) AS cnt FROM contacts ct ${whereClause}`, ...params);
  const total = totalRow?.cnt ?? 0;

  const items = await db.query(
    `SELECT ct.id, ct.email, ct.name, ct.company, ct.first_seen, ct.last_seen,
            ct.email_count, ct.sent_count, ct.received_count
     FROM contacts ct ${whereClause} ORDER BY ${sortCol} ${order} LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const companies = (await db.query<{ company: string }>(
    'SELECT DISTINCT company FROM contacts WHERE company IS NOT NULL ORDER BY company'
  )).map(r => r.company);

  res.json({ items, total, companies });
});

// ── /api/contacts/:email ───────────────────────────────────────────────────

app.get('/api/contacts/:email', async (req: Request, res: Response) => {
  const email = decodeURIComponent(req.params.email);

  const contact = await db.queryOne('SELECT * FROM contacts WHERE email = ?', email);
  if (!contact) { res.status(404).json({ error: 'Contact not found' }); return; }

  const memoryRaw = await db.queryOne('SELECT * FROM contact_memories WHERE email = ?', email);
  const memory = memoryRaw ? {
    ...memoryRaw,
    discussions: parseJsonField<Array<{ topic: string; status: string }>>((memoryRaw as any).discussions) ?? [],
    key_facts: parseJsonField<string[]>((memoryRaw as any).key_facts) ?? [],
  } : null;

  let threads = await db.query(
    `SELECT t.id, t.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.participants, t.summary
     FROM threads t
     INNER JOIN (
       SELECT DISTINCT dt.thread_id FROM discussion_threads dt
       INNER JOIN discussions d ON d.id = dt.discussion_id
       INNER JOIN company_contacts cc ON cc.company_id = d.company_id
       WHERE cc.contact_email = ?
     ) linked ON linked.thread_id = t.thread_id
     ORDER BY t.last_date DESC LIMIT 20`, email
  );

  if (threads.length === 0) {
    threads = await db.query(
      `SELECT id, thread_id, subject, email_count, first_date, last_date, participants, summary
       FROM threads WHERE participants LIKE ? ORDER BY last_date DESC LIMIT 20`,
      `%${email}%`
    );
  }

  const enrichedThreads = threads.map((t: any) => ({ ...t, participants: parseJsonField<string[]>(t.participants) ?? [] }));

  // Look up the unified person and any HubSpot identity attached to this contact.
  const personRow = await db.queryOne<{ person_id: number }>(
    `SELECT person_id FROM person_identities
     WHERE source = 'email' AND match_key = LOWER(?) LIMIT 1`,
    email
  );
  let hubspot: Record<string, unknown> | null = null;
  let sources: string[] = ['email'];
  if (personRow) {
    const sourceRows = await db.query<{ source: string }>(
      `SELECT DISTINCT source FROM person_identities WHERE person_id = ?`,
      personRow.person_id
    );
    sources = sourceRows.map(r => r.source).sort();
    const hsIdent = await db.queryOne<{ source_id: string }>(
      `SELECT source_id FROM person_identities
       WHERE person_id = ? AND source = 'hubspot'
       ORDER BY source_id LIMIT 1`,
      personRow.person_id
    );
    if (hsIdent) {
      hubspot = await db.queryOne<DbRow>(
        `SELECT id, email, firstname, lastname, company_name, job_title, phone,
                city, state, country, address, lifecycle_stage, lead_status,
                owner_id, twitter_handle, linkedin_url, website, industry, salutation,
                hs_updated_at, hs_url
         FROM hubspot_contacts WHERE id = ?`,
        hsIdent.source_id
      ) ?? null;
    }
  }

  res.json({
    ...contact, memory, threads: enrichedThreads,
    person_id: personRow?.person_id ?? null,
    sources,
    hubspot,
  });
});

// ── /api/discussions ───────────────────────────────────────────────────────

app.get('/api/discussions', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const category = (req.query.category as string) ?? '';
  const state = (req.query.state as string) ?? '';
  const exclude_states = (req.query.exclude_states as string) ?? '';
  const company_id = (req.query.company_id as string) ?? '';
  const sort = (req.query.sort as string) ?? 'last_seen';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = { last_seen: 'd.last_seen', first_seen: 'd.first_seen', title: 'd.title' };
  const sortCol = allowedSorts[sort] ?? 'd.last_seen';

  const params: unknown[] = [];
  const where: string[] = ['d.parent_id IS NULL'];

  if (q) { where.push('(d.title LIKE ? OR d.summary LIKE ? OR c.name LIKE ?)'); params.push(`%${q}%`, `%${q}%`, `%${q}%`); }
  if (category) { where.push('d.category = ?'); params.push(category); }
  if (state) { where.push('d.current_state = ?'); params.push(state); }
  if (exclude_states) {
    const excluded = exclude_states.split(',').filter(Boolean);
    if (excluded.length > 0) {
      const placeholders = excluded.map(() => '?').join(', ');
      where.push(`(d.current_state IS NULL OR d.current_state NOT IN (${placeholders}))`);
      params.push(...excluded);
    }
  }
  if (company_id) { where.push('d.company_id = ?'); params.push(company_id); }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM discussions d LEFT JOIN companies c ON c.id = d.company_id ${whereClause}`, ...params
  );
  const total = totalRow?.cnt ?? 0;

  const items = await db.query(
    `SELECT d.id, d.title, d.category, d.current_state, d.company_id, d.parent_id, d.summary,
            d.participants, d.first_seen, d.last_seen, d.updated_at,
            c.name AS company_name,
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id AND (pa.status IS NULL OR pa.status != 'rejected')) AS proposed_action_count,
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id AND pa.priority = 'high' AND (pa.status IS NULL OR pa.status != 'rejected')) AS high_priority_count,
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id AND pa.priority = 'medium' AND (pa.status IS NULL OR pa.status != 'rejected')) AS med_priority_count
     FROM discussions d LEFT JOIN companies c ON c.id = d.company_id
     ${whereClause} ORDER BY ${sortCol} ${order} LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const enriched = items.map((d: any) => ({ ...d, participants: parseJsonField<string[]>(d.participants) ?? [] }));

  const categoriesResult = (await db.query<{ category: string }>(
    'SELECT DISTINCT category FROM discussions WHERE category IS NOT NULL ORDER BY category'
  )).map(r => r.category);

  const statesResult = (await db.query<{ current_state: string }>(
    'SELECT DISTINCT current_state FROM discussions WHERE current_state IS NOT NULL ORDER BY current_state'
  )).map(r => r.current_state);

  res.json({ items: enriched, total, categories: categoriesResult, states: statesResult });
});

// ── /api/discussions/:id ───────────────────────────────────────────────────

app.get('/api/discussions/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid discussion id' }); return; }

  const discussion = await db.queryOne(
    `SELECT d.*, c.name AS company_name FROM discussions d
     LEFT JOIN companies c ON c.id = d.company_id WHERE d.id = ?`, id
  );
  if (!discussion) { res.status(404).json({ error: 'Discussion not found' }); return; }

  const stateHistory = await db.query(
    `SELECT id, state, entered_at, reasoning, model_used, detected_at
     FROM discussion_state_history WHERE discussion_id = ? ORDER BY entered_at ASC`, id
  );

  const threadsRaw = await db.query(
    `SELECT t.id, t.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.participants, t.summary
     FROM threads t INNER JOIN discussion_threads dt ON dt.thread_id = t.thread_id
     WHERE dt.discussion_id = ? ORDER BY t.last_date DESC`, id
  );
  const threads = threadsRaw.map((t: any) => ({ ...t, participants: parseJsonField<string[]>(t.participants) ?? [] }));

  const actionsRaw = await db.query(
    `SELECT id, description, assignee_emails, target_date, status, source_date, completed_date
     FROM actions WHERE discussion_id = ? ORDER BY status ASC, source_date ASC`, id
  );
  const actions = actionsRaw.map((a: any) => ({ ...a, assignee_emails: parseJsonField<string[]>(a.assignee_emails) ?? [] }));

  const calendarEventsRaw = await db.query(
    `SELECT ce.id, ce.event_id, ce.title, ce.description, ce.location,
            ce.start_time, ce.end_time, ce.all_day, ce.status,
            ce.organizer_email, ce.attendees, ce.html_link,
            de.match_score, de.match_reason
     FROM calendar_events ce INNER JOIN discussion_events de ON de.event_id = ce.id
     WHERE de.discussion_id = ? ORDER BY ce.start_time DESC`, id
  );
  const calendarEvents = calendarEventsRaw.map((e: any) => ({
    ...e, all_day: !!e.all_day,
    attendees: parseJsonField<Array<{ email: string; name?: string; response_status?: string }>>(e.attendees) ?? [],
  }));

  const events = await db.query(
    `SELECT id, domain, type, actor, target, event_date, detail, confidence, thread_id, source_email_id
     FROM event_ledger WHERE discussion_id = ? ORDER BY event_date ASC, created_at ASC`, id
  );

  const milestonesRaw = await db.query(
    `SELECT id, name, achieved, achieved_date, evidence_event_ids, confidence, source
     FROM milestones WHERE discussion_id = ? AND (source IS NULL OR source != 'human_deleted')
     ORDER BY achieved DESC, achieved_date ASC NULLS LAST`, id
  );
  const milestones = milestonesRaw.map((m: any) => ({
    ...m, achieved: !!m.achieved,
    evidence_event_ids: parseJsonField<string[]>(m.evidence_event_ids) ?? [],
    source: m.source ?? 'ai',
  }));

  const proposedActions = await db.query(
    `SELECT id, action, reasoning, priority, wait_until, assignee, created_at, status, source
     FROM proposed_actions WHERE discussion_id = ? AND (status IS NULL OR status != 'rejected')
     ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, id ASC`, id
  ).catch(() => []);

  const notes = await db.query(
    `SELECT hn.id, hn.body, hn.created_at, hn.updated_at, hn.owner_id, hn.hs_url
     FROM hubspot_notes hn INNER JOIN discussion_notes dn ON dn.note_id = hn.id
     WHERE dn.discussion_id = ? ORDER BY hn.created_at ASC`, id
  ).catch(() => []);

  const childrenRaw = await db.query(
    `SELECT d.id, d.title, d.category, d.current_state, d.company_id, d.parent_id, d.summary,
            d.participants, d.first_seen, d.last_seen, d.updated_at, c.name AS company_name
     FROM discussions d LEFT JOIN companies c ON c.id = d.company_id
     WHERE d.parent_id = ? ORDER BY d.last_seen DESC`, id
  );
  const children = childrenRaw.map((d: any) => ({ ...d, participants: parseJsonField<string[]>(d.participants) ?? [] }));

  // If this is a sub-discussion, fetch parent details
  let parent = null;
  if ((discussion as any).parent_id) {
    parent = await db.queryOne(
      `SELECT d.id, d.title, d.category, d.current_state, d.summary,
              d.first_seen, d.last_seen, c.name AS company_name
       FROM discussions d LEFT JOIN companies c ON c.id = d.company_id
       WHERE d.id = ?`,
      (discussion as any).parent_id
    ) ?? null;
  }

  res.json({
    ...discussion,
    participants: parseJsonField<string[]>((discussion as any).participants) ?? [],
    parent,
    state_history: stateHistory, threads, actions,
    calendar_events: calendarEvents, events, milestones,
    proposed_actions: proposedActions, children, notes,
  });
});

// ── /api/discussions/:id/proposed-actions ──────────────────────────────────

app.get('/api/discussions/:id/proposed-actions', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid discussion id' }); return; }

  const actions = await db.query(
    `SELECT id, action, reasoning, priority, wait_until, assignee, created_at, status, source
     FROM proposed_actions WHERE discussion_id = ? AND (status IS NULL OR status != 'rejected')
     ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, id ASC`, id
  ).catch(() => []);
  res.json(actions);
});

// ── Feedback mutations on discussions + events + threads ──────────────────
//
// Every edit writes to the `feedback` table (layer + target_type + target_id +
// action + old_value + new_value + reason). Phase 2 will also snapshot the
// LLM input context into few_shot_examples — for now we record the correction
// only. The `feedback` rows are consumed later by format_examples_block() /
// format_rules_block() in analysis/feedback.py.

function nowIso(): string { return new Date().toISOString(); }

async function recordFeedback(
  layer: string,
  targetType: string,
  targetId: string | number,
  action: string,
  oldValue: unknown,
  newValue: unknown,
  reason: string | null | undefined,
): Promise<void> {
  const toText = (v: unknown): string | null => {
    if (v === undefined || v === null) return null;
    if (typeof v === 'string') return v;
    return JSON.stringify(v);
  };
  await db.query(
    `INSERT INTO feedback (layer, target_type, target_id, action, old_value, new_value, reason, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    layer, targetType, String(targetId), action, toText(oldValue), toText(newValue), reason ?? null, nowIso(),
  );
}

// PATCH /api/discussions/:id — edit title / summary (one or both).
// Records a feedback row per changed field so the learning loop has fine-grained
// corrections to draw on.
app.patch('/api/discussions/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid discussion id' }); return; }

  const { title, summary, state, reason } = (req.body ?? {}) as {
    title?: string | null; summary?: string | null; state?: string | null; reason?: string | null;
  };
  if (title === undefined && summary === undefined && state === undefined) {
    res.status(400).json({ error: 'Provide at least one of: title, summary, state' });
    return;
  }

  const existing = await db.queryOne<{ title: string; summary: string | null; current_state: string | null }>(
    'SELECT title, summary, current_state FROM discussions WHERE id = ?', id,
  );
  if (!existing) { res.status(404).json({ error: 'Discussion not found' }); return; }

  const now = nowIso();
  const changes: string[] = [];

  if (title !== undefined && title !== existing.title) {
    await db.query('UPDATE discussions SET title = ?, updated_at = ? WHERE id = ?', title, now, id);
    await recordFeedback('discussions', 'discussion', id, 'title_change', existing.title, title, reason);
    changes.push('title');
  }
  if (summary !== undefined && summary !== existing.summary) {
    await db.query('UPDATE discussions SET summary = ?, updated_at = ? WHERE id = ?', summary, now, id);
    await recordFeedback('discussions', 'discussion', id, 'summary_change', existing.summary, summary, reason);
    changes.push('summary');
  }
  if (state !== undefined && state !== existing.current_state) {
    await db.query(
      'UPDATE discussions SET current_state = ?, updated_at = ? WHERE id = ?', state, now, id,
    );
    // Record in the state history so the timeline stays accurate
    await db.query(
      `INSERT INTO discussion_state_history (discussion_id, state, entered_at, reasoning, model_used, detected_at)
       VALUES (?, ?, ?, ?, 'human', ?)`,
      id, state, now, reason ?? null, now,
    );
    await recordFeedback('discussions', 'discussion', id, 'state_override', existing.current_state, state, reason);
    changes.push('state');
  }

  res.json({ id, updated: changes });
});

// POST /api/discussions/:id/merge — merge source discussion into this (target).
// Moves events, threads, actions, proposed_actions, milestones, state history,
// calendar event links, and re-parents sub-discussions. Ports the CLI
// `merge-discussions` command (cli.py:1056).
app.post('/api/discussions/:id/merge', async (req: Request, res: Response) => {
  const targetId = parseInt(req.params.id, 10);
  const { source_id, reason } = (req.body ?? {}) as { source_id?: number; reason?: string | null };
  if (isNaN(targetId) || !source_id || isNaN(Number(source_id))) {
    res.status(400).json({ error: 'Invalid target or source id' });
    return;
  }
  const sourceId = Number(source_id);
  if (sourceId === targetId) {
    res.status(400).json({ error: 'source_id must differ from target id' });
    return;
  }

  const target = await db.queryOne<{ id: number; title: string; first_seen: string; last_seen: string }>(
    'SELECT id, title, first_seen, last_seen FROM discussions WHERE id = ?', targetId,
  );
  const source = await db.queryOne<{ id: number; title: string; first_seen: string; last_seen: string }>(
    'SELECT id, title, first_seen, last_seen FROM discussions WHERE id = ?', sourceId,
  );
  if (!target) { res.status(404).json({ error: 'Target discussion not found' }); return; }
  if (!source) { res.status(404).json({ error: 'Source discussion not found' }); return; }

  const now = nowIso();

  // Move child tables from source → target. Use UPDATE OR IGNORE (SQLite) /
  // ON CONFLICT DO NOTHING (PG) semantics where there's a uniqueness constraint.
  await db.query('UPDATE event_ledger SET discussion_id = ? WHERE discussion_id = ?', targetId, sourceId);
  await db.query('UPDATE actions SET discussion_id = ? WHERE discussion_id = ?', targetId, sourceId);
  await db.query('UPDATE proposed_actions SET discussion_id = ? WHERE discussion_id = ?', targetId, sourceId);
  await db.query('UPDATE discussion_state_history SET discussion_id = ? WHERE discussion_id = ?', targetId, sourceId);
  await db.query('UPDATE discussions SET parent_id = ? WHERE parent_id = ?', targetId, sourceId);

  // Composite-PK tables: delete source rows that already exist on target, then move the rest.
  for (const table of ['discussion_threads', 'discussion_events', 'milestones'] as const) {
    const joinCol = table === 'discussion_threads' ? 'thread_id' : table === 'discussion_events' ? 'event_id' : 'name';
    await db.query(
      `DELETE FROM ${table} WHERE discussion_id = ? AND ${joinCol} IN (
         SELECT ${joinCol} FROM ${table} WHERE discussion_id = ?
       )`,
      sourceId, targetId,
    );
    await db.query(`UPDATE ${table} SET discussion_id = ? WHERE discussion_id = ?`, targetId, sourceId);
  }

  // Extend target's date range.
  await db.query(
    `UPDATE discussions SET
       first_seen = LEAST(first_seen, ?),
       last_seen = GREATEST(last_seen, ?),
       updated_at = ? WHERE id = ?`,
    source.first_seen, source.last_seen, now, targetId,
  );
  await db.query('DELETE FROM discussions WHERE id = ?', sourceId);

  await recordFeedback(
    'discussions', 'discussion', targetId, 'merge',
    { source_id: sourceId, source_title: source.title },
    { merged_into: targetId },
    reason,
  );

  res.json({ target_id: targetId, source_id: sourceId });
});

// DELETE /api/discussions/:id/threads/:threadId — remove a thread from a discussion.
app.delete('/api/discussions/:id/threads/:threadId', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  const threadId = decodeURIComponent(req.params.threadId);
  if (isNaN(id) || !threadId) { res.status(400).json({ error: 'Invalid id or thread_id' }); return; }

  const reason = (req.query.reason as string | undefined) ?? null;
  await db.query(
    'DELETE FROM discussion_threads WHERE discussion_id = ? AND thread_id = ?', id, threadId,
  );
  await recordFeedback(
    'discussions', 'discussion_thread', id, 'thread_removed',
    { thread_id: threadId }, null, reason,
  );
  res.json({ removed: true, discussion_id: id, thread_id: threadId });
});

// POST /api/discussions/:id/threads — attach a thread to a discussion (reassignment).
// If the thread is already in another discussion, move it. `from_discussion_id`
// is optional metadata captured in feedback.
app.post('/api/discussions/:id/threads', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  const { thread_id, from_discussion_id, reason } = (req.body ?? {}) as {
    thread_id?: string; from_discussion_id?: number; reason?: string | null;
  };
  if (isNaN(id) || !thread_id) { res.status(400).json({ error: 'Invalid id or thread_id' }); return; }

  // Remove any existing links, then link to target. Simplest safe reassignment.
  await db.query('DELETE FROM discussion_threads WHERE thread_id = ?', thread_id);
  await db.query(
    'INSERT INTO discussion_threads (discussion_id, thread_id) VALUES (?, ?)', id, thread_id,
  );
  await recordFeedback(
    'discussions', 'discussion_thread', id, 'thread_added',
    from_discussion_id ? { from_discussion_id } : null,
    { thread_id },
    reason,
  );
  res.json({ added: true, discussion_id: id, thread_id });
});

// PATCH /api/events/:id — edit event_ledger row fields.
app.patch('/api/events/:id', async (req: Request, res: Response) => {
  const id = req.params.id;
  const body = (req.body ?? {}) as Record<string, string | null>;
  const editable = ['type', 'actor', 'target', 'event_date', 'detail'] as const;
  const updates: Array<[string, string | null]> = [];
  for (const k of editable) {
    if (k in body) updates.push([k, body[k] ?? null]);
  }
  if (updates.length === 0) {
    res.status(400).json({ error: `Provide at least one of: ${editable.join(', ')}` });
    return;
  }

  const existing = await db.queryOne<Record<string, unknown>>(
    'SELECT id, discussion_id, type, actor, target, event_date, detail FROM event_ledger WHERE id = ?', id,
  );
  if (!existing) { res.status(404).json({ error: 'Event not found' }); return; }

  const setClause = updates.map(([k]) => `${k} = ?`).join(', ');
  const values = updates.map(([, v]) => v);
  await db.query(`UPDATE event_ledger SET ${setClause} WHERE id = ?`, ...values, id);

  const oldSnapshot: Record<string, unknown> = {};
  const newSnapshot: Record<string, unknown> = {};
  for (const [k, v] of updates) {
    oldSnapshot[k] = (existing as any)[k];
    newSnapshot[k] = v;
  }
  await recordFeedback('events', 'event', id, 'edit', oldSnapshot, newSnapshot, body.reason);
  res.json({ id, updated: updates.map(([k]) => k) });
});

// DELETE /api/events/:id — remove an event from the ledger.
app.delete('/api/events/:id', async (req: Request, res: Response) => {
  const id = req.params.id;
  const reason = (req.query.reason as string | undefined) ?? null;
  const existing = await db.queryOne<Record<string, unknown>>(
    'SELECT id, discussion_id, type, actor, target, event_date, detail FROM event_ledger WHERE id = ?', id,
  );
  if (!existing) { res.status(404).json({ error: 'Event not found' }); return; }

  await db.query('UPDATE event_ledger SET human_deleted = 1 WHERE id = ?', id);
  await recordFeedback('events', 'event', id, 'delete', existing, null, reason);
  res.json({ deleted: true, id });
});

// ── /api/threads/:threadId/events ─────────────────────────────────────────

app.get('/api/threads/:threadId/events', async (req: Request, res: Response) => {
  const threadId = decodeURIComponent(req.params.threadId);
  const events = await db.query(
    `SELECT id, domain, type, actor, target, event_date, detail, confidence, thread_id, source_email_id, discussion_id
     FROM event_ledger WHERE thread_id = ? AND (human_deleted IS NULL OR human_deleted = 0)
     ORDER BY event_date ASC, created_at ASC`, threadId
  );
  res.json({ events });
});

app.post('/api/threads/:threadId/events', async (req: Request, res: Response) => {
  const threadId = decodeURIComponent(req.params.threadId);
  const { type, actor, target, event_date, detail, discussion_id } = (req.body ?? {}) as {
    type?: string; actor?: string | null; target?: string | null;
    event_date?: string | null; detail?: string | null; discussion_id?: number | null;
  };
  if (!type) { res.status(400).json({ error: 'type is required' }); return; }

  const id = `human-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const now = nowIso();
  await db.query(
    `INSERT INTO event_ledger (id, thread_id, discussion_id, domain, type, actor, target, event_date, detail, confidence, model_version, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    id, threadId, discussion_id ?? null, 'other', type,
    actor ?? null, target ?? null, event_date ?? null, detail ?? null,
    1.0, 'human', now
  );
  await recordFeedback('events', 'event', id, 'create', null, { type, actor, target, event_date, detail, thread_id: threadId }, null);
  const created = await db.queryOne('SELECT id, domain, type, actor, target, event_date, detail, confidence, thread_id, source_email_id, discussion_id FROM event_ledger WHERE id = ?', id);
  res.status(201).json(created);
});

// ── /api/discussions/:id/milestones ───────────────────────────────────────

app.post('/api/discussions/:id/milestones', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid discussion id' }); return; }

  const { name, achieved, achieved_date } = (req.body ?? {}) as {
    name?: string; achieved?: boolean; achieved_date?: string | null;
  };
  if (!name) { res.status(400).json({ error: 'name is required' }); return; }

  const now = nowIso();
  try {
    await db.query(
      `INSERT INTO milestones (discussion_id, name, achieved, achieved_date, source, last_evaluated_at)
       VALUES (?, ?, ?, ?, 'human', ?)`,
      id, name, achieved ? 1 : 0, achieved_date ?? null, now
    );
  } catch (e: any) {
    if (String(e.message).includes('UNIQUE')) {
      res.status(409).json({ error: 'A milestone with that name already exists for this discussion' });
      return;
    }
    throw e;
  }

  const created = await db.queryOne(
    `SELECT id, name, achieved, achieved_date, evidence_event_ids, confidence, source FROM milestones WHERE discussion_id = ? AND name = ?`,
    id, name
  );
  await recordFeedback('discussion_updates', 'milestone', id, 'create', null, { name, achieved, achieved_date }, null);
  res.status(201).json(created ? { ...created, achieved: !!(created as any).achieved, evidence_event_ids: [], source: 'human' } : null);
});

// ── /api/milestones/:id ───────────────────────────────────────────────────

app.patch('/api/milestones/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid milestone id' }); return; }

  const { name, achieved, achieved_date } = (req.body ?? {}) as {
    name?: string; achieved?: boolean; achieved_date?: string | null;
  };

  const existing = await db.queryOne<Record<string, unknown>>(
    'SELECT id, discussion_id, name, achieved, achieved_date FROM milestones WHERE id = ?', id
  );
  if (!existing) { res.status(404).json({ error: 'Milestone not found' }); return; }

  const updates: string[] = [];
  const values: unknown[] = [];
  if (name !== undefined) { updates.push('name = ?'); values.push(name); }
  if (achieved !== undefined) { updates.push('achieved = ?'); values.push(achieved ? 1 : 0); }
  if (achieved_date !== undefined) { updates.push('achieved_date = ?'); values.push(achieved_date); }

  if (updates.length > 0) {
    await db.query(`UPDATE milestones SET ${updates.join(', ')} WHERE id = ?`, ...values, id);
    await recordFeedback('discussion_updates', 'milestone', id, 'edit',
      { name: existing.name, achieved: existing.achieved, achieved_date: existing.achieved_date },
      { name, achieved, achieved_date }, null);
  }
  res.json({ id, updated: updates.map(u => u.split(' = ')[0]) });
});

app.delete('/api/milestones/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid milestone id' }); return; }

  const reason = (req.query.reason as string | undefined) ?? null;
  const existing = await db.queryOne<Record<string, unknown>>(
    'SELECT id, discussion_id, name, achieved FROM milestones WHERE id = ?', id
  );
  if (!existing) { res.status(404).json({ error: 'Milestone not found' }); return; }

  await db.query("UPDATE milestones SET source = 'human_deleted' WHERE id = ?", id);
  await recordFeedback('discussion_updates', 'milestone', id, 'delete', existing, null, reason);
  res.json({ deleted: true, id });
});

// ── /api/proposed-actions/:id ─────────────────────────────────────────────

app.patch('/api/proposed-actions/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid proposed action id' }); return; }

  const { action, priority, wait_until, assignee, status } = (req.body ?? {}) as {
    action?: string; priority?: string; wait_until?: string | null; assignee?: string | null; status?: string;
  };

  const existing = await db.queryOne<Record<string, unknown>>(
    'SELECT id, discussion_id, action, priority, wait_until, assignee, status FROM proposed_actions WHERE id = ?', id
  );
  if (!existing) { res.status(404).json({ error: 'Proposed action not found' }); return; }

  const updates: string[] = [];
  const values: unknown[] = [];
  if (action !== undefined) { updates.push('action = ?'); values.push(action); }
  if (priority !== undefined) { updates.push('priority = ?'); values.push(priority); }
  if (wait_until !== undefined) { updates.push('wait_until = ?'); values.push(wait_until); }
  if (assignee !== undefined) { updates.push('assignee = ?'); values.push(assignee); }
  if (status !== undefined) { updates.push('status = ?'); values.push(status); }

  if (updates.length > 0) {
    await db.query(`UPDATE proposed_actions SET ${updates.join(', ')} WHERE id = ?`, ...values, id);
    await recordFeedback('actions', 'proposed_action', id, 'edit',
      { action: existing.action, priority: existing.priority, wait_until: existing.wait_until, assignee: existing.assignee, status: existing.status },
      { action, priority, wait_until, assignee, status }, null);
  }
  res.json({ id, updated: updates.map(u => u.split(' = ')[0]) });
});

app.delete('/api/proposed-actions/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid proposed action id' }); return; }

  const reason = (req.query.reason as string | undefined) ?? null;
  const existing = await db.queryOne<Record<string, unknown>>(
    'SELECT id, discussion_id, action, priority FROM proposed_actions WHERE id = ?', id
  );
  if (!existing) { res.status(404).json({ error: 'Proposed action not found' }); return; }

  await db.query("UPDATE proposed_actions SET status = 'rejected', source = 'human' WHERE id = ?", id);
  await recordFeedback('actions', 'proposed_action', id, 'delete', existing, null, reason);
  res.json({ deleted: true, id });
});

app.post('/api/discussions/:id/proposed-actions', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid discussion id' }); return; }

  const { action, priority, wait_until, assignee } = (req.body ?? {}) as {
    action?: string; priority?: string; wait_until?: string | null; assignee?: string | null;
  };
  if (!action) { res.status(400).json({ error: 'action is required' }); return; }

  const now = nowIso();
  const result = await db.query(
    `INSERT INTO proposed_actions (discussion_id, action, priority, wait_until, assignee, source, status, created_at)
     VALUES (?, ?, ?, ?, ?, 'human', 'open', ?)`,
    id, action, priority ?? 'medium', wait_until ?? null, assignee ?? null, now
  );

  // Retrieve the created row using the last inserted id
  const newId = (result as any)?.lastID ?? (result as any)?.insertId;
  let created: any = null;
  if (newId) {
    created = await db.queryOne(
      `SELECT id, action, reasoning, priority, wait_until, assignee, created_at, status, source FROM proposed_actions WHERE id = ?`, newId
    );
  }
  await recordFeedback('actions', 'proposed_action', id, 'create', null, { action, priority, wait_until, assignee }, null);
  res.status(201).json(created ?? { id: newId, action, priority: priority ?? 'medium', wait_until: wait_until ?? null, assignee: assignee ?? null, reasoning: null, created_at: now, status: 'open', source: 'human' });
});

// ── /api/threads/:threadId/emails ─────────────────────────────────────────

app.get('/api/threads/:threadId/emails', async (req: Request, res: Response) => {
  const threadId = decodeURIComponent(req.params.threadId);
  const discussionId = req.query.discussion_id ? parseInt(req.query.discussion_id as string, 10) : null;

  let emails: any[];
  if (discussionId) {
    // When viewing from a discussion context, show only emails that are relevant:
    // either referenced by events in this discussion, or involving the company's domain.
    // The extraction stage also filters this way, so the display matches what the LLM saw.
    const disc = await db.queryOne(
      'SELECT d.company_id, c.domain FROM discussions d LEFT JOIN companies c ON c.id = d.company_id WHERE d.id = ?',
      discussionId
    );
    if (disc && disc.domain) {
      const like = `%@${disc.domain}%`;
      emails = await db.query(
        `SELECT id, message_id, subject, from_address, from_name, to_addresses, cc_addresses, date, body_text
         FROM emails WHERE thread_id = ? AND (
           from_address LIKE ? OR to_addresses LIKE ? OR cc_addresses LIKE ?
           OR message_id IN (SELECT source_email_id FROM event_ledger WHERE discussion_id = ? AND source_email_id IS NOT NULL)
         ) ORDER BY date ASC`,
        threadId, like, like, like, discussionId
      );
      // Fall back to all emails in thread if domain filter matched nothing
      // (e.g. event sourced from an internal thread that mentions the company)
      if (emails.length === 0) {
        emails = await db.query(
          `SELECT id, message_id, subject, from_address, from_name, to_addresses, cc_addresses, date, body_text
           FROM emails WHERE thread_id = ? ORDER BY date ASC`, threadId
        );
      }
    } else {
      emails = await db.query(
        `SELECT id, message_id, subject, from_address, from_name, to_addresses, cc_addresses, date, body_text
         FROM emails WHERE thread_id = ? ORDER BY date ASC`, threadId
      );
    }
  } else {
    emails = await db.query(
      `SELECT id, message_id, subject, from_address, from_name, to_addresses, cc_addresses, date, body_text
       FROM emails WHERE thread_id = ? ORDER BY date ASC`, threadId
    );
  }

  const enriched = emails.map((e: any) => ({
    ...e,
    to_addresses: parseJsonField<string[]>(e.to_addresses) ?? [],
    cc_addresses: parseJsonField<string[]>(e.cc_addresses) ?? [],
  }));

  res.json({ emails: enriched });
});

// ── /api/actions ──────────────────────────────────────────────────────────

app.get('/api/actions', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const status = (req.query.status as string) ?? '';
  const assignee = (req.query.assignee as string) ?? '';
  const company_id = (req.query.company_id as string) ?? '';
  const discussion_id = (req.query.discussion_id as string) ?? '';
  const sort = (req.query.sort as string) ?? 'status';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = { status: 'a.status', target_date: 'a.target_date', source_date: 'a.source_date', assignee: 'a.assignee_emails' };
  const sortCol = allowedSorts[sort] ?? 'a.status';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) { where.push('(a.description LIKE ? OR a.assignee_emails LIKE ?)'); params.push(`%${q}%`, `%${q}%`); }
  if (status) { where.push('a.status = ?'); params.push(status); }
  if (assignee) {
    const assigneeList = assignee.split(',').map(a => a.trim()).filter(Boolean);
    if (assigneeList.length === 1) {
      where.push('a.assignee_emails LIKE ?'); params.push(`%${assigneeList[0]}%`);
    } else if (assigneeList.length > 1) {
      const clauses = assigneeList.map(a => { params.push(`%${a}%`); return 'a.assignee_emails LIKE ?'; });
      where.push(`(${clauses.join(' OR ')})`);
    }
  }
  if (company_id) { where.push('d.company_id = ?'); params.push(company_id); }
  if (discussion_id) { where.push('a.discussion_id = ?'); params.push(discussion_id); }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM actions a JOIN discussions d ON a.discussion_id = d.id ${whereClause}`, ...params
  );
  const total = totalRow?.cnt ?? 0;

  const itemsRaw = await db.query(
    `SELECT a.id, a.discussion_id, a.description, a.assignee_emails, a.target_date,
            a.status, a.source_date, a.completed_date,
            d.title AS discussion_title, d.company_id, c.name AS company_name
     FROM actions a JOIN discussions d ON a.discussion_id = d.id
     LEFT JOIN companies c ON c.id = d.company_id
     ${whereClause} ORDER BY ${sortCol} ${order}, a.target_date ASC NULLS LAST LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );
  const items = itemsRaw.map((a: any) => ({ ...a, assignee_emails: parseJsonField<string[]>(a.assignee_emails) ?? [] }));

  const statuses = (await db.query<{ status: string }>(
    'SELECT DISTINCT status FROM actions WHERE status IS NOT NULL ORDER BY status'
  )).map(r => r.status);

  const allAssigneeRows = await db.query<{ assignee_emails: string }>(
    'SELECT assignee_emails FROM actions WHERE assignee_emails IS NOT NULL'
  );
  const assigneeSet = new Set<string>();
  for (const row of allAssigneeRows) {
    const emails = parseJsonField<string[]>(row.assignee_emails);
    if (emails) for (const e of emails) assigneeSet.add(e);
  }
  const assignees = [...assigneeSet].sort();

  res.json({ items, total, statuses, assignees });
});

// ── /api/calendar-events ──────────────────────────────────────────────────

app.get('/api/calendar-events', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const from = (req.query.from as string) ?? '';
  const to = (req.query.to as string) ?? '';
  const status = (req.query.status as string) ?? '';
  const sort = (req.query.sort as string) ?? 'start_time';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = { start_time: 'ce.start_time', end_time: 'ce.end_time', title: 'ce.title' };
  const sortCol = allowedSorts[sort] ?? 'ce.start_time';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) { where.push('(ce.title LIKE ? OR ce.description LIKE ? OR ce.location LIKE ?)'); params.push(`%${q}%`, `%${q}%`, `%${q}%`); }
  if (from) { where.push('ce.start_time >= ?'); params.push(from); }
  if (to) { where.push('ce.start_time <= ?'); params.push(to); }
  if (status) { where.push('ce.status = ?'); params.push(status); }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(`SELECT COUNT(*) AS cnt FROM calendar_events ce ${whereClause}`, ...params);
  const total = totalRow?.cnt ?? 0;

  const items = await db.query(
    `SELECT ce.id, ce.event_id, ce.title, ce.description, ce.location,
            ce.start_time, ce.end_time, ce.all_day, ce.status,
            ce.organizer_email, ce.attendees, ce.html_link,
            de.discussion_id, d.title AS discussion_title
     FROM calendar_events ce
     LEFT JOIN discussion_events de ON de.event_id = ce.id
     LEFT JOIN discussions d ON d.id = de.discussion_id
     ${whereClause} ORDER BY ${sortCol} ${order} LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const enriched = items.map((e: any) => ({
    ...e, all_day: !!e.all_day,
    attendees: parseJsonField<Array<{ email: string; name?: string; response_status?: string }>>(e.attendees) ?? [],
  }));

  res.json({ items: enriched, total });
});

// ── /api/tasks ────────────────────────────────────────────────────────────

app.get('/api/tasks', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const status = (req.query.status as string) ?? '';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 50));
  const offset = (page - 1) * limit;

  const params: unknown[] = [];
  const where: string[] = [];

  if (status === 'open') {
    where.push("(t.status IS NULL OR t.status != 'COMPLETED')");
  } else if (status === 'completed') {
    where.push("t.status = 'COMPLETED'");
  } else if (status && status !== 'all') {
    where.push('t.status = ?');
    params.push(status);
  }

  if (q) {
    where.push('(t.subject LIKE ? OR t.body LIKE ?)');
    params.push(`%${q}%`, `%${q}%`);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM hubspot_tasks t ${whereClause}`, ...params
  );
  const total = totalRow?.cnt ?? 0;

  const rows = await db.query<DbRow>(
    `SELECT t.*,
            (SELECT COUNT(*) FROM hubspot_task_threads thr WHERE thr.task_id = t.id) AS thread_count
     FROM hubspot_tasks t
     ${whereClause}
     ORDER BY CASE WHEN t.due_date IS NULL THEN 1 ELSE 0 END, t.due_date ASC
     LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const items = await Promise.all(rows.map(async (row) => {
    const contactIds: string[] = parseJsonField<string[]>(row.associated_contact_ids) ?? [];
    const contacts = (await Promise.all(
      contactIds.map(id =>
        db.queryOne<DbRow>(`SELECT id, email, firstname, lastname FROM hubspot_contacts WHERE id = ?`, id)
      )
    ))
      .filter(Boolean)
      .map(c => ({
        id: c!.id as string,
        email: (c!.email as string) ?? null,
        name: [c!.firstname, c!.lastname].filter(Boolean).join(' ') || null,
      }));

    const companyIds: string[] = parseJsonField<string[]>(row.associated_company_ids) ?? [];
    const companies = (await Promise.all(
      companyIds.map(id =>
        db.queryOne<DbRow>(
          `SELECT hc.id, hc.name, hc.domain, hc.hs_url, c.id AS local_id
           FROM hubspot_companies hc
           LEFT JOIN companies c ON c.domain = hc.domain AND hc.domain IS NOT NULL AND hc.domain != ''
           WHERE hc.id = ?`,
          id
        )
      )
    ))
      .filter(Boolean)
      .map(c => ({
        id: c!.id as string,
        name: (c!.name as string) ?? null,
        domain: (c!.domain as string) ?? null,
        hs_url: (c!.hs_url as string) ?? null,
        local_id: c!.local_id != null ? Number(c!.local_id) : null,
      }));

    return {
      ...row,
      associated_contact_ids: contactIds,
      associated_company_ids: companyIds,
      contacts,
      companies,
      thread_count: Number(row.thread_count ?? 0),
    };
  }));

  res.json({ items, total });
});

app.get('/api/tasks/:id', async (req: Request, res: Response) => {
  const task = await db.queryOne<DbRow>(
    `SELECT t.*,
            (SELECT COUNT(*) FROM hubspot_task_threads thr WHERE thr.task_id = t.id) AS thread_count
     FROM hubspot_tasks t WHERE t.id = ?`,
    req.params.id
  );
  if (!task) return res.status(404).json({ error: 'Task not found' });

  const contactIds: string[] = parseJsonField<string[]>(task.associated_contact_ids) ?? [];
  const contacts = (await Promise.all(
    contactIds.map(id =>
      db.queryOne<DbRow>(`SELECT id, email, firstname, lastname FROM hubspot_contacts WHERE id = ?`, id)
    )
  ))
    .filter(Boolean)
    .map(c => ({
      id: c!.id as string,
      email: (c!.email as string) ?? null,
      name: [c!.firstname, c!.lastname].filter(Boolean).join(' ') || null,
    }));

  const companyIds: string[] = parseJsonField<string[]>(task.associated_company_ids) ?? [];
  const companies = (await Promise.all(
    companyIds.map(id =>
      db.queryOne<DbRow>(
        `SELECT hc.id, hc.name, hc.domain, hc.hs_url, c.id AS local_id
         FROM hubspot_companies hc
         LEFT JOIN companies c ON c.domain = hc.domain AND hc.domain IS NOT NULL AND hc.domain != ''
         WHERE hc.id = ?`,
        id
      )
    )
  ))
    .filter(Boolean)
    .map(c => ({
      id: c!.id as string,
      name: (c!.name as string) ?? null,
      domain: (c!.domain as string) ?? null,
      hs_url: (c!.hs_url as string) ?? null,
      local_id: c!.local_id != null ? Number(c!.local_id) : null,
    }));

  const threads = await db.query<DbRow>(
    `SELECT thr.thread_id, thr.contact_email,
            MIN(e.subject) AS subject,
            COUNT(e.id) AS email_count,
            MIN(e.date) AS first_date,
            MAX(e.date) AS last_date
     FROM hubspot_task_threads thr
     JOIN emails e ON e.thread_id = thr.thread_id
     WHERE thr.task_id = ?
     GROUP BY thr.thread_id, thr.contact_email
     ORDER BY last_date DESC
     LIMIT 50`,
    req.params.id
  );

  res.json({
    ...task,
    associated_contact_ids: contactIds,
    associated_company_ids: companyIds,
    contacts,
    companies,
    thread_count: Number(task.thread_count ?? 0),
    threads: threads.map(t => ({ ...t, email_count: Number(t.email_count ?? 0) })),
  });
});

// ── /api/jobs ─────────────────────────────────────────────────────────────

app.get('/api/jobs/stages', (_req: Request, res: Response) => {
  res.json({ stages: PIPELINE_STAGES });
});

app.get('/api/jobs/active', (_req: Request, res: Response) => {
  const active = getActiveJob();
  res.json({ active });
});

app.get('/api/jobs', async (req: Request, res: Response) => {
  const status = (req.query.status as string) ?? '';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));

  const result = await listJobs({ status: status || undefined, page, limit });
  res.json(result);
});

app.post('/api/jobs', async (req: Request, res: Response) => {
  const body = req.body;

  const jobType = body.job_type;
  if (!jobType || !['sync', 'analyse'].includes(jobType)) {
    res.status(400).json({ error: 'Invalid job_type. Must be "sync" or "analyse".' });
    return;
  }

  if (jobType === 'analyse' && body.stages) {
    const validNames = new Set(PIPELINE_STAGES.map(s => s.name));
    const invalid = body.stages.filter((s: string) => !validNames.has(s));
    if (invalid.length > 0) {
      res.status(400).json({ error: `Invalid stages: ${invalid.join(', ')}` });
      return;
    }
  }

  const config: JobConfig = {
    job_type: jobType,
    stages: body.stages ?? null,
    company: body.company ?? null,
    label: body.label ?? null,
    force: !!body.force,
    clean: !!body.clean,
    per_company: !!body.per_company,
    concurrency: body.concurrency ?? 1,
    new_emails: !!body.new_emails,
    stale_model: !!body.stale_model,
    stale_prompt: !!body.stale_prompt,
  };

  try {
    const job = await createJob(config);
    res.status(201).json(job);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error('[jobs] createJob failed:', msg);
    res.status(500).json({ error: msg });
  }
});

app.get('/api/jobs/:id', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid job id' }); return; }

  const job = await getJob(id);
  if (!job) { res.status(404).json({ error: 'Job not found' }); return; }

  res.json(job);
});

app.post('/api/jobs/:id/cancel', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid job id' }); return; }

  const ok = await cancelJob(id);
  if (!ok) {
    res.status(400).json({ error: 'Job cannot be cancelled (not running or queued)' });
    return;
  }

  const job = await getJob(id);
  res.json(job);
});

app.get('/api/jobs/:id/logs', (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid job id' }); return; }

  subscribeToLogs(id, res);
});

// ── /api/search ───────────────────────────────────────────────────────────

app.get('/api/search', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  if (!q.trim()) { res.json({ results: [], discussion_results: [], total: 0, discussion_total: 0, query_time_ms: 0, search_mode: 'none' }); return; }

  const limitParam = Math.min(50, Math.max(1, parseInt(req.query.limit as string) || 20));
  const pageParam = Math.max(1, parseInt(req.query.page as string) || 1);
  const offset = (pageParam - 1) * limitParam;
  const company = (req.query.company as string) ?? '';
  const label = (req.query.label as string) ?? '';
  const dateFrom = (req.query.from as string) ?? '';
  const dateTo = (req.query.to as string) ?? '';
  const category = (req.query.category as string) ?? '';      // discussion category filter
  const discussionOnly = req.query.discussions === '1';        // only threads in a discussion
  const model = (req.query.model as string) === 'quality' ? 'quality' as const : 'fast' as const;

  const start = Date.now();

  if (db.backend !== 'postgres') {
    // SQLite fallback: simple LIKE search
    const likeQ = `%${q}%`;
    const totalRow = await db.queryOne<{ cnt: number }>(
      `SELECT COUNT(*) AS cnt FROM thread_search_docs WHERE doc_text LIKE ?`,
      likeQ
    );
    const results = await db.query(
      `SELECT tsd.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.participants,
              tsd.company_domain, c.name AS company_name, 1.0 AS score
       FROM thread_search_docs tsd
       JOIN threads t ON t.thread_id = tsd.thread_id
       LEFT JOIN companies c ON c.domain = tsd.company_domain
       WHERE tsd.doc_text LIKE ? ORDER BY t.last_date DESC LIMIT ? OFFSET ?`,
      likeQ, limitParam, offset
    );
    res.json({
      results: results.map((r: any) => ({
        thread_id: r.thread_id, subject: r.subject, company_domain: r.company_domain,
        company_name: r.company_name, participants: parseJsonField<string[]>(r.participants) ?? [],
        first_date: r.first_date, last_date: r.last_date, email_count: r.email_count,
        snippet: null, score: r.score, score_type: 'like',
      })),
      discussion_results: [],
      total: totalRow?.cnt ?? results.length,
      discussion_total: 0,
      query_time_ms: Date.now() - start, search_mode: 'like',
    });
    return;
  }

  // ── Build filter clauses (shared between BM25 and hybrid paths) ──
  // These apply in the "ranked" CTE which has access to tsd.* and can join threads
  const rankFilters: string[] = [];
  const rankFilterParams: unknown[] = [];

  if (company) {
    rankFilters.push('AND tsd.company_domain = ?');
    rankFilterParams.push(company);
  }
  if (label) {
    rankFilters.push('AND tsd.company_domain IN (SELECT c.domain FROM companies c JOIN company_labels cl ON cl.company_id = c.id WHERE cl.label = ?)');
    rankFilterParams.push(label);
  }
  if (dateFrom) {
    rankFilters.push('AND tsd.thread_id IN (SELECT thread_id FROM threads WHERE last_date >= ?)');
    rankFilterParams.push(dateFrom);
  }
  if (dateTo) {
    rankFilters.push('AND tsd.thread_id IN (SELECT thread_id FROM threads WHERE first_date <= ?)');
    rankFilterParams.push(dateTo);
  }
  if (discussionOnly || category) {
    if (category) {
      rankFilters.push('AND tsd.thread_id IN (SELECT dt.thread_id FROM discussion_threads dt JOIN discussions d ON d.id = dt.discussion_id WHERE d.category = ?)');
      rankFilterParams.push(category);
    } else {
      rankFilters.push('AND tsd.thread_id IN (SELECT dt.thread_id FROM discussion_threads dt)');
    }
  }

  const filterSQL = rankFilters.join(' ');

  // ── Discussion BM25 search (runs alongside thread search) ──
  // Filters applied: company (via discussion_search_docs.company_domain), category.
  // Date and label filters are skipped — discussions aren't tied to individual emails.
  let discussionResults: any[] = [];
  let discussionTotal = 0;
  if (db.backend === 'postgres') {
    const discFilters: string[] = [];
    const discFilterParams: unknown[] = [];
    if (company) { discFilters.push('AND dsd.company_domain = ?'); discFilterParams.push(company); }
    if (category) { discFilters.push('AND dsd.category = ?'); discFilterParams.push(category); }
    const discFilterSQL = discFilters.join(' ');

    const DISC_LIMIT = 10;
    const discParams: unknown[] = [q, q, q, q, ...discFilterParams, DISC_LIMIT, q];

    try {
      discussionResults = await db.query(
        `WITH matched AS (
           SELECT discussion_id FROM discussion_search_docs WHERE doc_tsv @@ websearch_to_tsquery('english', ?)
           UNION
           SELECT discussion_id FROM discussion_search_docs WHERE doc_tsv_simple @@ phraseto_tsquery('simple', ?)
         ),
         ranked AS (
           SELECT dsd.discussion_id,
                  (ts_rank_cd(dsd.doc_tsv, websearch_to_tsquery('english', ?))
                    + 10.0 * ts_rank_cd(dsd.doc_tsv_simple, phraseto_tsquery('simple', ?)))
                  AS score,
                  dsd.company_domain, dsd.category, dsd.current_state
           FROM matched m
           JOIN discussion_search_docs dsd ON dsd.discussion_id = m.discussion_id
           WHERE 1=1 ${discFilterSQL}
           ORDER BY score DESC LIMIT ?
         )
         SELECT r.discussion_id, r.score, r.category, r.current_state, r.company_domain,
                d.title, d.first_seen, d.last_seen,
                c.name AS company_name,
                ts_headline('simple', dsd2.doc_text, phraseto_tsquery('simple', ?),
                  'StartSel=**, StopSel=**, MaxWords=60, MinWords=20, MaxFragments=2'
                ) AS snippet
         FROM ranked r
         JOIN discussions d ON d.id = r.discussion_id
         JOIN discussion_search_docs dsd2 ON dsd2.discussion_id = r.discussion_id
         LEFT JOIN companies c ON c.domain = r.company_domain
         ORDER BY r.score DESC`,
        ...discParams
      );

      const discCountParams: unknown[] = [q, q, ...discFilterParams];
      const discTotalRow = await db.queryOne<{ cnt: number }>(
        `WITH matched AS (
           SELECT discussion_id FROM discussion_search_docs WHERE doc_tsv @@ websearch_to_tsquery('english', ?)
           UNION
           SELECT discussion_id FROM discussion_search_docs WHERE doc_tsv_simple @@ phraseto_tsquery('simple', ?)
         )
         SELECT COUNT(*) AS cnt FROM matched m
         JOIN discussion_search_docs dsd ON dsd.discussion_id = m.discussion_id
         WHERE 1=1 ${discFilterSQL}`,
        ...discCountParams
      );
      discussionTotal = Number(discTotalRow?.cnt ?? discussionResults.length);
    } catch (err) {
      // discussion_search_docs may not exist yet — swallow and return empty
      console.log('[search] Discussion search failed:', (err as Error).message);
    }
  }

  const discussionResultsMapped = discussionResults.map((r: any) => ({
    discussion_id: r.discussion_id,
    title: r.title,
    category: r.category,
    current_state: r.current_state,
    company_domain: r.company_domain,
    company_name: r.company_name,
    first_seen: r.first_seen,
    last_seen: r.last_seen,
    snippet: r.snippet,
    score: Number(r.score),
    score_type: 'bm25',
  }));

  // ── Try hybrid search (BM25 + vector) first ──
  let threadIds: string[] | null = null;
  let searchMode = 'bm25';

  try {
    const hybrid = await hybridSearch(db, {
      query: q, limit: limitParam, offset, model, company: company || undefined, label: label || undefined,
    });
    if (hybrid.vector_available && hybrid.results.length > 0) {
      threadIds = hybrid.results.map(r => r.thread_id);
      searchMode = 'hybrid';
    }
  } catch (err) {
    console.log('[search] Hybrid search failed, falling back to BM25:', (err as Error).message);
  }

  if (threadIds) {
    // Fetch full thread data for the RRF-ranked results, preserving order
    const placeholders = threadIds.map(() => '?').join(',');
    const rows = await db.query(
      `SELECT tsd.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.participants,
              tsd.company_domain, c.name AS company_name,
              ts_headline('simple', tsd.doc_text, phraseto_tsquery('simple', ?),
                'StartSel=**, StopSel=**, MaxWords=60, MinWords=20, MaxFragments=2'
              ) AS snippet
       FROM thread_search_docs tsd
       JOIN threads t ON t.thread_id = tsd.thread_id
       LEFT JOIN companies c ON c.domain = tsd.company_domain
       WHERE tsd.thread_id IN (${placeholders}) ${filterSQL}`,
      q, ...threadIds, ...rankFilterParams
    );

    const rowMap = new Map(rows.map((r: any) => [r.thread_id, r]));
    const enriched = threadIds
      .map((tid, i) => {
        const r: any = rowMap.get(tid);
        if (!r) return null;
        return {
          thread_id: r.thread_id, subject: r.subject, company_domain: r.company_domain,
          company_name: r.company_name, participants: parseJsonField<string[]>(r.participants) ?? [],
          first_date: r.first_date, last_date: r.last_date, email_count: r.email_count,
          snippet: r.snippet, score: limitParam - i, score_type: 'hybrid',
        };
      })
      .filter(Boolean);

    // Count total matches with filters applied
    const hybridCountParams: unknown[] = [q, q, ...rankFilterParams];
    const hybridTotal = await db.queryOne<{ cnt: number }>(
      `WITH matched AS (
         SELECT thread_id FROM thread_search_docs WHERE doc_tsv @@ websearch_to_tsquery('english', ?)
         UNION
         SELECT thread_id FROM thread_search_docs WHERE doc_tsv_simple @@ phraseto_tsquery('simple', ?)
       )
       SELECT COUNT(*) AS cnt FROM matched m
       JOIN thread_search_docs tsd ON tsd.thread_id = m.thread_id
       WHERE 1=1 ${filterSQL}`,
      ...hybridCountParams
    );

    res.json({
      results: enriched,
      discussion_results: discussionResultsMapped,
      total: hybridTotal?.cnt ?? enriched.length,
      discussion_total: discussionTotal,
      query_time_ms: Date.now() - start, search_mode: searchMode,
    });
  } else {
    // ── BM25-only with recency boost ──
    // Recency: ln(days_ago + 1) decays slowly. We subtract a fraction of it from the score.
    // A thread from today gets 0 penalty; 1 year ago ~6 penalty; 5 years ago ~7.5 penalty.
    // With text scores in the 1-15 range, scale factor of 0.3 gives meaningful but not overwhelming boost.
    const bm25Params: unknown[] = [q, q, q, q, ...rankFilterParams, limitParam, offset, q];

    const results = await db.query(
      `WITH matched AS (
         SELECT thread_id FROM thread_search_docs WHERE doc_tsv @@ websearch_to_tsquery('english', ?)
         UNION
         SELECT thread_id FROM thread_search_docs WHERE doc_tsv_simple @@ phraseto_tsquery('simple', ?)
       ),
       ranked AS (
         SELECT tsd.thread_id,
                (ts_rank_cd(tsd.doc_tsv, websearch_to_tsquery('english', ?))
                  + 10.0 * ts_rank_cd(tsd.doc_tsv_simple, phraseto_tsquery('simple', ?)))
                * (1.0 + 0.3 / (1.0 + EXTRACT(EPOCH FROM (NOW() - COALESCE(t.last_date::timestamptz, NOW()))) / 86400.0 / 365.0))
                * (1.0 + 0.4 * LN(1.0 + COALESCE(tsd.outreach_score, 0)))
                AS score,
                tsd.company_domain
         FROM matched m
         JOIN thread_search_docs tsd ON tsd.thread_id = m.thread_id
         JOIN threads t ON t.thread_id = m.thread_id
         WHERE 1=1 ${filterSQL}
         ORDER BY score DESC LIMIT ? OFFSET ?
       )
       SELECT r.thread_id, r.score, r.company_domain,
              t.subject, t.email_count, t.first_date, t.last_date, t.participants,
              c.name AS company_name,
              ts_headline('simple', tsd2.doc_text, phraseto_tsquery('simple', ?),
                'StartSel=**, StopSel=**, MaxWords=60, MinWords=20, MaxFragments=2'
              ) AS snippet
       FROM ranked r
       JOIN threads t ON t.thread_id = r.thread_id
       JOIN thread_search_docs tsd2 ON tsd2.thread_id = r.thread_id
       LEFT JOIN companies c ON c.domain = r.company_domain
       ORDER BY r.score DESC`,
      ...bm25Params
    );

    // Count (with filters applied)
    const countParams: unknown[] = [q, q, ...rankFilterParams];
    const totalRow = await db.queryOne<{ cnt: number }>(
      `WITH matched AS (
         SELECT thread_id FROM thread_search_docs WHERE doc_tsv @@ websearch_to_tsquery('english', ?)
         UNION
         SELECT thread_id FROM thread_search_docs WHERE doc_tsv_simple @@ phraseto_tsquery('simple', ?)
       )
       SELECT COUNT(*) AS cnt FROM matched m
       JOIN thread_search_docs tsd ON tsd.thread_id = m.thread_id
       WHERE 1=1 ${filterSQL}`,
      ...countParams
    );

    res.json({
      results: results.map((r: any) => ({
        thread_id: r.thread_id, subject: r.subject, company_domain: r.company_domain,
        company_name: r.company_name, participants: parseJsonField<string[]>(r.participants) ?? [],
        first_date: r.first_date, last_date: r.last_date, email_count: r.email_count,
        snippet: r.snippet, score: r.score, score_type: 'bm25',
      })),
      discussion_results: discussionResultsMapped,
      total: totalRow?.cnt ?? results.length,
      discussion_total: discussionTotal,
      query_time_ms: Date.now() - start,
      search_mode: 'bm25',
    });
  }
});

// ── Production SPA fallback ────────────────────────────────────────────────

if (process.env.NODE_ENV === 'production') {
  const distPath = path.resolve(__dirname, '../dist');
  app.get('*', (_req: Request, res: Response) => {
    res.sendFile(path.join(distPath, 'index.html'));
  });
}

// ── Start server ───────────────────────────────────────────────────────────

registerReviewRoutes(app, db);
registerConfigRoutes(app);

// ── Prefect proxy routes ────────────────────────────────────────────────────

app.get('/api/prefect/status', (_req: Request, res: Response) => {
  res.json({
    enabled: prefectEnabled(),
    url: process.env.PREFECT_API_URL ?? null,
  });
});

app.get('/api/prefect/deployments', async (_req: Request, res: Response) => {
  try {
    const deps = await listDeployments();
    res.json(deps);
  } catch (err) {
    res.status(502).json({ error: String(err) });
  }
});

app.post('/api/prefect/deployments/:id/run', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { parameters = {} } = req.body as { parameters?: Record<string, unknown> };
    const run = await triggerDeployment(id, parameters);
    res.status(201).json(run);
  } catch (err) {
    res.status(502).json({ error: String(err) });
  }
});

app.get('/api/prefect/runs', async (req: Request, res: Response) => {
  try {
    const { deployment_id, limit, offset } = req.query as Record<string, string>;
    const runs = await listFlowRuns({
      deploymentId: deployment_id,
      limit: limit ? parseInt(limit, 10) : 25,
      offset: offset ? parseInt(offset, 10) : 0,
    });
    res.json(runs);
  } catch (err) {
    res.status(502).json({ error: String(err) });
  }
});

app.get('/api/prefect/runs/:id', async (req: Request, res: Response) => {
  try {
    const run = await getFlowRun(req.params.id);
    res.json(run);
  } catch (err) {
    res.status(502).json({ error: String(err) });
  }
});

app.get('/api/prefect/runs/:id/logs', async (req: Request, res: Response) => {
  try {
    const { offset, limit } = req.query as Record<string, string>;
    const logs = await getFlowRunLogs(
      req.params.id,
      offset ? parseInt(offset, 10) : 0,
      limit ? parseInt(limit, 10) : 200,
    );
    res.json(logs);
  } catch (err) {
    res.status(502).json({ error: String(err) });
  }
});

app.delete('/api/prefect/runs/:id', async (req: Request, res: Response) => {
  try {
    await cancelFlowRun(req.params.id);
    res.json({ ok: true });
  } catch (err) {
    res.status(502).json({ error: String(err) });
  }
});

initJobs(db).then(() => {
  app.listen(PORT, () => {
    console.log(`API server running on http://localhost:${PORT}`);
    console.log(`Database backend: ${db.backend}`);
  });
}).catch((err) => {
  console.error('Cannot connect to PostgreSQL database. Check DB_URL and ensure the database is accessible.');
  console.error(err.message);
  process.exit(1);
});
