import 'dotenv/config';
import express, { Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import yaml from 'js-yaml';
import { createDb, type Database, type DbRow } from './db.js';

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

  res.json({ labels, categories, states, stats, userEmails, categoryConfig });
});

// ── /api/companies ─────────────────────────────────────────────────────────

app.get('/api/companies', async (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const label = (req.query.label as string) ?? '';
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

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(`SELECT COUNT(*) AS cnt FROM companies c ${whereClause}`, ...params);
  const total = totalRow?.cnt ?? 0;

  const items = await db.query(
    `SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen,
            c.homepage_fetched_at, c.description,
            GROUP_CONCAT(cl.label, '||') AS labels_concat
     FROM companies c
     LEFT JOIN company_labels cl ON cl.company_id = c.id
     ${whereClause}
     GROUP BY c.id
     ORDER BY ${sortCol} ${order}
     LIMIT ? OFFSET ?`,
    ...params, limit, offset
  );

  const enriched = items.map(({ labels_concat, ...rest }: any) => ({
    ...rest,
    labels: labels_concat ? [...new Set((labels_concat as string).split('||'))] : [],
  }));

  const allLabels = (await db.query<{ label: string }>('SELECT DISTINCT label FROM company_labels ORDER BY label')).map(r => r.label);

  res.json({ items: enriched, total, labels: allLabels });
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
  const threadsRaw = await db.query(
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

  res.json({ ...company, labels, contacts, discussions, threads: threadsRaw });
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

  // Processing runs for this company
  const runs = await db.query(
    `SELECT id, mode, model, started_at, completed_at,
            events_created, discussions_created, discussions_updated, actions_proposed,
            input_tokens, output_tokens, llm_calls
     FROM processing_runs WHERE company_domain = ?
     ORDER BY started_at DESC LIMIT 20`,
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
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id) AS action_count,
            (SELECT COUNT(*) FROM milestones m WHERE m.discussion_id = d.id AND m.achieved = 1) AS milestones_achieved,
            (SELECT COUNT(*) FROM milestones m WHERE m.discussion_id = d.id) AS milestones_total,
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

  res.json({ ...contact, memory, threads: enrichedThreads });
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
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id) AS proposed_action_count,
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id AND pa.priority = 'high') AS high_priority_count,
            (SELECT COUNT(*) FROM proposed_actions pa WHERE pa.discussion_id = d.id AND pa.priority = 'medium') AS med_priority_count
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
    `SELECT name, achieved, achieved_date, evidence_event_ids, confidence
     FROM milestones WHERE discussion_id = ? ORDER BY achieved DESC, achieved_date ASC NULLS LAST`, id
  );
  const milestones = milestonesRaw.map((m: any) => ({
    ...m, achieved: !!m.achieved,
    evidence_event_ids: parseJsonField<string[]>(m.evidence_event_ids) ?? [],
  }));

  const proposedActions = await db.query(
    `SELECT id, action, reasoning, priority, wait_until, assignee, created_at
     FROM proposed_actions WHERE discussion_id = ?
     ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, id ASC`, id
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
    proposed_actions: proposedActions, children,
  });
});

// ── /api/discussions/:id/proposed-actions ──────────────────────────────────

app.get('/api/discussions/:id/proposed-actions', async (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) { res.status(400).json({ error: 'Invalid discussion id' }); return; }

  const actions = await db.query(
    `SELECT id, action, reasoning, priority, wait_until, assignee, created_at
     FROM proposed_actions WHERE discussion_id = ?
     ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END, id ASC`, id
  ).catch(() => []);
  res.json(actions);
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

// ── Production SPA fallback ────────────────────────────────────────────────

if (process.env.NODE_ENV === 'production') {
  const distPath = path.resolve(__dirname, '../dist');
  app.get('*', (_req: Request, res: Response) => {
    res.sendFile(path.join(distPath, 'index.html'));
  });
}

app.listen(PORT, () => {
  console.log(`API server running on http://localhost:${PORT}`);
  console.log(`Database backend: ${db.backend}`);
});
