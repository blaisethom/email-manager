import express, { Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { DatabaseSync } from 'node:sqlite';
import yaml from 'js-yaml';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_PATH = process.env.DB_PATH ?? path.resolve(__dirname, '../../data/email_manager.db');

const db = new DatabaseSync(DB_PATH, { open: true });
db.exec('PRAGMA journal_mode=WAL');

// Ensure indexes exist for expensive queries
db.exec('CREATE INDEX IF NOT EXISTS idx_emails_folder ON emails(folder, from_address)');

// Ensure calendar tables exist (may not yet if Python migration hasn't run)
db.exec(`CREATE TABLE IF NOT EXISTS calendar_events (
    id              INTEGER PRIMARY KEY,
    event_id        TEXT UNIQUE NOT NULL,
    calendar_id     TEXT NOT NULL DEFAULT 'primary',
    account_name    TEXT,
    title           TEXT,
    description     TEXT,
    location        TEXT,
    start_time      TEXT NOT NULL,
    end_time        TEXT NOT NULL,
    all_day         INTEGER DEFAULT 0,
    status          TEXT,
    organizer_email TEXT,
    attendees       TEXT,
    html_link       TEXT,
    recurring_event_id TEXT,
    created_at      TEXT,
    updated_at      TEXT,
    fetched_at      TEXT NOT NULL
)`);
db.exec(`CREATE TABLE IF NOT EXISTS discussion_events (
    discussion_id   INTEGER REFERENCES discussions(id),
    event_id        INTEGER REFERENCES calendar_events(id),
    match_score     REAL,
    match_reason    TEXT,
    PRIMARY KEY (discussion_id, event_id)
)`);
db.exec('CREATE INDEX IF NOT EXISTS idx_calendar_events_start ON calendar_events(start_time)');
db.exec('CREATE INDEX IF NOT EXISTS idx_discussion_events_event ON discussion_events(event_id)');

// Ensure event ledger and milestones tables exist (may not yet if Python migration hasn't run)
db.exec(`CREATE TABLE IF NOT EXISTS event_ledger (
    id              TEXT PRIMARY KEY,
    thread_id       TEXT,
    source_email_id TEXT,
    source_calendar_event_id TEXT,
    discussion_id   INTEGER REFERENCES discussions(id),
    domain          TEXT NOT NULL,
    type            TEXT NOT NULL,
    actor           TEXT,
    target          TEXT,
    event_date      TEXT,
    detail          TEXT,
    confidence      REAL,
    model_version   TEXT,
    prompt_version  TEXT,
    created_at      TEXT NOT NULL
)`);
db.exec(`CREATE TABLE IF NOT EXISTS milestones (
    id              INTEGER PRIMARY KEY,
    discussion_id   INTEGER REFERENCES discussions(id),
    name            TEXT NOT NULL,
    achieved        INTEGER DEFAULT 0,
    achieved_date   TEXT,
    evidence_event_ids TEXT,
    confidence      REAL,
    last_evaluated_at TEXT,
    UNIQUE(discussion_id, name)
)`);

console.log(`Database opened: ${DB_PATH}`);

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

// Serve static files in production
if (process.env.NODE_ENV === 'production') {
  const distPath = path.resolve(__dirname, '../dist');
  app.use(express.static(distPath));
}

// ── /api/meta ──────────────────────────────────────────────────────────────

app.get('/api/meta', (_req: Request, res: Response) => {
  const labels = (
    db.prepare('SELECT DISTINCT label FROM company_labels ORDER BY label').all() as { label: string }[]
  ).map((r) => r.label);

  const categories = (
    db
      .prepare('SELECT DISTINCT category FROM discussions WHERE category IS NOT NULL ORDER BY category')
      .all() as { category: string }[]
  ).map((r) => r.category);

  const states = (
    db
      .prepare(
        'SELECT DISTINCT current_state FROM discussions WHERE current_state IS NOT NULL ORDER BY current_state'
      )
      .all() as { current_state: string }[]
  ).map((r) => r.current_state);

  const stats = db
    .prepare(
      `SELECT
        (SELECT COUNT(*) FROM companies) AS companies,
        (SELECT COUNT(*) FROM contacts) AS contacts,
        (SELECT COUNT(*) FROM discussions) AS discussions,
        (SELECT COUNT(*) FROM actions) AS actions,
        (SELECT COALESCE(SUM(email_count), 0) FROM companies) AS emails,
        (SELECT COUNT(*) FROM calendar_events) AS calendar_events`
    )
    .get() as { companies: number; contacts: number; discussions: number; actions: number; emails: number; calendar_events: number };

  // Get user's own email addresses by looking at sent-folder emails
  const userEmailRows = db
    .prepare(
      `SELECT from_address, COUNT(*) AS cnt FROM emails
       WHERE folder IN ('SENT', 'Sent', 'Sent Items', 'Sent Mail', '[Gmail]/Sent Mail')
         AND from_address IS NOT NULL
       GROUP BY from_address
       ORDER BY cnt DESC`
    )
    .all() as { from_address: string; cnt: number }[];
  const userEmails = userEmailRows.map((r) => r.from_address);

  res.json({ labels, categories, states, stats, userEmails, categoryConfig });
});

// ── /api/companies ─────────────────────────────────────────────────────────

app.get('/api/companies', (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const label = (req.query.label as string) ?? '';
  const sort = (req.query.sort as string) ?? 'email_count';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = {
    email_count: 'c.email_count',
    name: 'c.name',
    last_seen: 'c.last_seen',
  };
  const sortCol = allowedSorts[sort] ?? 'c.email_count';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) {
    where.push('(c.name LIKE ? OR c.domain LIKE ?)');
    params.push(`%${q}%`, `%${q}%`);
  }

  if (label) {
    where.push('c.id IN (SELECT company_id FROM company_labels WHERE label = ?)');
    params.push(label);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = db.prepare(`SELECT COUNT(*) AS cnt FROM companies c ${whereClause}`).get(
    ...params
  ) as { cnt: number };
  const total = totalRow.cnt;

  const items = db
    .prepare(
      `SELECT c.id, c.name, c.domain, c.email_count, c.first_seen, c.last_seen,
              c.homepage_fetched_at, c.description,
              GROUP_CONCAT(cl.label, '||') AS labels_concat
       FROM companies c
       LEFT JOIN company_labels cl ON cl.company_id = c.id
       ${whereClause}
       GROUP BY c.id
       ORDER BY ${sortCol} ${order}
       LIMIT ? OFFSET ?`
    )
    .all(...params, limit, offset) as Array<{
    id: number;
    name: string;
    domain: string | null;
    email_count: number;
    first_seen: string | null;
    last_seen: string | null;
    homepage_fetched_at: string | null;
    description: string | null;
    labels_concat: string | null;
  }>;

  const enriched = items.map(({ labels_concat, ...rest }) => ({
    ...rest,
    labels: labels_concat ? [...new Set(labels_concat.split('||'))] : [],
  }));

  const allLabels = (
    db.prepare('SELECT DISTINCT label FROM company_labels ORDER BY label').all() as { label: string }[]
  ).map((r) => r.label);

  res.json({ items: enriched, total, labels: allLabels });
});

// ── /api/companies/:id ─────────────────────────────────────────────────────

app.get('/api/companies/:id', (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) {
    res.status(400).json({ error: 'Invalid company id' });
    return;
  }

  const company = db.prepare('SELECT * FROM companies WHERE id = ?').get(id) as
    | {
        id: number;
        name: string;
        domain: string | null;
        email_count: number;
        first_seen: string | null;
        last_seen: string | null;
        homepage_fetched_at: string | null;
        description: string | null;
      }
    | undefined;

  if (!company) {
    res.status(404).json({ error: 'Company not found' });
    return;
  }

  const labels = db
    .prepare(
      'SELECT label, confidence, reasoning, model_used, assigned_at FROM company_labels WHERE company_id = ?'
    )
    .all(id) as Array<{
    label: string;
    confidence: number | null;
    reasoning: string | null;
    model_used: string | null;
    assigned_at: string | null;
  }>;

  const contacts = db
    .prepare(
      `SELECT ct.id, ct.email, ct.name, ct.email_count, ct.sent_count, ct.received_count, ct.last_seen
       FROM contacts ct
       INNER JOIN company_contacts cc ON cc.contact_email = ct.email
       WHERE cc.company_id = ?
       ORDER BY ct.email_count DESC
       LIMIT 50`
    )
    .all(id) as Array<{
    id: number;
    email: string;
    name: string | null;
    email_count: number;
    sent_count: number;
    received_count: number;
    last_seen: string | null;
  }>;

  const discussionsRaw = db
    .prepare(
      `SELECT id, title, category, current_state, summary, participants, first_seen, last_seen
       FROM discussions WHERE company_id = ?
       ORDER BY last_seen DESC`
    )
    .all(id) as Array<{
    id: number;
    title: string;
    category: string | null;
    current_state: string | null;
    summary: string | null;
    participants: string | null;
    first_seen: string | null;
    last_seen: string | null;
  }>;

  const discussions = discussionsRaw.map((d) => ({
    ...d,
    participants: parseJsonField<string[]>(d.participants) ?? [],
  }));

  res.json({ ...company, labels, contacts, discussions });
});

// ── /api/companies/:id/homepage ────────────────────────────────────────────

app.get('/api/companies/:id/homepage', (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) {
    res.status(400).json({ error: 'Invalid company id' });
    return;
  }

  const company = db.prepare('SELECT domain, homepage_fetched_at FROM companies WHERE id = ?').get(id) as
    | { domain: string | null; homepage_fetched_at: string | null }
    | undefined;

  if (!company) {
    res.status(404).json({ error: 'Company not found' });
    return;
  }

  if (!company.homepage_fetched_at || !company.domain) {
    res.status(404).json({ error: 'Homepage not fetched' });
    return;
  }

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

// ── /api/contacts ──────────────────────────────────────────────────────────

app.get('/api/contacts', (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const company = (req.query.company as string) ?? '';
  const sort = (req.query.sort as string) ?? 'email_count';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = {
    email_count: 'ct.email_count',
    name: 'ct.name',
    last_seen: 'ct.last_seen',
  };
  const sortCol = allowedSorts[sort] ?? 'ct.email_count';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) {
    where.push('(ct.name LIKE ? OR ct.email LIKE ?)');
    params.push(`%${q}%`, `%${q}%`);
  }

  if (company) {
    where.push('ct.company LIKE ?');
    params.push(`%${company}%`);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = db
    .prepare(`SELECT COUNT(*) AS cnt FROM contacts ct ${whereClause}`)
    .get(...params) as { cnt: number };
  const total = totalRow.cnt;

  const items = db
    .prepare(
      `SELECT ct.id, ct.email, ct.name, ct.company, ct.first_seen, ct.last_seen,
              ct.email_count, ct.sent_count, ct.received_count
       FROM contacts ct
       ${whereClause}
       ORDER BY ${sortCol} ${order}
       LIMIT ? OFFSET ?`
    )
    .all(...params, limit, offset) as Array<{
    id: number;
    email: string;
    name: string | null;
    company: string | null;
    first_seen: string | null;
    last_seen: string | null;
    email_count: number;
    sent_count: number;
    received_count: number;
  }>;

  const companies = (
    db
      .prepare('SELECT DISTINCT company FROM contacts WHERE company IS NOT NULL ORDER BY company')
      .all() as { company: string }[]
  ).map((r) => r.company);

  res.json({ items, total, companies });
});

// ── /api/contacts/:email ───────────────────────────────────────────────────

app.get('/api/contacts/:email', (req: Request, res: Response) => {
  const email = decodeURIComponent(req.params.email);

  const contact = db.prepare('SELECT * FROM contacts WHERE email = ?').get(email) as
    | {
        id: number;
        email: string;
        name: string | null;
        company: string | null;
        first_seen: string | null;
        last_seen: string | null;
        email_count: number;
        sent_count: number;
        received_count: number;
      }
    | undefined;

  if (!contact) {
    res.status(404).json({ error: 'Contact not found' });
    return;
  }

  const memoryRaw = db.prepare('SELECT * FROM contact_memories WHERE email = ?').get(email) as
    | {
        email: string;
        name: string | null;
        relationship: string | null;
        summary: string | null;
        discussions: string | null;
        key_facts: string | null;
        model_used: string | null;
        strategy_used: string | null;
        generated_at: string | null;
      }
    | undefined;

  const memory = memoryRaw
    ? {
        ...memoryRaw,
        discussions:
          parseJsonField<Array<{ topic: string; status: string }>>(memoryRaw.discussions) ?? [],
        key_facts: parseJsonField<string[]>(memoryRaw.key_facts) ?? [],
      }
    : null;

  let threads = db
    .prepare(
      `SELECT t.id, t.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.participants, t.summary
       FROM threads t
       INNER JOIN (
         SELECT DISTINCT dt.thread_id
         FROM discussion_threads dt
         INNER JOIN discussions d ON d.id = dt.discussion_id
         INNER JOIN company_contacts cc ON cc.company_id = d.company_id
         WHERE cc.contact_email = ?
       ) linked ON linked.thread_id = t.thread_id
       ORDER BY t.last_date DESC
       LIMIT 20`
    )
    .all(email) as Array<{
    id: number;
    thread_id: string;
    subject: string | null;
    email_count: number;
    first_date: string | null;
    last_date: string | null;
    participants: string | null;
    summary: string | null;
  }>;

  if (threads.length === 0) {
    threads = db
      .prepare(
        `SELECT id, thread_id, subject, email_count, first_date, last_date, participants, summary
         FROM threads
         WHERE participants LIKE ?
         ORDER BY last_date DESC
         LIMIT 20`
      )
      .all(`%${email}%`) as typeof threads;
  }

  const enrichedThreads = threads.map((t) => ({
    ...t,
    participants: parseJsonField<string[]>(t.participants) ?? [],
  }));

  res.json({ ...contact, memory, threads: enrichedThreads });
});

// ── /api/discussions ───────────────────────────────────────────────────────

app.get('/api/discussions', (req: Request, res: Response) => {
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

  const allowedSorts: Record<string, string> = {
    last_seen: 'd.last_seen',
    first_seen: 'd.first_seen',
    title: 'd.title',
  };
  const sortCol = allowedSorts[sort] ?? 'd.last_seen';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) {
    where.push('(d.title LIKE ? OR d.summary LIKE ?)');
    params.push(`%${q}%`, `%${q}%`);
  }

  if (category) {
    where.push('d.category = ?');
    params.push(category);
  }

  if (state) {
    where.push('d.current_state = ?');
    params.push(state);
  }

  if (exclude_states) {
    const excluded = exclude_states.split(',').filter(Boolean);
    if (excluded.length > 0) {
      const placeholders = excluded.map(() => '?').join(', ');
      where.push(`(d.current_state IS NULL OR d.current_state NOT IN (${placeholders}))`);
      params.push(...excluded);
    }
  }

  if (company_id) {
    where.push('d.company_id = ?');
    params.push(company_id);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = db
    .prepare(`SELECT COUNT(*) AS cnt FROM discussions d ${whereClause}`)
    .get(...params) as { cnt: number };
  const total = totalRow.cnt;

  const items = db
    .prepare(
      `SELECT d.id, d.title, d.category, d.current_state, d.company_id, d.summary,
              d.participants, d.first_seen, d.last_seen, d.updated_at,
              c.name AS company_name
       FROM discussions d
       LEFT JOIN companies c ON c.id = d.company_id
       ${whereClause}
       ORDER BY ${sortCol} ${order}
       LIMIT ? OFFSET ?`
    )
    .all(...params, limit, offset) as Array<{
    id: number;
    title: string;
    category: string | null;
    current_state: string | null;
    company_id: number | null;
    summary: string | null;
    participants: string | null;
    first_seen: string | null;
    last_seen: string | null;
    updated_at: string | null;
    company_name: string | null;
  }>;

  const enriched = items.map((d) => ({
    ...d,
    participants: parseJsonField<string[]>(d.participants) ?? [],
  }));

  const categories = (
    db
      .prepare(
        'SELECT DISTINCT category FROM discussions WHERE category IS NOT NULL ORDER BY category'
      )
      .all() as { category: string }[]
  ).map((r) => r.category);

  const states = (
    db
      .prepare(
        'SELECT DISTINCT current_state FROM discussions WHERE current_state IS NOT NULL ORDER BY current_state'
      )
      .all() as { current_state: string }[]
  ).map((r) => r.current_state);

  res.json({ items: enriched, total, categories, states });
});

// ── /api/discussions/:id ───────────────────────────────────────────────────

app.get('/api/discussions/:id', (req: Request, res: Response) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) {
    res.status(400).json({ error: 'Invalid discussion id' });
    return;
  }

  const discussion = db
    .prepare(
      `SELECT d.*, c.name AS company_name
       FROM discussions d
       LEFT JOIN companies c ON c.id = d.company_id
       WHERE d.id = ?`
    )
    .get(id) as
    | {
        id: number;
        title: string;
        category: string | null;
        current_state: string | null;
        company_id: number | null;
        company_name: string | null;
        summary: string | null;
        participants: string | null;
        first_seen: string | null;
        last_seen: string | null;
        updated_at: string | null;
      }
    | undefined;

  if (!discussion) {
    res.status(404).json({ error: 'Discussion not found' });
    return;
  }

  const stateHistory = db
    .prepare(
      `SELECT id, state, entered_at, reasoning, model_used, detected_at
       FROM discussion_state_history
       WHERE discussion_id = ?
       ORDER BY entered_at ASC`
    )
    .all(id) as Array<{
    id: number;
    state: string;
    entered_at: string | null;
    reasoning: string | null;
    model_used: string | null;
    detected_at: string | null;
  }>;

  const threadsRaw = db
    .prepare(
      `SELECT t.id, t.thread_id, t.subject, t.email_count, t.first_date, t.last_date, t.participants, t.summary
       FROM threads t
       INNER JOIN discussion_threads dt ON dt.thread_id = t.thread_id
       WHERE dt.discussion_id = ?
       ORDER BY t.last_date DESC`
    )
    .all(id) as Array<{
    id: number;
    thread_id: string;
    subject: string | null;
    email_count: number;
    first_date: string | null;
    last_date: string | null;
    participants: string | null;
    summary: string | null;
  }>;

  const threads = threadsRaw.map((t) => ({
    ...t,
    participants: parseJsonField<string[]>(t.participants) ?? [],
  }));

  const actionsRaw = db
    .prepare(
      `SELECT id, description, assignee_emails, target_date, status, source_date, completed_date
       FROM actions
       WHERE discussion_id = ?
       ORDER BY status ASC, source_date ASC`
    )
    .all(id) as Array<{
    id: number;
    description: string;
    assignee_emails: string | null;
    target_date: string | null;
    status: string;
    source_date: string | null;
    completed_date: string | null;
  }>;

  const actions = actionsRaw.map((a) => ({
    ...a,
    assignee_emails: parseJsonField<string[]>(a.assignee_emails) ?? [],
  }));

  const calendarEventsRaw = db
    .prepare(
      `SELECT ce.id, ce.event_id, ce.title, ce.description, ce.location,
              ce.start_time, ce.end_time, ce.all_day, ce.status,
              ce.organizer_email, ce.attendees, ce.html_link,
              de.match_score, de.match_reason
       FROM calendar_events ce
       INNER JOIN discussion_events de ON de.event_id = ce.id
       WHERE de.discussion_id = ?
       ORDER BY ce.start_time DESC`
    )
    .all(id) as Array<{
    id: number;
    event_id: string;
    title: string | null;
    description: string | null;
    location: string | null;
    start_time: string;
    end_time: string;
    all_day: number;
    status: string | null;
    organizer_email: string | null;
    attendees: string | null;
    html_link: string | null;
    match_score: number | null;
    match_reason: string | null;
  }>;

  const calendarEvents = calendarEventsRaw.map((e) => ({
    ...e,
    all_day: !!e.all_day,
    attendees: parseJsonField<Array<{ email: string; name?: string; response_status?: string }>>(e.attendees) ?? [],
  }));

  // Events from the event ledger
  const events = db
    .prepare(
      `SELECT id, domain, type, actor, target, event_date, detail, confidence, thread_id, source_email_id
       FROM event_ledger
       WHERE discussion_id = ?
       ORDER BY event_date ASC, created_at ASC`
    )
    .all(id) as Array<{
    id: string;
    domain: string;
    type: string;
    actor: string | null;
    target: string | null;
    event_date: string | null;
    detail: string | null;
    confidence: number | null;
    thread_id: string | null;
    source_email_id: string | null;
  }>;

  // Milestones
  const milestonesRaw = db
    .prepare(
      `SELECT name, achieved, achieved_date, evidence_event_ids, confidence
       FROM milestones
       WHERE discussion_id = ?
       ORDER BY achieved DESC, achieved_date ASC NULLS LAST`
    )
    .all(id) as Array<{
    name: string;
    achieved: number;
    achieved_date: string | null;
    evidence_event_ids: string | null;
    confidence: number | null;
  }>;

  const milestones = milestonesRaw.map((m) => ({
    ...m,
    achieved: !!m.achieved,
    evidence_event_ids: parseJsonField<string[]>(m.evidence_event_ids) ?? [],
  }));

  res.json({
    ...discussion,
    participants: parseJsonField<string[]>(discussion.participants) ?? [],
    state_history: stateHistory,
    threads,
    actions,
    calendar_events: calendarEvents,
    events,
    milestones,
  });
});

// ── /api/threads/:threadId/emails ─────────────────────────────────────────

app.get('/api/threads/:threadId/emails', (req: Request, res: Response) => {
  const threadId = decodeURIComponent(req.params.threadId);

  const emails = db
    .prepare(
      `SELECT id, message_id, subject, from_address, from_name, to_addresses, cc_addresses,
              date, body_text
       FROM emails
       WHERE thread_id = ?
       ORDER BY date ASC`
    )
    .all(threadId) as Array<{
    id: number;
    message_id: string;
    subject: string | null;
    from_address: string;
    from_name: string | null;
    to_addresses: string | null;
    cc_addresses: string | null;
    date: string;
    body_text: string | null;
  }>;

  const enriched = emails.map((e) => ({
    ...e,
    to_addresses: parseJsonField<string[]>(e.to_addresses) ?? [],
    cc_addresses: parseJsonField<string[]>(e.cc_addresses) ?? [],
  }));

  res.json({ emails: enriched });
});

// ── /api/actions ──────────────────────────────────────────────────────────

app.get('/api/actions', (req: Request, res: Response) => {
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

  const allowedSorts: Record<string, string> = {
    status: 'a.status',
    target_date: 'a.target_date',
    source_date: 'a.source_date',
    assignee: 'a.assignee_emails',
  };
  const sortCol = allowedSorts[sort] ?? 'a.status';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) {
    where.push('(a.description LIKE ? OR a.assignee_emails LIKE ?)');
    params.push(`%${q}%`, `%${q}%`);
  }

  if (status) {
    where.push('a.status = ?');
    params.push(status);
  }

  if (assignee) {
    const assigneeList = assignee.split(',').map((a) => a.trim()).filter(Boolean);
    if (assigneeList.length === 1) {
      where.push('a.assignee_emails LIKE ?');
      params.push(`%${assigneeList[0]}%`);
    } else if (assigneeList.length > 1) {
      const clauses = assigneeList.map((a) => {
        params.push(`%${a}%`);
        return 'a.assignee_emails LIKE ?';
      });
      where.push(`(${clauses.join(' OR ')})`);
    }
  }

  if (company_id) {
    where.push('d.company_id = ?');
    params.push(company_id);
  }

  if (discussion_id) {
    where.push('a.discussion_id = ?');
    params.push(discussion_id);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = db
    .prepare(
      `SELECT COUNT(*) AS cnt FROM actions a
       JOIN discussions d ON a.discussion_id = d.id
       ${whereClause}`
    )
    .get(...params) as { cnt: number };
  const total = totalRow.cnt;

  const itemsRaw = db
    .prepare(
      `SELECT a.id, a.discussion_id, a.description, a.assignee_emails, a.target_date,
              a.status, a.source_date, a.completed_date,
              d.title AS discussion_title, d.company_id,
              c.name AS company_name
       FROM actions a
       JOIN discussions d ON a.discussion_id = d.id
       LEFT JOIN companies c ON c.id = d.company_id
       ${whereClause}
       ORDER BY ${sortCol} ${order}, a.target_date ASC NULLS LAST
       LIMIT ? OFFSET ?`
    )
    .all(...params, limit, offset) as Array<{
    id: number;
    discussion_id: number;
    description: string;
    assignee_emails: string | null;
    target_date: string | null;
    status: string;
    source_date: string | null;
    completed_date: string | null;
    discussion_title: string | null;
    company_id: number | null;
    company_name: string | null;
  }>;

  const items = itemsRaw.map((a) => ({
    ...a,
    assignee_emails: parseJsonField<string[]>(a.assignee_emails) ?? [],
  }));

  const statuses = (
    db
      .prepare('SELECT DISTINCT status FROM actions WHERE status IS NOT NULL ORDER BY status')
      .all() as { status: string }[]
  ).map((r) => r.status);

  // Extract unique assignees from JSON arrays
  const allAssigneeRows = db
    .prepare('SELECT assignee_emails FROM actions WHERE assignee_emails IS NOT NULL')
    .all() as { assignee_emails: string }[];
  const assigneeSet = new Set<string>();
  for (const row of allAssigneeRows) {
    const emails = parseJsonField<string[]>(row.assignee_emails);
    if (emails) {
      for (const e of emails) assigneeSet.add(e);
    }
  }
  const assignees = [...assigneeSet].sort();

  res.json({ items, total, statuses, assignees });
});

// ── /api/calendar-events ──────────────────────────────────────────────────

app.get('/api/calendar-events', (req: Request, res: Response) => {
  const q = (req.query.q as string) ?? '';
  const from = (req.query.from as string) ?? '';
  const to = (req.query.to as string) ?? '';
  const status = (req.query.status as string) ?? '';
  const sort = (req.query.sort as string) ?? 'start_time';
  const order = (req.query.order as string) === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 25));
  const offset = (page - 1) * limit;

  const allowedSorts: Record<string, string> = {
    start_time: 'ce.start_time',
    end_time: 'ce.end_time',
    title: 'ce.title',
  };
  const sortCol = allowedSorts[sort] ?? 'ce.start_time';

  const params: unknown[] = [];
  const where: string[] = [];

  if (q) {
    where.push('(ce.title LIKE ? OR ce.description LIKE ? OR ce.location LIKE ?)');
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }

  if (from) {
    where.push('ce.start_time >= ?');
    params.push(from);
  }

  if (to) {
    where.push('ce.start_time <= ?');
    params.push(to);
  }

  if (status) {
    where.push('ce.status = ?');
    params.push(status);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = db
    .prepare(`SELECT COUNT(*) AS cnt FROM calendar_events ce ${whereClause}`)
    .get(...params) as { cnt: number };
  const total = totalRow.cnt;

  const items = db
    .prepare(
      `SELECT ce.id, ce.event_id, ce.title, ce.description, ce.location,
              ce.start_time, ce.end_time, ce.all_day, ce.status,
              ce.organizer_email, ce.attendees, ce.html_link,
              de.discussion_id, d.title AS discussion_title
       FROM calendar_events ce
       LEFT JOIN discussion_events de ON de.event_id = ce.id
       LEFT JOIN discussions d ON d.id = de.discussion_id
       ${whereClause}
       ORDER BY ${sortCol} ${order}
       LIMIT ? OFFSET ?`
    )
    .all(...params, limit, offset) as Array<{
    id: number;
    event_id: string;
    title: string | null;
    description: string | null;
    location: string | null;
    start_time: string;
    end_time: string;
    all_day: number;
    status: string | null;
    organizer_email: string | null;
    attendees: string | null;
    html_link: string | null;
    discussion_id: number | null;
    discussion_title: string | null;
  }>;

  const enriched = items.map((e) => ({
    ...e,
    all_day: !!e.all_day,
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
  console.log(`Database: ${DB_PATH}`);
});
