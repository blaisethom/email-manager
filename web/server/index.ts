import express, { Request, Response } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { DatabaseSync } from 'node:sqlite';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_PATH = process.env.DB_PATH ?? path.resolve(__dirname, '../../data/email_manager.db');

const db = new DatabaseSync(DB_PATH, { open: true });
db.exec('PRAGMA journal_mode=WAL');

console.log(`Database opened: ${DB_PATH}`);

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
        (SELECT COALESCE(SUM(email_count), 0) FROM companies) AS emails`
    )
    .get() as { companies: number; contacts: number; discussions: number; emails: number };

  res.json({ labels, categories, states, stats });
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

  res.json({
    ...discussion,
    participants: parseJsonField<string[]>(discussion.participants) ?? [],
    state_history: stateHistory,
    threads,
  });
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
