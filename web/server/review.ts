import type { Express, Request, Response } from 'express';
import type { Database } from './db.js';

function nowIso() { return new Date().toISOString(); }

const GRANULARITY_CATEGORY = '__granularity__';

const GRANULARITY_RULES: Record<string, string> = {
  fewer: 'Prefer fewer, broader discussions. Merge related topics into a single discussion unless they are clearly separate business relationships or have a significant time gap (180+ days).',
  more:  'Prefer more specific, granular discussions. Create a separate discussion for each distinct product, contract, or workstream, even if they involve the same contacts.',
};

export function registerReviewRoutes(app: Express, db: Database): void {

  // Guard: ensure learned_rules and feedback exist even before Python migrations run
  db.exec(`CREATE TABLE IF NOT EXISTS learned_rules (
    id                  INTEGER PRIMARY KEY,
    layer               TEXT NOT NULL,
    category            TEXT,
    rule_text           TEXT NOT NULL,
    source_feedback_ids TEXT,
    active              INTEGER DEFAULT 1,
    created_at          TEXT NOT NULL
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS feedback (
    id          INTEGER PRIMARY KEY,
    layer       TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id   TEXT NOT NULL,
    action      TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    reason      TEXT,
    applied     INTEGER DEFAULT 0,
    created_at  TEXT NOT NULL
  )`);

  // ── Labels ─────────────────────────────────────────────────────────────────

  // GET /api/review/labels
  // Companies with AI-assigned labels, ordered by most-recently analysed.
  app.get('/api/review/labels', async (req: Request, res: Response) => {
    const { q, label, page: pageStr, limit: limitStr } = req.query as Record<string, string>;
    const page  = Math.max(1, parseInt(pageStr  || '1',  10));
    const limit = Math.min(100, Math.max(1, parseInt(limitStr || '50', 10)));
    const offset = (page - 1) * limit;

    const where: string[]   = ['c.id IN (SELECT DISTINCT company_id FROM company_labels)'];
    const params: unknown[] = [];

    if (q) {
      where.push('(c.name LIKE ? OR c.domain LIKE ?)');
      params.push(`%${q}%`, `%${q}%`);
    }
    if (label) {
      where.push('c.id IN (SELECT company_id FROM company_labels WHERE label = ?)');
      params.push(label);
    }

    const whereClause = `WHERE ${where.join(' AND ')}`;

    const total = (await db.queryOne<{ n: number }>(
      `SELECT COUNT(*) AS n FROM companies c ${whereClause}`, ...params,
    ))?.n ?? 0;

    const rows = await db.query<{
      company_id: number; domain: string | null; name: string | null;
      description: string | null; last_analysed_at: string | null;
    }>(
      `SELECT c.id AS company_id, c.domain, c.name, c.description,
              MAX(cl.assigned_at) AS last_analysed_at
       FROM companies c
       JOIN company_labels cl ON cl.company_id = c.id
       ${whereClause}
       GROUP BY c.id
       ORDER BY last_analysed_at DESC NULLS LAST
       LIMIT ? OFFSET ?`,
      ...params, limit, offset,
    );

    // Attach label details for each company
    const ids = rows.map(r => r.company_id);
    const labelRows = ids.length
      ? await db.query<{ company_id: number; label: string; confidence: number | null; reasoning: string | null; model_used: string | null }>(
          `SELECT company_id, label, confidence, reasoning, model_used
           FROM company_labels WHERE company_id IN (${ids.map(() => '?').join(',')})
           ORDER BY confidence DESC NULLS LAST`,
          ...ids,
        )
      : [];

    const labelsByCompany = new Map<number, typeof labelRows>();
    for (const r of labelRows) {
      if (!labelsByCompany.has(r.company_id)) labelsByCompany.set(r.company_id, []);
      labelsByCompany.get(r.company_id)!.push(r);
    }

    res.json({
      items: rows.map(r => ({ ...r, labels: labelsByCompany.get(r.company_id) ?? [] })),
      total,
    });
  });

  // POST /api/review/labels/:companyId
  // Replace the company's labels. Passing an empty array clears all labels
  // (so the AI will re-classify on the next run).
  app.post('/api/review/labels/:companyId', async (req: Request, res: Response) => {
    const companyId = parseInt(req.params.companyId, 10);
    if (isNaN(companyId)) { res.status(400).json({ error: 'Invalid company id' }); return; }

    const { labels, reason } = req.body as { labels: string[]; reason?: string };
    if (!Array.isArray(labels)) { res.status(400).json({ error: 'labels must be an array' }); return; }

    const existing = await db.query<{ label: string }>(
      'SELECT label FROM company_labels WHERE company_id = ?', companyId,
    );
    const oldLabels = existing.map(r => r.label);

    await db.query('DELETE FROM company_labels WHERE company_id = ?', companyId);

    const now = nowIso();
    for (const label of labels) {
      await db.query(
        `INSERT INTO company_labels (company_id, label, model_used, assigned_at)
         VALUES (?, ?, 'human', ?)`,
        companyId, label, now,
      );
    }

    await db.query(
      `INSERT INTO feedback (layer, target_type, target_id, action, old_value, new_value, reason, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      'labels', 'company', String(companyId), 'label_override',
      JSON.stringify(oldLabels), JSON.stringify(labels), reason ?? null, now,
    );

    // If a reason was given, convert it to a learned rule
    if (reason && reason.trim()) {
      const company = await db.queryOne<{ name: string | null; domain: string | null }>(
        'SELECT name, domain FROM companies WHERE id = ?', companyId,
      );
      const ruleText = `${company?.name ?? 'Company'} (${company?.domain ?? companyId}) labels are: ${labels.join(', ')}. ${reason.trim()}`;
      await db.query(
        `INSERT INTO learned_rules (layer, category, rule_text, created_at) VALUES (?, ?, ?, ?)`,
        'labels', null, ruleText, now,
      );
    }

    res.json({ ok: true, labels });
  });

  // ── Rules ──────────────────────────────────────────────────────────────────

  // GET /api/review/rules?layer=
  app.get('/api/review/rules', async (req: Request, res: Response) => {
    const { layer } = req.query as { layer?: string };
    const where = layer
      ? "WHERE layer = ? AND category != '__granularity__'"
      : "WHERE category IS NULL OR category != '__granularity__'";
    const params = layer ? [layer] : [];
    const rows = await db.query<{
      id: number; layer: string; category: string | null; rule_text: string;
      active: number; created_at: string; source_feedback_ids: string | null;
    }>(`SELECT id, layer, category, rule_text, active, created_at, source_feedback_ids
        FROM learned_rules ${where} ORDER BY layer, created_at DESC`, ...params);
    res.json(rows.map(r => ({ ...r, active: Boolean(r.active) })));
  });

  // POST /api/review/rules
  app.post('/api/review/rules', async (req: Request, res: Response) => {
    const { layer, category, rule_text } = req.body as { layer: string; category?: string; rule_text: string };
    if (!layer || !rule_text?.trim()) {
      res.status(400).json({ error: 'layer and rule_text are required' }); return;
    }
    const now = nowIso();
    const result = await db.queryOne<{ id: number }>(
      `INSERT INTO learned_rules (layer, category, rule_text, active, created_at)
       VALUES (?, ?, ?, 1, ?) RETURNING id`,
      layer, category ?? null, rule_text.trim(), now,
    );
    res.status(201).json({ id: result?.id, layer, category: category ?? null, rule_text: rule_text.trim(), active: true, created_at: now });
  });

  // PATCH /api/review/rules/:id
  app.patch('/api/review/rules/:id', async (req: Request, res: Response) => {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) { res.status(400).json({ error: 'Invalid rule id' }); return; }

    const { active, rule_text } = req.body as { active?: boolean; rule_text?: string };
    if (active === undefined && rule_text === undefined) {
      res.status(400).json({ error: 'Provide active or rule_text' }); return;
    }

    if (active !== undefined) {
      await db.query('UPDATE learned_rules SET active = ? WHERE id = ?', active ? 1 : 0, id);
    }
    if (rule_text !== undefined) {
      await db.query('UPDATE learned_rules SET rule_text = ? WHERE id = ?', rule_text.trim(), id);
    }
    res.json({ ok: true });
  });

  // DELETE /api/review/rules/:id
  app.delete('/api/review/rules/:id', async (req: Request, res: Response) => {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) { res.status(400).json({ error: 'Invalid rule id' }); return; }
    await db.query('DELETE FROM learned_rules WHERE id = ?', id);
    res.json({ ok: true });
  });

  // ── Granularity ────────────────────────────────────────────────────────────

  // GET /api/review/granularity
  app.get('/api/review/granularity', async (_req: Request, res: Response) => {
    const row = await db.queryOne<{ rule_text: string }>(
      `SELECT rule_text FROM learned_rules
       WHERE layer = 'discussions' AND category = ? AND active = 1 LIMIT 1`,
      GRANULARITY_CATEGORY,
    );
    if (!row) { res.json({ value: 'balanced' }); return; }
    const value = row.rule_text === GRANULARITY_RULES.fewer ? 'fewer'
      : row.rule_text === GRANULARITY_RULES.more ? 'more'
      : 'balanced';
    res.json({ value });
  });

  // POST /api/review/granularity — body: { value: 'fewer' | 'balanced' | 'more' }
  app.post('/api/review/granularity', async (req: Request, res: Response) => {
    const { value } = req.body as { value: string };
    if (!['fewer', 'balanced', 'more'].includes(value)) {
      res.status(400).json({ error: "value must be 'fewer', 'balanced', or 'more'" }); return;
    }

    // Replace any existing granularity rule
    await db.query(
      `DELETE FROM learned_rules WHERE layer = 'discussions' AND category = ?`,
      GRANULARITY_CATEGORY,
    );

    if (value !== 'balanced') {
      await db.query(
        `INSERT INTO learned_rules (layer, category, rule_text, active, created_at) VALUES (?, ?, ?, 1, ?)`,
        'discussions', GRANULARITY_CATEGORY, GRANULARITY_RULES[value], nowIso(),
      );
    }

    res.json({ ok: true, value });
  });

  // ── Per-company discussion feedback ────────────────────────────────────────

  // GET /api/review/companies/:companyId/discussion-feedback
  app.get('/api/review/companies/:companyId/discussion-feedback', async (req: Request, res: Response) => {
    const companyId = parseInt(req.params.companyId, 10);
    if (isNaN(companyId)) { res.status(400).json({ error: 'Invalid company id' }); return; }

    const rows = await db.query<{ id: number; rule_text: string; active: number; created_at: string }>(
      `SELECT id, rule_text, active, created_at FROM learned_rules
       WHERE layer = 'discussions' AND category = ? AND active = 1
       ORDER BY created_at DESC`,
      `__company_${companyId}__`,
    );
    res.json(rows.map(r => ({ ...r, active: Boolean(r.active) })));
  });

  // POST /api/review/companies/:companyId/discussion-feedback
  // body: { feedback: string } — plain-text instruction for the AI
  app.post('/api/review/companies/:companyId/discussion-feedback', async (req: Request, res: Response) => {
    const companyId = parseInt(req.params.companyId, 10);
    if (isNaN(companyId)) { res.status(400).json({ error: 'Invalid company id' }); return; }

    const { feedback } = req.body as { feedback?: string };
    if (!feedback?.trim()) { res.status(400).json({ error: 'feedback is required' }); return; }

    const company = await db.queryOne<{ name: string | null; domain: string | null }>(
      'SELECT name, domain FROM companies WHERE id = ?', companyId,
    );
    if (!company) { res.status(404).json({ error: 'Company not found' }); return; }

    const category = `__company_${companyId}__`;
    const now = nowIso();

    // Deactivate old feedback rules for this company
    await db.query(
      `UPDATE learned_rules SET active = 0 WHERE layer = 'discussions' AND category = ?`,
      category,
    );

    // Store the rule text as-is — company scope is tracked via the category field,
    // so no "For Company: " prefix is needed in the text itself.
    const ruleText = feedback.trim();

    const result = await db.queryOne<{ id: number }>(
      `INSERT INTO learned_rules (layer, category, rule_text, active, created_at)
       VALUES ('discussions', ?, ?, 1, ?) RETURNING id`,
      category, ruleText, now,
    );

    await db.query(
      `INSERT INTO feedback (layer, target_type, target_id, action, new_value, reason, created_at)
       VALUES ('discussions', 'company', ?, 'granularity_feedback', ?, ?, ?)`,
      String(companyId), ruleText, feedback.trim(), now,
    );

    res.status(201).json({ id: result?.id, rule_text: ruleText, active: true, created_at: now });
  });

  // ── Companies ───────────────────────────────────────────────────────────────

  // GET /api/review/companies
  // Companies that have been through the AI pipeline, ordered by most-recently analysed.
  app.get('/api/review/companies', async (req: Request, res: Response) => {
    const { q, stage, page: pageStr, limit: limitStr } = req.query as Record<string, string>;
    const page  = Math.max(1, parseInt(pageStr  || '1',  10));
    const limit = Math.min(200, Math.max(1, parseInt(limitStr || '100', 10)));
    const offset = (page - 1) * limit;

    const where: string[]   = ['pr.mode LIKE \'staged:%\' AND pr.error IS NULL'];
    const params: unknown[] = [];

    if (q) {
      where.push('(c.name LIKE ? OR c.domain LIKE ?)');
      params.push(`%${q}%`, `%${q}%`);
    }
    if (stage) {
      where.push('pr.mode = ?');
      params.push(`staged:${stage}`);
    }

    const whereClause = `WHERE ${where.join(' AND ')}`;

    const stageRows = await db.query<{ stage: string }>(
      `SELECT DISTINCT SUBSTR(mode, 8) AS stage
       FROM processing_runs
       WHERE mode LIKE 'staged:%' AND error IS NULL
       ORDER BY stage`,
    );

    const total = (await db.queryOne<{ n: number }>(
      `SELECT COUNT(DISTINCT c.id) AS n
       FROM companies c
       JOIN processing_runs pr ON LOWER(pr.company_domain) = LOWER(c.domain)
       ${whereClause}`,
      ...params,
    ))?.n ?? 0;

    const rows = await db.query<{
      company_id: number; name: string | null; domain: string | null;
      last_analysed_at: string | null; name_source: string | null;
    }>(
      `SELECT c.id AS company_id, c.name, c.domain, c.name_source,
              MAX(pr.started_at) AS last_analysed_at
       FROM companies c
       JOIN processing_runs pr ON LOWER(pr.company_domain) = LOWER(c.domain)
       ${whereClause}
       GROUP BY c.id
       ORDER BY last_analysed_at DESC
       LIMIT ? OFFSET ?`,
      ...params, limit, offset,
    );

    // Fetch distinct stages run per company in one query
    const domains = rows.map(r => r.domain).filter(Boolean) as string[];
    let stagesByDomain: Record<string, string[]> = {};
    if (domains.length > 0) {
      const perDomain = await db.query<{ domain: string; stage: string }>(
        `SELECT LOWER(pr2.company_domain) AS domain, SUBSTR(pr2.mode, 8) AS stage
         FROM processing_runs pr2
         WHERE pr2.mode LIKE 'staged:%' AND pr2.error IS NULL
           AND LOWER(pr2.company_domain) IN (${domains.map(() => '?').join(',')})
         GROUP BY LOWER(pr2.company_domain), pr2.mode
         ORDER BY stage`,
        ...domains.map(d => d.toLowerCase()),
      );
      for (const r of perDomain) {
        if (!stagesByDomain[r.domain]) stagesByDomain[r.domain] = [];
        stagesByDomain[r.domain].push(r.stage);
      }
    }

    const items = rows.map(r => ({
      ...r,
      stages_run: r.domain ? (stagesByDomain[r.domain.toLowerCase()] ?? []) : [],
    }));

    res.json({ items, total, stages: stageRows.map(r => r.stage) });
  });

  // PATCH /api/review/companies/:companyId/name
  // Update a company's display name; marks it as human-set so it won't be overwritten.
  app.patch('/api/review/companies/:companyId/name', async (req: Request, res: Response) => {
    const companyId = parseInt(req.params.companyId, 10);
    if (isNaN(companyId)) { res.status(400).json({ error: 'Invalid company id' }); return; }
    const { name } = req.body as { name?: string };
    if (!name?.trim()) { res.status(400).json({ error: 'name is required' }); return; }
    await db.query(
      `UPDATE companies SET name = ?, name_source = 'human' WHERE id = ?`,
      name.trim(), companyId,
    );
    res.json({ ok: true, name: name.trim(), name_source: 'human' });
  });
}
