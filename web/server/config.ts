import type { Express, Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import yaml from 'js-yaml';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function resolveConfigFile(names: string[]): string | null {
  for (const name of names) {
    const p = path.resolve(__dirname, '../../email-analyser', name);
    if (fs.existsSync(p)) return p;
    const p2 = path.resolve(__dirname, '../../', name);
    if (fs.existsSync(p2)) return p2;
  }
  return null;
}

const LABEL_FILE = resolveConfigFile(['company_labels.yaml']);
const CATEGORY_FILE = resolveConfigFile(['discussion_categories.yaml']);

// ── Types ─────────────────────────────────────────────────────────────────────

interface LabelDef { name: string; description: string; }

interface NameDesc { name: string; description: string; }

interface CategoryDef {
  name: string;
  description: string;
  workflow_states: string[];
  terminal_states: string[];
  sub_discussion?: boolean;
  event_types?: NameDesc[];
  terminal_event_types?: string[];
  milestones?: NameDesc[];
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function readLabels(): LabelDef[] {
  if (!LABEL_FILE) return [];
  const raw = yaml.load(fs.readFileSync(LABEL_FILE, 'utf8')) as { labels?: LabelDef[] } | null;
  return raw?.labels ?? [];
}

function writeLabels(labels: LabelDef[]): void {
  if (!LABEL_FILE) throw new Error('company_labels.yaml not found');
  const content = yaml.dump({ labels }, { lineWidth: 120, quotingType: '"' });
  fs.writeFileSync(LABEL_FILE, content, 'utf8');
}

function readCategories(): CategoryDef[] {
  if (!CATEGORY_FILE) return [];
  const raw = yaml.load(fs.readFileSync(CATEGORY_FILE, 'utf8')) as { categories?: CategoryDef[] } | null;
  return raw?.categories ?? [];
}

function writeCategories(categories: CategoryDef[]): void {
  if (!CATEGORY_FILE) throw new Error('discussion_categories.yaml not found');
  const content = yaml.dump({ categories }, { lineWidth: 120, quotingType: '"' });
  fs.writeFileSync(CATEGORY_FILE, content, 'utf8');
}

function cleanCategory(c: Partial<CategoryDef>): CategoryDef {
  const out: CategoryDef = {
    name: (c.name ?? '').trim(),
    description: (c.description ?? '').trim(),
    workflow_states: c.workflow_states ?? [],
    terminal_states: (c.terminal_states ?? []).filter(s => (c.workflow_states ?? []).includes(s)),
  };
  if (c.sub_discussion) out.sub_discussion = true;
  if (c.event_types?.length) {
    out.event_types = c.event_types.map(e => ({ name: e.name.trim(), description: e.description.trim() }));
    const eventNames = new Set(out.event_types.map(e => e.name));
    const termEvents = (c.terminal_event_types ?? []).filter(n => eventNames.has(n));
    if (termEvents.length) out.terminal_event_types = termEvents;
  }
  if (c.milestones?.length) {
    out.milestones = c.milestones.map(m => ({ name: m.name.trim(), description: m.description.trim() }));
  }
  return out;
}

// ── Routes ────────────────────────────────────────────────────────────────────

export function registerConfigRoutes(app: Express): void {

  // GET /api/config/labels
  app.get('/api/config/labels', (_req: Request, res: Response) => {
    try {
      res.json({ labels: readLabels(), filePath: LABEL_FILE });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // PUT /api/config/labels — body: { labels: [{name, description}] }
  app.put('/api/config/labels', (req: Request, res: Response) => {
    const { labels } = req.body as { labels?: LabelDef[] };
    if (!Array.isArray(labels)) {
      res.status(400).json({ error: 'labels must be an array' }); return;
    }
    for (const l of labels) {
      if (!l.name?.trim()) { res.status(400).json({ error: 'Each label must have a name' }); return; }
    }
    try {
      writeLabels(labels.map(l => ({ name: l.name.trim(), description: (l.description ?? '').trim() })));
      res.json({ ok: true, count: labels.length });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // GET /api/config/categories
  app.get('/api/config/categories', (_req: Request, res: Response) => {
    try {
      res.json({ categories: readCategories(), filePath: CATEGORY_FILE });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // PUT /api/config/categories — body: { categories: [CategoryDef] }
  // The client sends the full structure (including event_types, milestones, sub_discussion).
  app.put('/api/config/categories', (req: Request, res: Response) => {
    const { categories } = req.body as { categories?: Partial<CategoryDef>[] };
    if (!Array.isArray(categories)) {
      res.status(400).json({ error: 'categories must be an array' }); return;
    }
    for (const c of categories) {
      if (!c.name?.trim()) { res.status(400).json({ error: 'Each category must have a name' }); return; }
    }
    try {
      writeCategories(categories.map(cleanCategory));
      res.json({ ok: true, count: categories.length });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });
}
