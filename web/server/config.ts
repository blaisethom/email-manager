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

interface CategoryDef {
  name: string;
  description: string;
  workflow_states: string[];
  terminal_states: string[];
  event_types?: Array<{ name: string; description: string }>;
  terminal_event_types?: string[];
  milestones?: Array<{ name: string; description: string }>;
  sub_discussion?: boolean;
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

  // PUT /api/config/labels
  // body: { labels: [{name, description}] }
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

  // PUT /api/config/categories
  // body: { categories: [{name, description, workflow_states, terminal_states, ...rest}] }
  // Merges with existing to preserve event_types, milestones, etc. that the UI doesn't edit.
  app.put('/api/config/categories', (req: Request, res: Response) => {
    const { categories } = req.body as { categories?: Partial<CategoryDef>[] };
    if (!Array.isArray(categories)) {
      res.status(400).json({ error: 'categories must be an array' }); return;
    }
    for (const c of categories) {
      if (!c.name?.trim()) { res.status(400).json({ error: 'Each category must have a name' }); return; }
    }
    try {
      // Read existing to preserve fields the UI doesn't edit
      const existing = readCategories();
      const existingByName = new Map(existing.map(c => [c.name, c]));

      const merged: CategoryDef[] = categories.map(patch => {
        const old = existingByName.get(patch.name ?? '') ?? {};
        return {
          name: (patch.name ?? '').trim(),
          description: (patch.description ?? '').trim(),
          workflow_states: patch.workflow_states ?? (old as CategoryDef).workflow_states ?? [],
          terminal_states: patch.terminal_states ?? (old as CategoryDef).terminal_states ?? [],
          // Preserve fields the UI doesn't touch
          ...((old as CategoryDef).event_types ? { event_types: (old as CategoryDef).event_types } : {}),
          ...((old as CategoryDef).terminal_event_types ? { terminal_event_types: (old as CategoryDef).terminal_event_types } : {}),
          ...((old as CategoryDef).milestones ? { milestones: (old as CategoryDef).milestones } : {}),
          ...((old as CategoryDef).sub_discussion ? { sub_discussion: (old as CategoryDef).sub_discussion } : {}),
        };
      });

      writeCategories(merged);
      res.json({ ok: true, count: merged.length });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });
}
