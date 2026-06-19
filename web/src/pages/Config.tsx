import { useEffect, useState, useCallback } from 'react';
import { api } from '../api';
import type { LabelDef, CategoryDef } from '../types';

// ── Shared helpers ────────────────────────────────────────────────────────────

function SaveBar({ dirty, saving, error, onSave, onReset }: {
  dirty: boolean;
  saving: boolean;
  error: string | null;
  onSave: () => void;
  onReset: () => void;
}) {
  if (!dirty && !error) return null;
  return (
    <div className="sticky top-0 z-10 flex items-center justify-between gap-4 px-4 py-2.5 mb-4 bg-amber-50 border border-amber-200 rounded-lg">
      <span className="text-sm text-amber-800 font-medium">
        {error ? `Error: ${error}` : 'Unsaved changes'}
      </span>
      <div className="flex gap-2">
        <button onClick={onReset} disabled={saving} className="px-3 py-1.5 text-sm text-slate-600 hover:text-slate-800 disabled:opacity-50">
          Reset
        </button>
        <button
          onClick={onSave}
          disabled={saving}
          className="px-4 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {saving ? 'Saving…' : 'Save changes'}
        </button>
      </div>
    </div>
  );
}

// ── Label editor ──────────────────────────────────────────────────────────────

function LabelsConfig() {
  const [labels, setLabels] = useState<LabelDef[]>([]);
  const [original, setOriginal] = useState<LabelDef[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    api.getConfigLabels()
      .then(({ labels: l }) => { setLabels(l); setOriginal(l); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const dirty = JSON.stringify(labels) !== JSON.stringify(original);

  const update = (i: number, field: keyof LabelDef, value: string) => {
    setError(null);
    setLabels(prev => prev.map((l, idx) => idx === i ? { ...l, [field]: value } : l));
  };

  const add = () => setLabels(prev => [...prev, { name: '', description: '' }]);

  const remove = (i: number) => setLabels(prev => prev.filter((_, idx) => idx !== i));

  const moveUp = (i: number) => {
    if (i === 0) return;
    setLabels(prev => {
      const next = [...prev];
      [next[i - 1], next[i]] = [next[i], next[i - 1]];
      return next;
    });
  };

  const moveDown = (i: number) => {
    setLabels(prev => {
      if (i >= prev.length - 1) return prev;
      const next = [...prev];
      [next[i], next[i + 1]] = [next[i + 1], next[i]];
      return next;
    });
  };

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      await api.saveConfigLabels(labels);
      setOriginal(labels);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  if (loading) return <div className="animate-pulse space-y-3"><div className="h-10 bg-slate-100 rounded" /><div className="h-10 bg-slate-100 rounded" /></div>;

  return (
    <div>
      <p className="text-sm text-slate-500 mb-4">
        Define which labels the AI can assign to companies. Changes take effect on the next analysis run.
      </p>
      <SaveBar dirty={dirty} saving={saving} error={error} onSave={save} onReset={() => { setLabels(original); setError(null); }} />

      <div className="space-y-2">
        {labels.map((label, i) => (
          <div key={i} className="flex items-start gap-2 group">
            <div className="flex flex-col gap-0.5 pt-2 opacity-0 group-hover:opacity-100 transition-opacity">
              <button onClick={() => moveUp(i)} disabled={i === 0} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-xs">▲</button>
              <button onClick={() => moveDown(i)} disabled={i === labels.length - 1} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-xs">▼</button>
            </div>
            <div className="flex-1 grid grid-cols-[180px_1fr] gap-2">
              <input
                value={label.name}
                onChange={e => update(i, 'name', e.target.value)}
                placeholder="label-name"
                className="px-3 py-2 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none font-mono"
              />
              <input
                value={label.description}
                onChange={e => update(i, 'description', e.target.value)}
                placeholder="Description shown to the AI when classifying companies"
                className="px-3 py-2 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
              />
            </div>
            <button
              onClick={() => remove(i)}
              className="mt-2 text-slate-300 hover:text-red-500 transition-colors text-lg leading-none opacity-0 group-hover:opacity-100"
              title="Remove"
            >
              ×
            </button>
          </div>
        ))}
      </div>

      <button
        onClick={add}
        className="mt-4 px-4 py-2 text-sm text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 transition-colors"
      >
        + Add label
      </button>
    </div>
  );
}

// ── Workflow editor ───────────────────────────────────────────────────────────

function StateTagInput({ states, onChange }: { states: string[]; onChange: (s: string[]) => void }) {
  const [input, setInput] = useState('');

  const commit = () => {
    const val = input.trim().toLowerCase().replace(/\s+/g, '_');
    if (val && !states.includes(val)) {
      onChange([...states, val]);
    }
    setInput('');
  };

  const remove = (s: string) => onChange(states.filter(x => x !== s));

  return (
    <div className="flex flex-wrap items-center gap-1.5 px-2 py-1.5 border border-slate-200 rounded-lg min-h-[38px] focus-within:ring-2 focus-within:ring-blue-500 focus-within:border-blue-500">
      {states.map(s => (
        <span key={s} className="inline-flex items-center gap-1 px-2 py-0.5 bg-slate-100 rounded text-xs font-mono text-slate-700">
          {s}
          <button onClick={() => remove(s)} className="text-slate-400 hover:text-red-500 ml-0.5">×</button>
        </span>
      ))}
      <input
        value={input}
        onChange={e => setInput(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); commit(); }
          if (e.key === 'Backspace' && !input && states.length) {
            onChange(states.slice(0, -1));
          }
        }}
        onBlur={commit}
        placeholder={states.length === 0 ? 'Type a state, press Enter' : ''}
        className="flex-1 min-w-[120px] text-xs outline-none bg-transparent"
      />
    </div>
  );
}

function CategoryEditor({ cat, onChange, onRemove, onMoveUp, onMoveDown, isFirst, isLast }: {
  cat: CategoryDef;
  onChange: (c: CategoryDef) => void;
  onRemove: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  isFirst: boolean;
  isLast: boolean;
}) {
  const [open, setOpen] = useState(false);

  const set = <K extends keyof CategoryDef>(k: K, v: CategoryDef[K]) =>
    onChange({ ...cat, [k]: v });

  const toggleTerminal = (state: string) => {
    const next = cat.terminal_states.includes(state)
      ? cat.terminal_states.filter(s => s !== state)
      : [...cat.terminal_states, state];
    set('terminal_states', next);
  };

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden group">
      <div className="flex items-center gap-2 px-4 py-3 bg-slate-50 hover:bg-slate-100 transition-colors">
        <div className="flex flex-col gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
          <button onClick={onMoveUp} disabled={isFirst} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-xs">▲</button>
          <button onClick={onMoveDown} disabled={isLast} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-xs">▼</button>
        </div>
        <button
          onClick={() => setOpen(!open)}
          className="flex-1 flex items-center gap-3 text-left"
        >
          <span className="font-mono text-sm font-medium text-slate-800">{cat.name || '(unnamed)'}</span>
          {cat.sub_discussion && (
            <span className="px-1.5 py-0.5 text-[10px] font-medium bg-purple-100 text-purple-700 rounded uppercase tracking-wider">sub</span>
          )}
          <span className="text-xs text-slate-400 truncate">{cat.description}</span>
          <span className="ml-auto text-xs text-slate-400 flex-shrink-0">
            {cat.workflow_states.length} states
          </span>
          <span className="text-slate-400 text-xs">{open ? '▲' : '▼'}</span>
        </button>
        <button
          onClick={onRemove}
          className="text-slate-300 hover:text-red-500 transition-colors text-lg leading-none opacity-0 group-hover:opacity-100 ml-1"
        >
          ×
        </button>
      </div>

      {open && (
        <div className="p-4 border-t border-slate-200 space-y-4">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">Name (ID)</label>
              <input
                value={cat.name}
                onChange={e => set('name', e.target.value.toLowerCase().replace(/\s+/g, '-'))}
                className="w-full px-3 py-1.5 text-sm font-mono border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">Description (shown to AI)</label>
              <input
                value={cat.description}
                onChange={e => set('description', e.target.value)}
                className="w-full px-3 py-1.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
              />
            </div>
          </div>

          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">
              Workflow states <span className="font-normal text-slate-400">(type then Enter to add)</span>
            </label>
            <StateTagInput
              states={cat.workflow_states}
              onChange={s => {
                // Remove any terminal_states that no longer exist
                set('workflow_states', s);
                onChange({ ...cat, workflow_states: s, terminal_states: cat.terminal_states.filter(t => s.includes(t)) });
              }}
            />
          </div>

          {cat.workflow_states.length > 0 && (
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-2">
                Terminal states <span className="font-normal text-slate-400">(discussions in these states are considered closed)</span>
              </label>
              <div className="flex flex-wrap gap-2">
                {cat.workflow_states.map(s => (
                  <label key={s} className="flex items-center gap-1.5 text-sm cursor-pointer">
                    <input
                      type="checkbox"
                      checked={cat.terminal_states.includes(s)}
                      onChange={() => toggleTerminal(s)}
                      className="rounded"
                    />
                    <span className="font-mono text-xs text-slate-700">{s}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function WorkflowsConfig() {
  const [categories, setCategories] = useState<CategoryDef[]>([]);
  const [original, setOriginal] = useState<CategoryDef[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    api.getConfigCategories()
      .then(({ categories: c }) => { setCategories(c); setOriginal(c); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const dirty = JSON.stringify(categories) !== JSON.stringify(original);

  const update = (i: number, c: CategoryDef) =>
    setCategories(prev => prev.map((x, idx) => idx === i ? c : x));

  const remove = (i: number) => setCategories(prev => prev.filter((_, idx) => idx !== i));

  const moveUp = (i: number) => {
    if (i === 0) return;
    setCategories(prev => {
      const next = [...prev];
      [next[i - 1], next[i]] = [next[i], next[i - 1]];
      return next;
    });
  };

  const moveDown = (i: number) => {
    setCategories(prev => {
      if (i >= prev.length - 1) return prev;
      const next = [...prev];
      [next[i], next[i + 1]] = [next[i + 1], next[i]];
      return next;
    });
  };

  const add = () => setCategories(prev => [...prev, {
    name: '',
    description: '',
    workflow_states: ['active', 'resolved', 'stale'],
    terminal_states: ['resolved', 'stale'],
  }]);

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      await api.saveConfigCategories(categories);
      setOriginal(categories);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  if (loading) return <div className="animate-pulse space-y-2"><div className="h-12 bg-slate-100 rounded-lg" /><div className="h-12 bg-slate-100 rounded-lg" /></div>;

  return (
    <div>
      <p className="text-sm text-slate-500 mb-4">
        Define discussion categories and their workflow states. The AI uses these to classify and track discussions.
        Event types and milestones (defined in the YAML) are preserved when saving.
      </p>
      <SaveBar dirty={dirty} saving={saving} error={error} onSave={save} onReset={() => { setCategories(original); setError(null); }} />

      <div className="space-y-2">
        {categories.map((cat, i) => (
          <CategoryEditor
            key={i}
            cat={cat}
            onChange={c => update(i, c)}
            onRemove={() => remove(i)}
            onMoveUp={() => moveUp(i)}
            onMoveDown={() => moveDown(i)}
            isFirst={i === 0}
            isLast={i === categories.length - 1}
          />
        ))}
      </div>

      <button
        onClick={add}
        className="mt-4 px-4 py-2 text-sm text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 transition-colors"
      >
        + Add workflow category
      </button>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

type Tab = 'labels' | 'workflows';

export default function ConfigPage() {
  const [tab, setTab] = useState<Tab>('labels');

  return (
    <div className="p-4 sm:p-8 max-w-4xl">
      <h1 className="text-2xl font-bold text-slate-900 mb-1">Configuration</h1>
      <p className="text-slate-500 text-sm mb-6">Edit label definitions and discussion workflow categories.</p>

      <div className="flex border-b border-slate-200 mb-6">
        {(['labels', 'workflows'] as Tab[]).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors capitalize ${
              tab === t
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300'
            }`}
          >
            {t === 'labels' ? 'Labels' : 'Workflow Categories'}
          </button>
        ))}
      </div>

      <div className="card p-6">
        {tab === 'labels' && <LabelsConfig />}
        {tab === 'workflows' && <WorkflowsConfig />}
      </div>
    </div>
  );
}
