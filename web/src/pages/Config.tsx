import { useEffect, useState, useCallback } from 'react';
import { api } from '../api';
import type { LabelDef, CategoryDef, EventTypeDef, MilestoneDef } from '../types';

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

// Generic list editor for {name, description} pairs (used for event_types and milestones)
function NameDescList({ items, onChange, namePlaceholder, descPlaceholder }: {
  items: Array<{ name: string; description: string }>;
  onChange: (items: Array<{ name: string; description: string }>) => void;
  namePlaceholder?: string;
  descPlaceholder?: string;
}) {
  const update = (i: number, field: 'name' | 'description', value: string) =>
    onChange(items.map((item, idx) => idx === i ? { ...item, [field]: value } : item));

  const remove = (i: number) => onChange(items.filter((_, idx) => idx !== i));

  const add = () => onChange([...items, { name: '', description: '' }]);

  return (
    <div className="space-y-1.5">
      {items.map((item, i) => (
        <div key={i} className="flex gap-2 group">
          <input
            value={item.name}
            onChange={e => update(i, 'name', e.target.value)}
            placeholder={namePlaceholder ?? 'name'}
            className="w-44 flex-shrink-0 px-2 py-1.5 text-xs font-mono border border-slate-200 rounded focus:ring-1 focus:ring-blue-500 outline-none"
          />
          <input
            value={item.description}
            onChange={e => update(i, 'description', e.target.value)}
            placeholder={descPlaceholder ?? 'Description'}
            className="flex-1 px-2 py-1.5 text-xs border border-slate-200 rounded focus:ring-1 focus:ring-blue-500 outline-none"
          />
          <button
            onClick={() => remove(i)}
            className="text-slate-300 hover:text-red-500 text-base leading-none opacity-0 group-hover:opacity-100 transition-opacity"
          >×</button>
        </div>
      ))}
      <button
        onClick={add}
        className="text-xs text-blue-600 hover:text-blue-700 mt-1"
      >
        + Add
      </button>
    </div>
  );
}

function SectionHeader({ label, count }: { label: string; count?: number }) {
  return (
    <div className="flex items-center gap-2 mt-4 mb-2">
      <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">{label}</span>
      {count !== undefined && <span className="text-xs text-slate-400">({count})</span>}
      <div className="flex-1 h-px bg-slate-100" />
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
    setLabels(prev => { const n = [...prev]; [n[i-1], n[i]] = [n[i], n[i-1]]; return n; });
  };
  const moveDown = (i: number) => {
    setLabels(prev => {
      if (i >= prev.length - 1) return prev;
      const n = [...prev]; [n[i], n[i+1]] = [n[i+1], n[i]]; return n;
    });
  };

  const save = async () => {
    setSaving(true); setError(null);
    try {
      await api.saveConfigLabels(labels);
      setOriginal(labels);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed');
    } finally { setSaving(false); }
  };

  if (loading) return <div className="animate-pulse space-y-3"><div className="h-10 bg-slate-100 rounded" /><div className="h-10 bg-slate-100 rounded" /></div>;

  return (
    <div>
      <p className="text-sm text-slate-500 mb-4">
        Define which labels the AI can assign to companies. Changes take effect on the next analysis run.
      </p>
      <SaveBar dirty={dirty} saving={saving} error={error} onSave={save} onReset={() => { setLabels(original); setError(null); }} />

      <div className="mb-2 grid grid-cols-[28px_180px_1fr_24px] gap-2 px-1">
        <div />
        <div className="text-xs font-medium text-slate-400 uppercase tracking-wider">Name</div>
        <div className="text-xs font-medium text-slate-400 uppercase tracking-wider">Description (shown to AI)</div>
        <div />
      </div>

      <div className="space-y-1.5">
        {labels.map((label, i) => (
          <div key={i} className="flex items-center gap-2 group">
            <div className="flex flex-col gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity w-6">
              <button onClick={() => moveUp(i)} disabled={i === 0} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-[10px]">▲</button>
              <button onClick={() => moveDown(i)} disabled={i === labels.length - 1} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-[10px]">▼</button>
            </div>
            <input
              value={label.name}
              onChange={e => update(i, 'name', e.target.value)}
              placeholder="label-name"
              className="w-44 flex-shrink-0 px-3 py-2 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none font-mono"
            />
            <input
              value={label.description}
              onChange={e => update(i, 'description', e.target.value)}
              placeholder="Description…"
              className="flex-1 px-3 py-2 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
            />
            <button
              onClick={() => remove(i)}
              className="text-slate-300 hover:text-red-500 transition-colors text-lg leading-none opacity-0 group-hover:opacity-100 w-6"
            >×</button>
          </div>
        ))}
      </div>

      <button onClick={add} className="mt-4 px-4 py-2 text-sm text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 transition-colors">
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
    if (val && !states.includes(val)) onChange([...states, val]);
    setInput('');
  };

  return (
    <div className="flex flex-wrap items-center gap-1.5 px-2 py-1.5 border border-slate-200 rounded-lg min-h-[38px] focus-within:ring-2 focus-within:ring-blue-500 focus-within:border-blue-500">
      {states.map(s => (
        <span key={s} className="inline-flex items-center gap-1 px-2 py-0.5 bg-slate-100 rounded text-xs font-mono text-slate-700">
          {s}
          <button onClick={() => onChange(states.filter(x => x !== s))} className="text-slate-400 hover:text-red-500 ml-0.5">×</button>
        </span>
      ))}
      <input
        value={input}
        onChange={e => setInput(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); commit(); }
          if (e.key === 'Backspace' && !input && states.length) onChange(states.slice(0, -1));
        }}
        onBlur={commit}
        placeholder={states.length === 0 ? 'Type a state, press Enter' : ''}
        className="flex-1 min-w-[120px] text-xs outline-none bg-transparent"
      />
    </div>
  );
}

function CheckboxList({ options, selected, onChange }: {
  options: string[];
  selected: string[];
  onChange: (s: string[]) => void;
}) {
  const toggle = (v: string) =>
    onChange(selected.includes(v) ? selected.filter(s => s !== v) : [...selected, v]);

  if (options.length === 0) return <p className="text-xs text-slate-400 italic">Add states above first</p>;

  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1.5">
      {options.map(s => (
        <label key={s} className="flex items-center gap-1.5 text-xs cursor-pointer">
          <input type="checkbox" checked={selected.includes(s)} onChange={() => toggle(s)} className="rounded" />
          <span className="font-mono text-slate-700">{s}</span>
        </label>
      ))}
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

  const set = <K extends keyof CategoryDef>(k: K, v: CategoryDef[K]) => onChange({ ...cat, [k]: v });

  const setWorkflowStates = (states: string[]) => onChange({
    ...cat,
    workflow_states: states,
    terminal_states: cat.terminal_states.filter(s => states.includes(s)),
  });

  const setEventTypes = (et: EventTypeDef[]) => onChange({
    ...cat,
    event_types: et,
    terminal_event_types: (cat.terminal_event_types ?? []).filter(n => et.some(e => e.name === n)),
  });

  const eventNames = (cat.event_types ?? []).map(e => e.name).filter(Boolean);

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden group">
      {/* Header row */}
      <div className="flex items-center gap-2 px-3 py-2.5 bg-slate-50 hover:bg-slate-100 transition-colors">
        <div className="flex flex-col gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
          <button onClick={onMoveUp} disabled={isFirst} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-[10px]">▲</button>
          <button onClick={onMoveDown} disabled={isLast} className="text-slate-300 hover:text-slate-600 disabled:opacity-0 leading-none text-[10px]">▼</button>
        </div>
        <button onClick={() => setOpen(!open)} className="flex-1 flex items-center gap-3 text-left min-w-0">
          <span className="font-mono text-sm font-semibold text-slate-800 flex-shrink-0">{cat.name || '(unnamed)'}</span>
          {cat.sub_discussion && (
            <span className="px-1.5 py-0.5 text-[10px] font-medium bg-purple-100 text-purple-700 rounded uppercase tracking-wider flex-shrink-0">sub</span>
          )}
          <span className="text-xs text-slate-400 truncate">{cat.description}</span>
          <span className="ml-auto text-xs text-slate-400 flex-shrink-0 pl-2">
            {cat.workflow_states.length} states{cat.event_types?.length ? `, ${cat.event_types.length} events` : ''}
          </span>
          <span className="text-slate-400 text-xs flex-shrink-0">{open ? '▲' : '▼'}</span>
        </button>
        <button
          onClick={onRemove}
          className="text-slate-300 hover:text-red-500 transition-colors text-lg leading-none opacity-0 group-hover:opacity-100 ml-1 flex-shrink-0"
          title="Remove category"
        >×</button>
      </div>

      {open && (
        <div className="p-4 border-t border-slate-200 space-y-4">

          {/* Name + description */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-slate-500 mb-1">Name (ID)</label>
              <input
                value={cat.name}
                onChange={e => set('name', e.target.value.toLowerCase().replace(/\s+/g, '-'))}
                className="w-full px-3 py-1.5 text-sm font-mono border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-500 mb-1">Description (shown to AI)</label>
              <input
                value={cat.description}
                onChange={e => set('description', e.target.value)}
                className="w-full px-3 py-1.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
              />
            </div>
          </div>

          {/* Sub-discussion flag */}
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input
              type="checkbox"
              checked={!!cat.sub_discussion}
              onChange={e => set('sub_discussion', e.target.checked || undefined)}
              className="rounded"
            />
            <span className="text-slate-700">Sub-discussion</span>
            <span className="text-xs text-slate-400">(nested inside another discussion)</span>
          </label>

          {/* Workflow states */}
          <div>
            <SectionHeader label="Workflow States" count={cat.workflow_states.length} />
            <StateTagInput states={cat.workflow_states} onChange={setWorkflowStates} />
          </div>

          {/* Terminal states */}
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1.5">
              Terminal States <span className="font-normal text-slate-400">(discussions in these states are considered closed)</span>
            </label>
            <CheckboxList
              options={cat.workflow_states}
              selected={cat.terminal_states}
              onChange={s => set('terminal_states', s)}
            />
          </div>

          {/* Event types */}
          <div>
            <SectionHeader label="Event Types" count={cat.event_types?.length ?? 0} />
            <NameDescList
              items={cat.event_types ?? []}
              onChange={setEventTypes}
              namePlaceholder="event_name"
              descPlaceholder="What this event represents"
            />
          </div>

          {/* Terminal event types */}
          {(cat.event_types?.length ?? 0) > 0 && (
            <div>
              <label className="block text-xs font-medium text-slate-500 mb-1.5">
                Terminal Event Types <span className="font-normal text-slate-400">(events that close the discussion)</span>
              </label>
              <CheckboxList
                options={eventNames}
                selected={cat.terminal_event_types ?? []}
                onChange={s => set('terminal_event_types', s)}
              />
            </div>
          )}

          {/* Milestones */}
          <div>
            <SectionHeader label="Milestones" count={cat.milestones?.length ?? 0} />
            <NameDescList
              items={cat.milestones ?? []}
              onChange={m => set('milestones', m)}
              namePlaceholder="milestone_name"
              descPlaceholder="What reaching this milestone means"
            />
          </div>

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
    setCategories(prev => { const n = [...prev]; [n[i-1], n[i]] = [n[i], n[i-1]]; return n; });
  };
  const moveDown = (i: number) => {
    setCategories(prev => {
      if (i >= prev.length - 1) return prev;
      const n = [...prev]; [n[i], n[i+1]] = [n[i+1], n[i]]; return n;
    });
  };

  const add = () => setCategories(prev => [...prev, {
    name: '',
    description: '',
    workflow_states: ['active', 'resolved', 'stale'],
    terminal_states: ['resolved', 'stale'],
    event_types: [],
    milestones: [],
  }]);

  const save = async () => {
    setSaving(true); setError(null);
    try {
      await api.saveConfigCategories(categories);
      setOriginal(categories);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed');
    } finally { setSaving(false); }
  };

  if (loading) return <div className="animate-pulse space-y-2"><div className="h-12 bg-slate-100 rounded-lg" /><div className="h-12 bg-slate-100 rounded-lg" /></div>;

  return (
    <div>
      <p className="text-sm text-slate-500 mb-4">
        Define discussion categories, their workflow states, event types, and milestones.
        All fields are written directly to <code className="text-xs bg-slate-100 px-1 rounded">discussion_categories.yaml</code>.
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

      <button onClick={add} className="mt-4 px-4 py-2 text-sm text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 transition-colors">
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
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
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
