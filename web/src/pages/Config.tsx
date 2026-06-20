import { useEffect, useState, useCallback } from 'react';
import { api } from '../api';
import type { LabelDef, CategoryDef, EventTypeDef, MilestoneDef, EmailAccount, ProkuraService } from '../types';

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

// ── Connections (accounts.json) ────────────────────────────────────────────────

function TagInput({ tags, onChange, placeholder }: {
  tags: string[];
  onChange: (t: string[]) => void;
  placeholder?: string;
}) {
  const [input, setInput] = useState('');
  const commit = () => {
    const v = input.trim();
    if (v && !tags.includes(v)) onChange([...tags, v]);
    setInput('');
  };
  return (
    <div className="flex flex-wrap items-center gap-1.5 px-2 py-1.5 border border-slate-200 rounded-lg min-h-[36px] focus-within:ring-2 focus-within:ring-blue-500 focus-within:border-blue-500">
      {tags.map(t => (
        <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 bg-slate-100 rounded text-xs font-mono text-slate-700">
          {t}
          <button type="button" onClick={() => onChange(tags.filter(x => x !== t))} className="text-slate-400 hover:text-red-500 ml-0.5">×</button>
        </span>
      ))}
      <input
        value={input}
        onChange={e => setInput(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); commit(); }
          if (e.key === 'Backspace' && !input && tags.length) onChange(tags.slice(0, -1));
        }}
        onBlur={commit}
        placeholder={tags.length === 0 ? (placeholder ?? 'Type and press Enter') : ''}
        className="flex-1 min-w-[100px] text-xs outline-none bg-transparent"
      />
    </div>
  );
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-500 mb-1">
        {label}
        {hint && <span className="ml-1 font-normal text-slate-400">{hint}</span>}
      </label>
      {children}
    </div>
  );
}

const INPUT = 'w-full px-3 py-1.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none';
const INPUT_MONO = INPUT + ' font-mono';

function expiryLabel(isoOrNull?: string | null): string {
  if (!isoOrNull || isoOrNull.startsWith('1970')) return 'no expiry';
  const d = new Date(isoOrNull);
  const now = new Date();
  const diffDays = Math.round((d.getTime() - now.getTime()) / 86_400_000);
  if (diffDays < 0) return 'expired';
  if (diffDays === 0) return 'expires today';
  if (diffDays === 1) return 'expires tomorrow';
  return `expires ${d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}`;
}

function ProkuraResourcePicker({ services, filterFn, onSelect, currentKey, label }: {
  services: ProkuraService[];
  filterFn: (s: ProkuraService) => boolean;
  onSelect: (s: ProkuraService) => void;
  currentKey?: string;
  label: string;
}) {
  const matches = services.filter(filterFn);
  if (matches.length === 0) return null;

  return (
    <div className="rounded-lg border border-purple-200 bg-purple-50 p-3">
      <div className="flex items-center gap-1.5 mb-2">
        <span className="text-[10px] font-bold text-purple-700 uppercase tracking-wider">Prokura</span>
        <span className="text-xs text-purple-600">{label}</span>
      </div>
      <div className="space-y-1.5">
        {matches.map(s => {
          const isActive = s.bearer_token_key && s.bearer_token_key === currentKey;
          return (
            <div
              key={s.service_slug}
              className={`flex items-center gap-2 px-2.5 py-1.5 rounded-lg border transition-colors ${
                isActive
                  ? 'bg-purple-100 border-purple-300'
                  : 'bg-white border-slate-200 hover:border-purple-300'
              }`}
            >
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-slate-800 truncate">
                    {s.account_email && s.account_email !== 'API Key' ? s.account_email : s.service_name}
                  </span>
                  {isActive && (
                    <span className="text-[10px] font-semibold text-purple-700 bg-purple-200 px-1.5 py-0.5 rounded flex-shrink-0">active</span>
                  )}
                </div>
                <div className="flex items-center gap-2 mt-0.5">
                  <code className="text-[10px] text-slate-500 font-mono">{s.bearer_token_key}</code>
                  <span className={`text-[10px] ${
                    s.token_expires_at?.startsWith('1970') || !s.token_expires_at ? 'text-slate-400' :
                    new Date(s.token_expires_at) < new Date() ? 'text-red-500' : 'text-green-600'
                  }`}>{expiryLabel(s.token_expires_at)}</span>
                </div>
              </div>
              {!isActive && (
                <button
                  type="button"
                  onClick={() => onSelect(s)}
                  className="flex-shrink-0 px-2.5 py-1 text-xs font-medium text-purple-700 bg-purple-100 hover:bg-purple-200 rounded-md transition-colors"
                >
                  Use
                </button>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function AccountEditor({ account, onChange, onRemove, isNew, prokuraServices }: {
  account: EmailAccount;
  onChange: (a: EmailAccount) => void;
  onRemove: () => void;
  isNew: boolean;
  prokuraServices: ProkuraService[];
}) {
  const [open, setOpen] = useState(isNew);
  const [showPassword, setShowPassword] = useState(false);
  const set = <K extends keyof EmailAccount>(k: K, v: EmailAccount[K]) => onChange({ ...account, [k]: v });

  const isGmail = account.backend === 'gmail';
  const hasHubSpot = !!(account.hubspot_bearer_token || account.hubspot_owner_email);
  const [hubspotExpanded, setHubspotExpanded] = useState(hasHubSpot);

  return (
    <div className="border border-slate-200 rounded-lg overflow-hidden group">
      <div className="flex items-center gap-2.5 px-3 py-2.5 bg-slate-50 hover:bg-slate-100 transition-colors">
        <button onClick={() => setOpen(!open)} className="flex-1 flex items-center gap-2.5 text-left min-w-0">
          <span className={`px-2 py-0.5 text-[10px] font-semibold rounded uppercase tracking-wider flex-shrink-0 ${
            isGmail ? 'bg-red-100 text-red-700' : 'bg-blue-100 text-blue-700'
          }`}>
            {isGmail ? 'Gmail' : 'IMAP'}
          </span>
          <span className="text-sm font-mono text-slate-800 font-medium truncate">
            {account.name || '(new account)'}
          </span>
          {isGmail && (
            <span className="px-1.5 py-0.5 text-[10px] bg-green-50 text-green-700 border border-green-200 rounded flex-shrink-0">
              + Calendar
            </span>
          )}
          {account.hubspot_bearer_token && (
            <span className="px-1.5 py-0.5 text-[10px] bg-orange-50 text-orange-700 border border-orange-200 rounded flex-shrink-0">
              HubSpot
            </span>
          )}
          <span className="ml-auto text-slate-400 text-xs flex-shrink-0">{open ? '▲' : '▼'}</span>
        </button>
        <button
          onClick={onRemove}
          className="text-slate-300 hover:text-red-500 transition-colors text-lg leading-none opacity-0 group-hover:opacity-100 ml-1 flex-shrink-0"
          title="Remove account"
        >×</button>
      </div>

      {open && (
        <div className="p-4 border-t border-slate-200 space-y-3">
          {/* Backend toggle */}
          <Field label="Backend">
            <div className="flex gap-2">
              {(['gmail', 'imap'] as const).map(b => (
                <button
                  key={b}
                  type="button"
                  onClick={() => set('backend', b)}
                  className={`px-4 py-1.5 text-sm rounded-lg border transition-colors font-medium ${
                    account.backend === b
                      ? 'bg-blue-600 text-white border-blue-600'
                      : 'bg-white text-slate-600 border-slate-200 hover:border-slate-400'
                  }`}
                >
                  {b === 'gmail' ? 'Gmail' : 'IMAP'}
                </button>
              ))}
            </div>
          </Field>

          {/* Name */}
          <Field label="Account name" hint="(used internally to identify this account)">
            <input
              value={account.name}
              onChange={e => set('name', e.target.value)}
              placeholder={isGmail ? 'you@gmail.com' : 'work-imap'}
              className={INPUT_MONO}
            />
          </Field>

          {isGmail ? (
            <>
              <ProkuraResourcePicker
                services={prokuraServices}
                filterFn={s => s.domain_pattern === 'gmail.googleapis.com' && !!s.bearer_token_key}
                currentKey={account.gmail_bearer_token}
                label="— select a Gmail credential"
                onSelect={s => onChange({
                  ...account,
                  gmail_bearer_token: s.bearer_token_key ?? '',
                  name: account.name || s.account_email || '',
                })}
              />
              <Field label="Bearer token" hint="(proxy-managed token key, or leave blank for local OAuth)">
                <input value={account.gmail_bearer_token ?? ''} onChange={e => set('gmail_bearer_token', e.target.value)} placeholder="GMAIL_TOKEN_EXAMPLE" className={INPUT_MONO} />
              </Field>
              <Field label="Credentials path" hint="(local OAuth — path to credentials.json)">
                <input value={account.gmail_credentials_path ?? ''} onChange={e => set('gmail_credentials_path', e.target.value)} placeholder="../data/gmail_credentials.json" className={INPUT} />
              </Field>
              <Field label="Token path" hint="(local OAuth — path to token.json)">
                <input value={account.gmail_token_path ?? ''} onChange={e => set('gmail_token_path', e.target.value)} placeholder="../data/gmail_token.json" className={INPUT} />
              </Field>
              <Field label="Gmail label IDs" hint="(leave empty to sync all mail; enter label IDs and press Enter)">
                <TagInput tags={account.gmail_labels ?? []} onChange={v => set('gmail_labels', v)} placeholder="Label/INBOX (empty = all mail)" />
              </Field>
            </>
          ) : (
            <>
              <Field label="IMAP host">
                <input value={account.imap_host ?? ''} onChange={e => set('imap_host', e.target.value)} placeholder="imap.example.com" className={INPUT_MONO} />
              </Field>
              <div className="grid grid-cols-2 gap-3">
                <Field label="IMAP user">
                  <input value={account.imap_user ?? ''} onChange={e => set('imap_user', e.target.value)} placeholder="you@example.com" className={INPUT_MONO} />
                </Field>
                <Field label="Password">
                  <div className="relative">
                    <input
                      type={showPassword ? 'text' : 'password'}
                      value={account.imap_password ?? ''}
                      onChange={e => set('imap_password', e.target.value)}
                      placeholder="••••••••"
                      className={INPUT_MONO + ' pr-16'}
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(s => !s)}
                      className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-slate-400 hover:text-slate-700"
                    >{showPassword ? 'Hide' : 'Show'}</button>
                  </div>
                </Field>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <Field label="Port">
                  <input type="number" value={account.imap_port ?? 993} onChange={e => set('imap_port', Number(e.target.value))} className={INPUT} />
                </Field>
                <Field label="SSL">
                  <label className="flex items-center gap-2 h-[34px] cursor-pointer">
                    <input type="checkbox" checked={account.imap_use_ssl !== false} onChange={e => set('imap_use_ssl', e.target.checked)} className="rounded" />
                    <span className="text-sm text-slate-700">Use SSL/TLS</span>
                  </label>
                </Field>
              </div>
              <Field label="Folders" hint='(* = all folders; press Enter after each)'>
                <TagInput tags={account.imap_folders ?? ['INBOX', 'Sent']} onChange={v => set('imap_folders', v)} placeholder="INBOX" />
              </Field>
            </>
          )}

          {/* HubSpot section */}
          <div className="pt-1 border-t border-slate-100">
            <button
              type="button"
              onClick={() => setHubspotExpanded(h => !h)}
              className="flex items-center gap-2 text-xs font-medium text-slate-500 hover:text-slate-700 py-1"
            >
              <span className="px-1.5 py-0.5 bg-orange-50 text-orange-700 border border-orange-200 rounded text-[10px]">HubSpot</span>
              <span>CRM integration</span>
              <span className="text-slate-400">{hubspotExpanded ? '▲' : '▼'}</span>
            </button>
            {hubspotExpanded && (
              <div className="mt-2 space-y-3">
                <ProkuraResourcePicker
                  services={prokuraServices}
                  filterFn={s => s.domain_pattern === 'api.hubapi.com' && !!s.bearer_token_key}
                  currentKey={account.hubspot_bearer_token}
                  label="— select a HubSpot credential"
                  onSelect={s => set('hubspot_bearer_token', s.bearer_token_key ?? '')}
                />
                <Field label="HubSpot bearer token" hint="(proxy-managed token key or direct private app token)">
                  <input value={account.hubspot_bearer_token ?? ''} onChange={e => set('hubspot_bearer_token', e.target.value)} placeholder="HUBSPOT_TOKEN" className={INPUT_MONO} />
                </Field>
                <Field label="Owner email" hint="(filter tasks/assignments to this user)">
                  <input value={account.hubspot_owner_email ?? ''} onChange={e => set('hubspot_owner_email', e.target.value)} placeholder="you@company.com" className={INPUT} />
                </Field>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function ConnectionsConfig() {
  const [accounts, setAccounts] = useState<EmailAccount[]>([]);
  const [original, setOriginal] = useState<EmailAccount[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [newCount, setNewCount] = useState(0);
  const [prokuraServices, setProkuraServices] = useState<ProkuraService[]>([]);

  const load = useCallback(() => {
    setLoading(true);
    Promise.all([
      api.getConfigAccounts(),
      api.getProkuraResources().catch(() => ({ available: false, services: [] as ProkuraService[] })),
    ]).then(([{ accounts: a }, prokura]) => {
      setAccounts(a);
      setOriginal(a);
      setProkuraServices(prokura.services ?? []);
    }).catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const dirty = JSON.stringify(accounts) !== JSON.stringify(original);

  const update = (i: number, a: EmailAccount) =>
    setAccounts(prev => prev.map((x, idx) => idx === i ? a : x));

  const remove = (i: number) => setAccounts(prev => prev.filter((_, idx) => idx !== i));

  const addAccount = (backend: 'gmail' | 'imap') => {
    setNewCount(n => n + 1);
    setAccounts(prev => [...prev, {
      name: '',
      backend,
      ...(backend === 'gmail' ? { gmail_labels: [] } : {
        imap_port: 993,
        imap_use_ssl: true,
        imap_folders: ['INBOX', 'Sent'],
      }),
    }]);
  };

  const save = async () => {
    setSaving(true); setError(null);
    try {
      await api.saveConfigAccounts(accounts);
      setOriginal(accounts);
      setNewCount(0);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed');
    } finally { setSaving(false); }
  };

  if (loading) return <div className="animate-pulse space-y-3"><div className="h-12 bg-slate-100 rounded-lg" /><div className="h-12 bg-slate-100 rounded-lg" /></div>;

  const newIdxStart = original.length;

  return (
    <div>
      <div className="flex items-start justify-between gap-4 mb-4">
        <p className="text-sm text-slate-500">
          Manage email accounts, calendar sync, and HubSpot connections.
          Changes are written to <code className="text-xs bg-slate-100 px-1 rounded">accounts.json</code> and take effect on the next sync run.
        </p>
        {prokuraServices.length > 0 && (
          <div className="flex-shrink-0 flex items-center gap-1.5 px-2.5 py-1 bg-purple-50 border border-purple-200 rounded-lg text-xs text-purple-700 whitespace-nowrap">
            <span className="font-bold uppercase tracking-wider text-[10px]">Prokura</span>
            <span>{prokuraServices.filter(s => !!s.bearer_token_key).length} credentials available</span>
          </div>
        )}
      </div>
      <SaveBar dirty={dirty} saving={saving} error={error} onSave={save} onReset={() => { setAccounts(original); setError(null); setNewCount(0); }} />

      <div className="space-y-2">
        {accounts.map((account, i) => (
          <AccountEditor
            key={`${i}-${account.name}-${account.backend}`}
            account={account}
            onChange={a => update(i, a)}
            onRemove={() => remove(i)}
            isNew={i >= newIdxStart}
            prokuraServices={prokuraServices}
          />
        ))}
      </div>

      {accounts.length === 0 && (
        <div className="py-10 text-center text-slate-400 border border-dashed border-slate-200 rounded-lg">
          No accounts configured. Add one below.
        </div>
      )}

      <div className="flex gap-2 mt-4">
        <button
          onClick={() => addAccount('gmail')}
          className="px-4 py-2 text-sm text-red-700 border border-red-200 rounded-lg hover:bg-red-50 transition-colors"
        >
          + Add Gmail account
        </button>
        <button
          onClick={() => addAccount('imap')}
          className="px-4 py-2 text-sm text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 transition-colors"
        >
          + Add IMAP account
        </button>
      </div>

      <div className="mt-6 p-3 bg-slate-50 rounded-lg border border-slate-200">
        <p className="text-xs text-slate-500 font-medium mb-1">Notes</p>
        <ul className="text-xs text-slate-400 space-y-1 list-disc list-inside">
          <li>Gmail accounts automatically sync Google Calendar events.</li>
          <li>For Gmail OAuth, run <code className="bg-slate-100 px-1 rounded">email-analyser auth &lt;account-name&gt;</code> to authenticate.</li>
          <li>Bearer tokens (e.g. <code className="bg-slate-100 px-1 rounded">GMAIL_TOKEN_EXAMPLE</code>) are resolved by an auth proxy at runtime.</li>
          <li>HubSpot can be added to any account — typically your primary work email.</li>
        </ul>
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

type Tab = 'connections' | 'labels' | 'workflows';

const TAB_LABELS: Record<Tab, string> = {
  connections: 'Connections',
  labels: 'Labels',
  workflows: 'Workflow Categories',
};

export default function ConfigPage() {
  const [tab, setTab] = useState<Tab>('connections');

  return (
    <div className="p-4 sm:p-8 max-w-4xl">
      <h1 className="text-2xl font-bold text-slate-900 mb-1">Configuration</h1>
      <p className="text-slate-500 text-sm mb-6">Manage accounts, label definitions, and discussion workflow categories.</p>

      <div className="flex border-b border-slate-200 mb-6">
        {(Object.keys(TAB_LABELS) as Tab[]).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              tab === t
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300'
            }`}
          >
            {TAB_LABELS[t]}
          </button>
        ))}
      </div>

      <div className="card p-6">
        {tab === 'connections' && <ConnectionsConfig />}
        {tab === 'labels' && <LabelsConfig />}
        {tab === 'workflows' && <WorkflowsConfig />}
      </div>
    </div>
  );
}
