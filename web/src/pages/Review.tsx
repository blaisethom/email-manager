import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../api';
import type { CategoryConfig, Granularity, LearnedRule, LabelConfig, ReviewCompany, PipelineCompany } from '../types';

// ── helpers ──────────────────────────────────────────────────────────────────

function formatDate(iso: string | null) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function ConfidenceBadge({ pct }: { pct: number | null }) {
  if (pct == null) return null;
  const p = Math.round(pct * 100);
  const cls = p >= 80 ? 'bg-green-100 text-green-700' : p >= 60 ? 'bg-yellow-100 text-yellow-700' : 'bg-red-100 text-red-700';
  return <span className={`text-xs px-1.5 py-0.5 rounded font-mono ${cls}`}>{p}%</span>;
}

// ── LABELS TAB ────────────────────────────────────────────────────────────────

function LabelsTab({ labelConfig }: { labelConfig: LabelConfig[] }) {
  const [items, setItems]     = useState<ReviewCompany[]>([]);
  const [total, setTotal]     = useState(0);
  const [q, setQ]             = useState('');
  const [filter, setFilter]   = useState('');
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState<number | null>(null);          // company_id being edited
  const [draft, setDraft]     = useState<string[]>([]);
  const [reason, setReason]   = useState('');
  const [saving, setSaving]   = useState(false);

  async function load() {
    setLoading(true);
    try {
      const data = await api.getReviewLabels({ q, label: filter });
      setItems(data.items);
      setTotal(data.total);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, [q, filter]); // eslint-disable-line

  function startEdit(company: ReviewCompany) {
    setEditing(company.company_id);
    setDraft(company.labels.map(l => l.label));
    setReason('');
  }

  function toggleLabel(name: string) {
    setDraft(d => d.includes(name) ? d.filter(l => l !== name) : [...d, name]);
  }

  async function save(companyId: number) {
    setSaving(true);
    try {
      await api.saveCompanyLabels(companyId, draft, reason || undefined);
      setItems(items.map(c =>
        c.company_id === companyId
          ? { ...c, labels: draft.map(l => ({ label: l, confidence: null, reasoning: reason || null, model_used: 'human' })) }
          : c
      ));
      setEditing(null);
    } catch (e) {
      alert(String(e));
    } finally {
      setSaving(false);
    }
  }

  const labelNames = labelConfig.length > 0
    ? labelConfig
    : [...new Set(items.flatMap(c => c.labels.map(l => l.label)))].sort().map(n => ({ name: n, description: '' }));

  return (
    <div className="space-y-4">
      <div className="flex gap-3">
        <input
          className="input flex-1"
          placeholder="Search companies…"
          value={q}
          onChange={e => setQ(e.target.value)}
        />
        <select className="input w-44" value={filter} onChange={e => setFilter(e.target.value)}>
          <option value="">All labels</option>
          {labelNames.map(l => <option key={l.name} value={l.name}>{l.name}</option>)}
        </select>
      </div>

      <p className="text-sm text-slate-500">{total} companies with AI labels</p>

      {loading ? (
        <p className="text-sm text-slate-400 py-8 text-center">Loading…</p>
      ) : items.length === 0 ? (
        <p className="text-sm text-slate-400 py-8 text-center">No companies match.</p>
      ) : (
        <div className="space-y-2">
          {items.map(company => (
            <div key={company.company_id} className="card">
              <div className="p-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <p className="font-medium text-slate-900 truncate">
                      {company.name ?? company.domain ?? `#${company.company_id}`}
                    </p>
                    <p className="text-xs text-slate-500">{company.domain} · analysed {formatDate(company.last_analysed_at)}</p>
                    <div className="flex flex-wrap gap-1.5 mt-2">
                      {company.labels.length === 0
                        ? <span className="text-xs text-slate-400 italic">no labels</span>
                        : company.labels.map(l => (
                          <span key={l.label} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-violet-100 text-violet-700">
                            {l.label}
                            <ConfidenceBadge pct={l.confidence} />
                          </span>
                        ))}
                    </div>
                  </div>
                  <button
                    className="btn-secondary text-xs shrink-0"
                    onClick={() => editing === company.company_id ? setEditing(null) : startEdit(company)}
                  >
                    {editing === company.company_id ? 'Cancel' : 'Edit labels'}
                  </button>
                </div>

                {/* Inline editor */}
                {editing === company.company_id && (
                  <div className="mt-4 border-t border-slate-100 pt-4 space-y-4">
                    {/* Reasoning for current labels */}
                    {company.labels.some(l => l.reasoning) && (
                      <div className="text-xs text-slate-500 space-y-1">
                        <p className="font-medium text-slate-600">AI reasoning:</p>
                        {company.labels.filter(l => l.reasoning).map(l => (
                          <p key={l.label}><span className="font-medium">{l.label}:</span> {l.reasoning}</p>
                        ))}
                      </div>
                    )}

                    <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                      {(labelConfig.length > 0 ? labelConfig : labelNames).map(lc => (
                        <label key={lc.name} className="flex items-start gap-2 cursor-pointer group">
                          <input
                            type="checkbox"
                            className="mt-0.5 rounded border-slate-300 text-violet-600"
                            checked={draft.includes(lc.name)}
                            onChange={() => toggleLabel(lc.name)}
                          />
                          <span className="text-sm">
                            <span className="font-medium text-slate-800">{lc.name}</span>
                            {lc.description && <span className="block text-xs text-slate-400 leading-tight">{lc.description}</span>}
                          </span>
                        </label>
                      ))}
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-600 mb-1">
                        Reason (optional — creates a rule for future runs)
                      </label>
                      <input
                        className="input w-full text-sm"
                        placeholder="e.g. Syneos Health is a CRO not a hospital"
                        value={reason}
                        onChange={e => setReason(e.target.value)}
                      />
                    </div>

                    <div className="flex justify-end gap-2">
                      <button className="btn-secondary text-sm" onClick={() => setEditing(null)}>Cancel</button>
                      <button
                        className="btn-primary text-sm"
                        onClick={() => save(company.company_id)}
                        disabled={saving}
                      >
                        {saving ? 'Saving…' : 'Save labels'}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── DISCUSSIONS TAB ───────────────────────────────────────────────────────────

interface ReviewDiscussion {
  id: number; title: string; category: string | null; current_state: string | null;
  company_id: number | null; company_name: string | null; summary: string | null;
  first_seen: string | null; last_seen: string | null; updated_at: string | null;
}

const GRANULARITY_LABELS: Record<Granularity, string> = {
  fewer:    'Fewer, broader',
  balanced: 'Balanced',
  more:     'More specific',
};
const GRANULARITY_HINTS: Record<Granularity, string> = {
  fewer:    'Merge related topics — prefer one discussion per business relationship',
  balanced: 'Default AI behaviour',
  more:     'Split into distinct discussions per product, contract, or workstream',
};

function DiscussionsTab({ categoryConfig }: { categoryConfig: CategoryConfig[] }) {
  const [items, setItems]           = useState<ReviewDiscussion[]>([]);
  const [total, setTotal]           = useState(0);
  const [loading, setLoading]       = useState(true);
  const [catFilter, setCatFilter]   = useState('');
  const [stateFilter, setStateFilter] = useState('');
  const [granularity, setGranularity] = useState<Granularity>('balanced');
  const [granSaving, setGranSaving] = useState(false);
  const [editingId, setEditingId]   = useState<number | null>(null);
  const [newState, setNewState]     = useState('');
  const [reason, setReason]         = useState('');
  const [saving, setSaving]         = useState(false);

  async function loadDiscussions() {
    setLoading(true);
    try {
      const qs = new URLSearchParams({ limit: '100' });
      if (catFilter)   qs.set('category', catFilter);
      if (stateFilter) qs.set('state', stateFilter);
      const res = await fetch(`/api/discussions?${qs}`);
      const data = await res.json();
      setItems(data.items ?? []);
      setTotal(data.total ?? 0);
    } finally {
      setLoading(false);
    }
  }

  async function loadGranularity() {
    const data = await api.getGranularity();
    setGranularity(data.value);
  }

  useEffect(() => { loadDiscussions(); loadGranularity(); }, []); // eslint-disable-line
  useEffect(() => { loadDiscussions(); }, [catFilter, stateFilter]); // eslint-disable-line

  async function saveGranularity(val: Granularity) {
    setGranSaving(true);
    try { await api.setGranularity(val); setGranularity(val); }
    catch (e) { alert(String(e)); }
    finally { setGranSaving(false); }
  }

  function startEdit(d: ReviewDiscussion) {
    setEditingId(d.id);
    setNewState(d.current_state ?? '');
    setReason('');
  }

  async function saveState(d: ReviewDiscussion) {
    setSaving(true);
    try {
      await api.updateDiscussion(d.id, { state: newState, reason: reason || undefined });
      setItems(items.map(i => i.id === d.id ? { ...i, current_state: newState } : i));
      setEditingId(null);
    } catch (e) {
      alert(String(e));
    } finally {
      setSaving(false);
    }
  }

  const catStates = (cat: string | null) =>
    categoryConfig.find(c => c.name === cat)?.states ?? [];

  const allCategories = [...new Set(items.map(d => d.category).filter(Boolean))].sort();
  const allStates     = [...new Set(items.map(d => d.current_state).filter(Boolean))].sort();

  const stateColor = (s: string | null) => {
    if (!s) return 'bg-slate-100 text-slate-500';
    const lc = s.toLowerCase();
    if (['signed', 'completed', 'hired', 'resolved', 'implemented'].includes(lc)) return 'bg-green-100 text-green-700';
    if (['lost', 'passed', 'stale', 'cancelled', 'abandoned'].includes(lc))        return 'bg-red-100 text-red-700';
    if (['negotiating', 'active', 'in progress', 'evaluating'].includes(lc))       return 'bg-blue-100 text-blue-700';
    return 'bg-slate-100 text-slate-600';
  };

  return (
    <div className="space-y-5">
      {/* Granularity control */}
      <div className="card p-4">
        <p className="text-sm font-semibold text-slate-700 mb-3">Discussion granularity</p>
        <div className="flex flex-col sm:flex-row gap-2">
          {(['fewer', 'balanced', 'more'] as Granularity[]).map(val => (
            <button
              key={val}
              disabled={granSaving}
              onClick={() => saveGranularity(val)}
              className={`flex-1 rounded-lg border px-4 py-3 text-left transition-colors ${
                granularity === val
                  ? 'border-violet-500 bg-violet-50'
                  : 'border-slate-200 hover:border-slate-300 bg-white'
              }`}
            >
              <p className={`text-sm font-medium ${granularity === val ? 'text-violet-700' : 'text-slate-700'}`}>
                {GRANULARITY_LABELS[val]}
              </p>
              <p className="text-xs text-slate-500 mt-0.5">{GRANULARITY_HINTS[val]}</p>
            </button>
          ))}
        </div>
        <p className="text-xs text-slate-400 mt-2">
          Takes effect on the next <code>discover_discussions</code> pipeline run.
        </p>
      </div>

      {/* Filters */}
      <div className="flex gap-3">
        <select className="input flex-1" value={catFilter} onChange={e => setCatFilter(e.target.value)}>
          <option value="">All categories</option>
          {allCategories.map(c => <option key={c!} value={c!}>{c}</option>)}
        </select>
        <select className="input flex-1" value={stateFilter} onChange={e => setStateFilter(e.target.value)}>
          <option value="">All states</option>
          {allStates.map(s => <option key={s!} value={s!}>{s}</option>)}
        </select>
      </div>

      <p className="text-sm text-slate-500">{total} discussions</p>

      {loading ? (
        <p className="text-sm text-slate-400 py-8 text-center">Loading…</p>
      ) : items.length === 0 ? (
        <p className="text-sm text-slate-400 py-8 text-center">No discussions found.</p>
      ) : (
        <div className="space-y-2">
          {items.map(d => (
            <div key={d.id} className="card">
              <div className="p-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <p className="font-medium text-slate-900">{d.title}</p>
                      {d.current_state && (
                        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${stateColor(d.current_state)}`}>
                          {d.current_state}
                        </span>
                      )}
                    </div>
                    <p className="text-xs text-slate-500 mt-0.5">
                      {d.category} · {d.company_name ?? 'unknown company'} · last seen {formatDate(d.last_seen)}
                    </p>
                  </div>
                  <button
                    className="btn-secondary text-xs shrink-0"
                    onClick={() => editingId === d.id ? setEditingId(null) : startEdit(d)}
                  >
                    {editingId === d.id ? 'Cancel' : 'Correct state'}
                  </button>
                </div>

                {/* State editor */}
                {editingId === d.id && (
                  <div className="mt-4 border-t border-slate-100 pt-4 space-y-3">
                    {d.summary && (
                      <p className="text-xs text-slate-500 leading-relaxed">{d.summary}</p>
                    )}
                    <div className="flex flex-col sm:flex-row gap-3">
                      <div className="flex-1">
                        <label className="block text-xs font-medium text-slate-600 mb-1">State</label>
                        <select
                          className="input w-full text-sm"
                          value={newState}
                          onChange={e => setNewState(e.target.value)}
                        >
                          <option value="">— unchanged —</option>
                          {catStates(d.category).map(s => (
                            <option key={s} value={s}>{s}</option>
                          ))}
                        </select>
                      </div>
                      <div className="flex-1">
                        <label className="block text-xs font-medium text-slate-600 mb-1">
                          Reason
                        </label>
                        <input
                          className="input w-full text-sm"
                          placeholder="Why is this wrong?"
                          value={reason}
                          onChange={e => setReason(e.target.value)}
                        />
                      </div>
                    </div>
                    <div className="flex justify-end gap-2">
                      <button className="btn-secondary text-sm" onClick={() => setEditingId(null)}>Cancel</button>
                      <button
                        className="btn-primary text-sm"
                        onClick={() => saveState(d)}
                        disabled={saving || !newState}
                      >
                        {saving ? 'Saving…' : 'Save'}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── RULES TAB ─────────────────────────────────────────────────────────────────

const LAYER_LABELS: Record<string, string> = {
  labels:             'Company Labels',
  events:             'Event Extraction',
  discussions:        'Discussion Discovery',
  discussion_updates: 'Discussion Analysis',
  actions:            'Action Proposals',
};

function RulesTab() {
  const [rules, setRules]       = useState<LearnedRule[]>([]);
  const [loading, setLoading]   = useState(true);
  const [addingFor, setAddingFor] = useState<string | null>(null);
  const [newText, setNewText]   = useState('');
  const [newCat, setNewCat]     = useState('');
  const [saving, setSaving]     = useState(false);
  const textRef = useRef<HTMLTextAreaElement>(null);

  async function load() {
    setLoading(true);
    try { setRules(await api.getRules()); }
    finally { setLoading(false); }
  }

  useEffect(() => { load(); }, []);

  useEffect(() => {
    if (addingFor) textRef.current?.focus();
  }, [addingFor]);

  async function toggleActive(rule: LearnedRule) {
    await api.updateRule(rule.id, { active: !rule.active });
    setRules(rules.map(r => r.id === rule.id ? { ...r, active: !r.active } : r));
  }

  async function deleteRule(id: number) {
    if (!confirm('Delete this rule?')) return;
    await api.deleteRule(id);
    setRules(rules.filter(r => r.id !== id));
  }

  async function addRule(layer: string) {
    if (!newText.trim()) return;
    setSaving(true);
    try {
      const rule = await api.createRule({ layer, category: newCat || undefined, rule_text: newText.trim() });
      setRules([rule, ...rules]);
      setAddingFor(null);
      setNewText('');
      setNewCat('');
    } finally {
      setSaving(false);
    }
  }

  const layers = Object.keys(LAYER_LABELS);
  const byLayer = (layer: string) => rules.filter(r => r.layer === layer);

  if (loading) return <p className="text-sm text-slate-400 py-8 text-center">Loading…</p>;

  return (
    <div className="space-y-6">
      <p className="text-sm text-slate-500">
        Rules are injected into AI prompts on every pipeline run. Edit or disable them here.
        Inactive rules are kept for reference but not used.
      </p>

      {layers.map(layer => {
        const layerRules = byLayer(layer);
        const isAdding = addingFor === layer;
        return (
          <div key={layer} className="card">
            <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between">
              <div>
                <p className="font-medium text-slate-900 text-sm">{LAYER_LABELS[layer] ?? layer}</p>
                <p className="text-xs text-slate-400">layer: <code>{layer}</code></p>
              </div>
              <button
                className="btn-secondary text-xs"
                onClick={() => { setAddingFor(isAdding ? null : layer); setNewText(''); setNewCat(''); }}
              >
                {isAdding ? 'Cancel' : '+ Add rule'}
              </button>
            </div>

            <div className="divide-y divide-slate-50">
              {/* Add rule form */}
              {isAdding && (
                <div className="p-4 bg-slate-50 space-y-3">
                  <textarea
                    ref={textRef}
                    className="input w-full text-sm"
                    rows={3}
                    placeholder="Write a rule that the AI should follow…"
                    value={newText}
                    onChange={e => setNewText(e.target.value)}
                  />
                  <div className="flex gap-3 items-end">
                    <div className="flex-1">
                      <label className="block text-xs text-slate-500 mb-1">Category (optional)</label>
                      <input
                        className="input w-full text-sm"
                        placeholder="e.g. pharma-deal"
                        value={newCat}
                        onChange={e => setNewCat(e.target.value)}
                      />
                    </div>
                    <button
                      className="btn-primary text-sm"
                      onClick={() => addRule(layer)}
                      disabled={saving || !newText.trim()}
                    >
                      {saving ? 'Saving…' : 'Add rule'}
                    </button>
                  </div>
                </div>
              )}

              {layerRules.length === 0 && !isAdding && (
                <p className="px-4 py-3 text-xs text-slate-400 italic">No rules yet.</p>
              )}

              {layerRules.map(rule => (
                <div key={rule.id} className={`px-4 py-3 flex gap-3 items-start ${!rule.active ? 'opacity-50' : ''}`}>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-slate-800 leading-relaxed">{rule.rule_text}</p>
                    <div className="flex items-center gap-2 mt-1">
                      {rule.category && (
                        <span className="text-xs bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded">
                          {rule.category}
                        </span>
                      )}
                      <span className="text-xs text-slate-400">{formatDate(rule.created_at)}</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <button
                      onClick={() => toggleActive(rule)}
                      title={rule.active ? 'Disable rule' : 'Enable rule'}
                      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                        rule.active ? 'bg-violet-500' : 'bg-slate-200'
                      }`}
                    >
                      <span className={`inline-block h-3.5 w-3.5 rounded-full bg-white shadow transition-transform ${
                        rule.active ? 'translate-x-4' : 'translate-x-1'
                      }`} />
                    </button>
                    <button
                      onClick={() => deleteRule(rule.id)}
                      className="text-slate-300 hover:text-red-400 transition-colors text-sm"
                      title="Delete rule"
                    >
                      ✕
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── COMPANIES TAB ─────────────────────────────────────────────────────────────

function formatStage(s: string) {
  return s.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function CompaniesTab() {
  const [items, setItems]       = useState<PipelineCompany[]>([]);
  const [total, setTotal]       = useState(0);
  const [stages, setStages]     = useState<string[]>([]);
  const [q, setQ]               = useState('');
  const [filter, setFilter]     = useState('');
  const [stageFilter, setStageFilter] = useState('');
  const [loading, setLoading]   = useState(true);
  const [page, setPage]         = useState(1);
  const navigate                = useNavigate();
  const LIMIT = 100;

  useEffect(() => {
    setLoading(true);
    api.getReviewCompanies({ q: filter || undefined, stage: stageFilter || undefined, page, limit: LIMIT })
      .then(data => {
        setItems(data.items);
        setTotal(data.total);
        if (data.stages?.length) setStages(data.stages);
      })
      .finally(() => setLoading(false));
  }, [filter, stageFilter, page]);

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    setPage(1);
    setFilter(q);
  }

  const totalPages = Math.max(1, Math.ceil(total / LIMIT));

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <form onSubmit={handleSearch} className="flex gap-2 flex-1">
          <input
            value={q}
            onChange={e => setQ(e.target.value)}
            placeholder="Search companies…"
            className="input flex-1"
          />
          <button type="submit" className="btn-secondary">Search</button>
        </form>
        {stages.length > 0 && (
          <select
            className="input w-52 shrink-0"
            value={stageFilter}
            onChange={e => { setStageFilter(e.target.value); setPage(1); }}
          >
            <option value="">All stages</option>
            {stages.map(s => (
              <option key={s} value={s}>{formatStage(s)}</option>
            ))}
          </select>
        )}
      </div>

      {loading ? (
        <div className="text-sm text-slate-500">Loading…</div>
      ) : items.length === 0 ? (
        <div className="text-sm text-slate-500">No companies found.</div>
      ) : (
        <>
          <div className="text-xs text-slate-400">{total} companies</div>
          <div className="divide-y divide-slate-100 border border-slate-200 rounded-lg overflow-hidden">
            {items.map(c => (
              <div
                key={c.company_id}
                className="flex items-center justify-between px-4 py-3 hover:bg-slate-50 cursor-pointer"
                onClick={() => navigate(`/companies/${c.company_id}`)}
              >
                <div>
                  <div className="font-medium text-slate-900 text-sm">{c.name ?? c.domain ?? '—'}</div>
                  {c.name && c.domain && (
                    <div className="text-xs text-slate-400 mt-0.5">{c.domain}</div>
                  )}
                </div>
                <div className="text-xs text-slate-400 shrink-0 ml-4">{formatDate(c.last_analysed_at)}</div>
              </div>
            ))}
          </div>

          {totalPages > 1 && (
            <div className="flex items-center gap-2 justify-center pt-2">
              <button
                disabled={page <= 1}
                onClick={() => setPage(p => p - 1)}
                className="btn-secondary text-xs disabled:opacity-40"
              >← Prev</button>
              <span className="text-xs text-slate-500">{page} / {totalPages}</span>
              <button
                disabled={page >= totalPages}
                onClick={() => setPage(p => p + 1)}
                className="btn-secondary text-xs disabled:opacity-40"
              >Next →</button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── PAGE SHELL ────────────────────────────────────────────────────────────────

type Tab = 'labels' | 'discussions' | 'rules' | 'companies';

export default function Review() {
  const [tab, setTab]               = useState<Tab>('labels');
  const [labelConfig, setLabelConfig] = useState<LabelConfig[]>([]);
  const [categoryConfig, setCategoryConfig] = useState<CategoryConfig[]>([]);

  useEffect(() => {
    fetch('/api/meta')
      .then(r => r.json())
      .then(data => {
        setLabelConfig(data.labelConfig ?? []);
        setCategoryConfig(data.categoryConfig ?? []);
      });
  }, []);

  const tabs: { id: Tab; label: string }[] = [
    { id: 'labels',      label: 'Labels' },
    { id: 'discussions', label: 'Discussions' },
    { id: 'rules',       label: 'Rules library' },
    { id: 'companies',   label: 'Companies' },
  ];

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">Review</h1>
        <p className="text-sm text-slate-500 mt-1">
          Correct AI decisions and build a library of rules that improve future runs.
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-slate-200">
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors -mb-px ${
              tab === t.id
                ? 'border-violet-500 text-violet-700'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div>
        {tab === 'labels'      && <LabelsTab      labelConfig={labelConfig} />}
        {tab === 'discussions' && <DiscussionsTab  categoryConfig={categoryConfig} />}
        {tab === 'rules'       && <RulesTab />}
        {tab === 'companies'   && <CompaniesTab />}
      </div>
    </div>
  );
}
