import { useEffect, useState, useCallback } from 'react';
import { useParams, useNavigate, useLocation, Link } from 'react-router-dom';
import { api } from '../api';
import type { Discussion, DiscussionDetail, DiscussionAction, StateHistoryEntry, Thread, ThreadEmail, CalendarEvent, EventLedgerEntry, Milestone, ProposedAction, HubSpotNote } from '../types';
import Badge from '../components/Badge';
import Breadcrumbs, { extendBreadcrumbs } from '../components/Breadcrumbs';
import Markdown from '../components/Markdown';
import ThreadModal from '../components/ThreadModal';
import { formatDate, formatDateTime } from '../utils';

// Modal wrapper — backdrop + centered card + esc-to-close.
function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: React.ReactNode }) {
  useEffect(() => {
    const h = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', h);
    return () => document.removeEventListener('keydown', h);
  }, [onClose]);
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg max-h-[85vh] flex flex-col">
        <div className="flex items-center justify-between px-5 py-3 border-b border-slate-200">
          <h2 className="text-base font-semibold text-slate-900">{title}</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-600 text-xl leading-none">&times;</button>
        </div>
        <div className="flex-1 overflow-y-auto p-5">{children}</div>
      </div>
    </div>
  );
}

// Inline editor for a single text field. Displays `value` in a read-only element
// (rendered by children); clicking the pencil button swaps to an input/textarea
// bound to local state, Save calls onSave(newValue), Cancel reverts.
function InlineEdit({
  value, multiline, onSave, children, placeholder, editLabel = 'Edit',
}: {
  value: string;
  multiline?: boolean;
  onSave: (next: string) => Promise<void> | void;
  children: React.ReactNode;
  placeholder?: string;
  editLabel?: string;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => { if (!editing) setDraft(value); }, [value, editing]);

  async function handleSave() {
    if (draft === value) { setEditing(false); return; }
    setSaving(true); setError(null);
    try {
      await onSave(draft);
      setEditing(false);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }

  if (!editing) {
    return (
      <div className="group relative">
        {children}
        <button
          onClick={() => setEditing(true)}
          className="ml-2 text-xs text-slate-400 hover:text-slate-700 opacity-0 group-hover:opacity-100 transition-opacity"
          title={editLabel}
        >
          ✎
        </button>
      </div>
    );
  }

  const Field = multiline ? 'textarea' : 'input';
  return (
    <div>
      <Field
        value={draft}
        onChange={(e: any) => setDraft(e.target.value)}
        placeholder={placeholder}
        rows={multiline ? 4 : undefined}
        className="w-full px-3 py-2 text-base border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        autoFocus
      />
      <div className="flex items-center gap-2 mt-2">
        <button
          onClick={handleSave}
          disabled={saving}
          className="btn-primary text-sm px-3 py-1"
        >
          {saving ? 'Saving…' : 'Save'}
        </button>
        <button
          onClick={() => { setEditing(false); setError(null); }}
          disabled={saving}
          className="btn-secondary text-sm px-3 py-1"
        >
          Cancel
        </button>
        {error && <span className="text-xs text-red-600">{error}</span>}
      </div>
    </div>
  );
}

// Async type-to-search picker over discussions. Biased toward same-company
// by default; the companyId filter can be relaxed by clearing the checkbox.
function DiscussionPicker({
  excludeId, companyId, onSelect,
}: {
  excludeId: number;
  companyId: number | null;
  onSelect: (d: Discussion) => void;
}) {
  const [query, setQuery] = useState('');
  const [sameCompany, setSameCompany] = useState(companyId !== null);
  const [results, setResults] = useState<Discussion[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api.getDiscussions({
      q: query || undefined,
      company_id: sameCompany && companyId ? companyId : undefined,
      limit: 20,
    })
      .then((d) => {
        if (!cancelled) {
          setResults(d.items.filter((x) => x.id !== excludeId));
        }
      })
      .catch(() => { if (!cancelled) setResults([]); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [query, sameCompany, companyId, excludeId]);

  return (
    <div>
      <div className="flex items-center gap-3 mb-3">
        <input
          type="text"
          autoFocus
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search by title…"
          className="flex-1 px-3 py-2 text-sm border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        {companyId !== null && (
          <label className="flex items-center gap-1.5 text-xs text-slate-600 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={sameCompany}
              onChange={(e) => setSameCompany(e.target.checked)}
              className="rounded border-slate-300"
            />
            Same company only
          </label>
        )}
      </div>
      <div className="max-h-80 overflow-y-auto border border-slate-200 rounded-lg">
        {loading ? (
          <div className="p-4 text-sm text-slate-400">Loading…</div>
        ) : results.length === 0 ? (
          <div className="p-4 text-sm text-slate-400">No matching discussions.</div>
        ) : (
          <ul className="divide-y divide-slate-100">
            {results.map((d) => (
              <li key={d.id}>
                <button
                  onClick={() => onSelect(d)}
                  className="w-full text-left px-3 py-2 hover:bg-slate-50 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-slate-900 truncate">{d.title}</span>
                    {d.category && <Badge label={d.category} variant="category" />}
                    {d.current_state && <Badge label={d.current_state} variant="state" />}
                  </div>
                  {d.summary && (
                    <p className="text-xs text-slate-500 mt-0.5 line-clamp-1">{d.summary}</p>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function StateTimeline({ history }: { history: StateHistoryEntry[] }) {
  if (history.length === 0) return null;

  return (
    <div className="relative">
      <div className="absolute left-3.5 top-0 bottom-0 w-px bg-slate-200" />
      <div className="space-y-4">
        {history.map((entry, i) => (
          <div key={entry.id} className="relative flex gap-4 pl-9">
            {/* Dot */}
            <div
              className={`absolute left-0 w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 border-2 ${
                i === history.length - 1
                  ? 'bg-blue-600 border-blue-600'
                  : 'bg-white border-slate-300'
              }`}
            >
              <div
                className={`w-2 h-2 rounded-full ${
                  i === history.length - 1 ? 'bg-white' : 'bg-slate-400'
                }`}
              />
            </div>

            <div className="flex-1 pb-4">
              <div className="flex items-center gap-3 mb-1">
                <Badge label={entry.state} variant="state" />
                {entry.entered_at && (
                  <span className="text-xs text-slate-400">{formatDateTime(entry.entered_at)}</span>
                )}
              </div>
              {entry.reasoning && (
                <p className="text-sm text-slate-600 leading-relaxed">{entry.reasoning}</p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ActionRow({ action, linkState }: { action: DiscussionAction; linkState?: object }) {
  return (
    <div className="py-3 border-b border-slate-100 last:border-0">
      <div className="flex items-start gap-3">
        <div className={`mt-0.5 w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 ${
          action.status === 'done'
            ? 'bg-green-100 text-green-600'
            : 'bg-amber-100 text-amber-600'
        }`}>
          {action.status === 'done' ? (
            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none"
              stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="20 6 9 17 4 12" />
            </svg>
          ) : (
            <div className="w-2 h-2 rounded-full bg-amber-500" />
          )}
        </div>
        <div className="flex-1 min-w-0">
          <p className={`text-sm leading-relaxed ${action.status === 'done' ? 'text-slate-500 line-through' : 'text-slate-900'}`}>
            {action.description}
          </p>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 mt-1 text-xs text-slate-400">
            {action.assignee_emails.length > 0 && (
              <span className="flex flex-wrap gap-x-2">
                {action.assignee_emails.map((email) => (
                  <Link
                    key={email}
                    to={`/contacts/${encodeURIComponent(email)}`}
                    state={linkState}
                    className="text-blue-600 hover:underline"
                  >
                    {email}
                  </Link>
                ))}
              </span>
            )}
            {action.target_date && (
              <span>Due {formatDate(action.target_date)}</span>
            )}
            {action.completed_date && (
              <span className="text-green-600">Completed {formatDate(action.completed_date)}</span>
            )}
            {action.source_date && (
              <span>From {formatDate(action.source_date)}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function MilestoneTracker({
  milestones: initialMilestones,
  discussionId,
}: {
  milestones: Milestone[];
  discussionId: number;
}) {
  const [milestones, setMilestones] = useState<Milestone[]>(initialMilestones);
  const [addingNew, setAddingNew] = useState(false);
  const [newName, setNewName] = useState('');
  const [newAchieved, setNewAchieved] = useState(false);
  const [saving, setSaving] = useState(false);
  const [editingDateId, setEditingDateId] = useState<number | null>(null);
  const [editingNameId, setEditingNameId] = useState<number | null>(null);
  const [nameDraft, setNameDraft] = useState('');
  const [dateDraft, setDateDraft] = useState('');

  // Sync with parent when props change (e.g. full refresh)
  useEffect(() => { setMilestones(initialMilestones); }, [initialMilestones]);

  const achieved = milestones.filter((m) => m.achieved);

  async function toggleAchieved(m: Milestone) {
    const next = !m.achieved;
    setMilestones((prev) => prev.map((x) => x.id === m.id ? { ...x, achieved: next } : x));
    try {
      await api.updateMilestone(m.id, { achieved: next });
    } catch (e) {
      setMilestones((prev) => prev.map((x) => x.id === m.id ? { ...x, achieved: m.achieved } : x));
      alert(`Update failed: ${(e as Error).message}`);
    }
  }

  async function saveName(m: Milestone) {
    if (!nameDraft.trim() || nameDraft === m.name) { setEditingNameId(null); return; }
    try {
      await api.updateMilestone(m.id, { name: nameDraft.trim() });
      setMilestones((prev) => prev.map((x) => x.id === m.id ? { ...x, name: nameDraft.trim() } : x));
    } catch (e) {
      alert(`Update failed: ${(e as Error).message}`);
    }
    setEditingNameId(null);
  }

  async function saveDate(m: Milestone) {
    const val = dateDraft.trim() || null;
    try {
      await api.updateMilestone(m.id, { achieved_date: val });
      setMilestones((prev) => prev.map((x) => x.id === m.id ? { ...x, achieved_date: val } : x));
    } catch (e) {
      alert(`Update failed: ${(e as Error).message}`);
    }
    setEditingDateId(null);
  }

  async function remove(m: Milestone) {
    if (!confirm(`Delete milestone "${m.name}"?`)) return;
    try {
      await api.deleteMilestone(m.id);
      setMilestones((prev) => prev.filter((x) => x.id !== m.id));
    } catch (e) {
      alert(`Delete failed: ${(e as Error).message}`);
    }
  }

  async function addMilestone() {
    if (!newName.trim()) return;
    setSaving(true);
    try {
      const created = await api.addMilestone(discussionId, { name: newName.trim(), achieved: newAchieved });
      setMilestones((prev) => [...prev, created]);
      setNewName('');
      setNewAchieved(false);
      setAddingNew(false);
    } catch (e) {
      alert(`Add failed: ${(e as Error).message}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div>
      {milestones.length > 0 && (
        <>
          <div className="flex items-center gap-3 mb-4">
            <div className="flex items-center gap-1.5 text-sm text-slate-500">
              <span className="font-semibold text-emerald-600">{achieved.length}</span>
              <span>of</span>
              <span className="font-semibold">{milestones.length}</span>
              <span>milestones</span>
            </div>
            <div className="flex-1 h-2 bg-slate-100 rounded-full overflow-hidden">
              <div
                className="h-full bg-emerald-500 rounded-full transition-all duration-500"
                style={{ width: milestones.length > 0 ? `${(achieved.length / milestones.length) * 100}%` : '0%' }}
              />
            </div>
          </div>

          <div className="space-y-2 mb-3">
            {milestones.map((m) => (
              <div key={m.id} className={`flex items-center gap-3 py-1.5 group ${m.achieved ? '' : 'opacity-60'}`}>
                {/* Achieved toggle */}
                <button
                  onClick={() => toggleAchieved(m)}
                  className={`w-6 h-6 rounded-full flex items-center justify-center flex-shrink-0 border-2 transition-colors ${
                    m.achieved
                      ? 'bg-emerald-100 border-emerald-400 text-emerald-600'
                      : 'border-slate-200 hover:border-emerald-300'
                  }`}
                  title={m.achieved ? 'Mark not achieved' : 'Mark achieved'}
                >
                  {m.achieved && (
                    <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none"
                      stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                      <polyline points="20 6 9 17 4 12" />
                    </svg>
                  )}
                </button>

                {/* Name */}
                <div className="flex-1 min-w-0">
                  {editingNameId === m.id ? (
                    <div className="flex items-center gap-2">
                      <input
                        autoFocus
                        value={nameDraft}
                        onChange={(e) => setNameDraft(e.target.value)}
                        onKeyDown={(e) => { if (e.key === 'Enter') saveName(m); if (e.key === 'Escape') setEditingNameId(null); }}
                        className="px-2 py-0.5 text-sm border border-slate-300 rounded flex-1"
                      />
                      <button onClick={() => saveName(m)} className="text-xs text-blue-600 hover:text-blue-800">Save</button>
                      <button onClick={() => setEditingNameId(null)} className="text-xs text-slate-400 hover:text-slate-600">Cancel</button>
                    </div>
                  ) : (
                    <div className="flex items-center gap-1.5">
                      <span className="text-sm font-medium text-slate-900">
                        {m.name.replace(/_/g, ' ')}
                      </span>
                      {m.source === 'human' ? (
                        <span className="text-[10px] font-medium text-purple-600 bg-purple-50 px-1 py-0.5 rounded">human</span>
                      ) : (
                        <span className="text-[10px] text-slate-300">ai</span>
                      )}
                      <button
                        onClick={() => { setEditingNameId(m.id); setNameDraft(m.name); }}
                        className="text-xs text-slate-400 hover:text-slate-700 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="Edit name"
                      >
                        ✎
                      </button>
                    </div>
                  )}
                </div>

                {/* Date */}
                {editingDateId === m.id ? (
                  <div className="flex items-center gap-1">
                    <input
                      type="date"
                      autoFocus
                      value={dateDraft}
                      onChange={(e) => setDateDraft(e.target.value)}
                      onKeyDown={(e) => { if (e.key === 'Enter') saveDate(m); if (e.key === 'Escape') setEditingDateId(null); }}
                      className="px-2 py-0.5 text-xs border border-slate-300 rounded"
                    />
                    <button onClick={() => saveDate(m)} className="text-xs text-blue-600">Save</button>
                    <button onClick={() => setEditingDateId(null)} className="text-xs text-slate-400">✕</button>
                  </div>
                ) : (
                  <div className="flex items-center gap-1 flex-shrink-0">
                    {m.achieved_date ? (
                      <span className="text-xs text-slate-400">{formatDate(m.achieved_date)}</span>
                    ) : null}
                    <button
                      onClick={() => { setEditingDateId(m.id); setDateDraft(m.achieved_date?.slice(0, 10) ?? ''); }}
                      className="text-xs text-slate-400 hover:text-slate-700 opacity-0 group-hover:opacity-100 transition-opacity"
                      title="Edit date"
                    >
                      ✎
                    </button>
                  </div>
                )}

                {m.confidence != null && m.confidence > 0 && (
                  <span className="text-xs text-slate-400 flex-shrink-0">
                    {Math.round(m.confidence * 100)}%
                  </span>
                )}

                {/* Delete */}
                <button
                  onClick={() => remove(m)}
                  className="text-xs text-slate-400 hover:text-red-600 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0"
                  title="Delete milestone"
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Add milestone */}
      {addingNew ? (
        <div className="border border-slate-200 rounded-lg p-3 bg-slate-50 space-y-2 mt-2">
          <input
            autoFocus
            type="text"
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') addMilestone(); if (e.key === 'Escape') setAddingNew(false); }}
            placeholder="Milestone name…"
            className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
          />
          <label className="flex items-center gap-2 text-sm text-slate-600">
            <input
              type="checkbox"
              checked={newAchieved}
              onChange={(e) => setNewAchieved(e.target.checked)}
              className="rounded border-slate-300"
            />
            Already achieved
          </label>
          <div className="flex gap-2">
            <button onClick={addMilestone} disabled={saving || !newName.trim()} className="btn-primary text-xs px-3 py-1">
              {saving ? 'Saving…' : 'Save'}
            </button>
            <button onClick={() => { setAddingNew(false); setNewName(''); }} disabled={saving} className="btn-secondary text-xs px-3 py-1">
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <button
          onClick={() => setAddingNew(true)}
          className="text-xs text-blue-600 hover:text-blue-800 mt-1"
        >
          + Add milestone
        </button>
      )}
    </div>
  );
}

const PRIORITY_STYLES: Record<string, { bg: string; icon: string; label: string }> = {
  high: { bg: 'bg-red-50 border-red-200', icon: 'text-red-500', label: 'High' },
  medium: { bg: 'bg-amber-50 border-amber-200', icon: 'text-amber-500', label: 'Medium' },
  low: { bg: 'bg-slate-50 border-slate-200', icon: 'text-slate-400', label: 'Low' },
};

const PRIORITY_CYCLE: Record<string, string> = { low: 'medium', medium: 'high', high: 'low' };

function ProposedActionsList({
  actions: initialActions,
  discussionId,
}: {
  actions: ProposedAction[];
  discussionId: number;
}) {
  const [actions, setActions] = useState<ProposedAction[]>(initialActions);
  const [addingNew, setAddingNew] = useState(false);
  const [newAction, setNewAction] = useState('');
  const [newPriority, setNewPriority] = useState('medium');
  const [newWaitUntil, setNewWaitUntil] = useState('');
  const [saving, setSaving] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editDraft, setEditDraft] = useState<Partial<ProposedAction>>({});
  const [editingDateId, setEditingDateId] = useState<number | null>(null);
  const [dateDraft, setDateDraft] = useState('');

  useEffect(() => { setActions(initialActions); }, [initialActions]);

  async function toggleStatus(pa: ProposedAction) {
    const next = pa.status === 'done' ? 'open' : 'done';
    setActions((prev) => prev.map((x) => x.id === pa.id ? { ...x, status: next } : x));
    try {
      await api.updateProposedAction(pa.id, { status: next });
    } catch (e) {
      setActions((prev) => prev.map((x) => x.id === pa.id ? { ...x, status: pa.status } : x));
      alert(`Update failed: ${(e as Error).message}`);
    }
  }

  async function cyclePriority(pa: ProposedAction) {
    const next = PRIORITY_CYCLE[pa.priority] ?? 'medium';
    setActions((prev) => prev.map((x) => x.id === pa.id ? { ...x, priority: next } : x));
    try {
      await api.updateProposedAction(pa.id, { priority: next });
    } catch (e) {
      setActions((prev) => prev.map((x) => x.id === pa.id ? { ...x, priority: pa.priority } : x));
      alert(`Update failed: ${(e as Error).message}`);
    }
  }

  async function saveEdit(pa: ProposedAction) {
    if (!editDraft.action?.trim()) { setEditingId(null); return; }
    try {
      await api.updateProposedAction(pa.id, { action: editDraft.action });
      setActions((prev) => prev.map((x) => x.id === pa.id ? { ...x, action: editDraft.action! } : x));
    } catch (e) {
      alert(`Update failed: ${(e as Error).message}`);
    }
    setEditingId(null);
  }

  async function saveDate(pa: ProposedAction) {
    const val = dateDraft.trim() || null;
    try {
      await api.updateProposedAction(pa.id, { wait_until: val });
      setActions((prev) => prev.map((x) => x.id === pa.id ? { ...x, wait_until: val } : x));
    } catch (e) {
      alert(`Update failed: ${(e as Error).message}`);
    }
    setEditingDateId(null);
  }

  async function remove(pa: ProposedAction) {
    if (!confirm(`Delete action "${pa.action}"?`)) return;
    try {
      await api.deleteProposedAction(pa.id);
      setActions((prev) => prev.filter((x) => x.id !== pa.id));
    } catch (e) {
      alert(`Delete failed: ${(e as Error).message}`);
    }
  }

  async function addAction() {
    if (!newAction.trim()) return;
    setSaving(true);
    try {
      const created = await api.addProposedAction(discussionId, {
        action: newAction.trim(),
        priority: newPriority,
        wait_until: newWaitUntil.trim() || null,
      });
      setActions((prev) => [...prev, created]);
      setNewAction('');
      setNewPriority('medium');
      setNewWaitUntil('');
      setAddingNew(false);
    } catch (e) {
      alert(`Add failed: ${(e as Error).message}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div>
      <div className="space-y-3 mb-3">
        {actions.map((pa) => {
          const style = PRIORITY_STYLES[pa.priority] ?? PRIORITY_STYLES.medium;
          const isDone = pa.status === 'done';

          return (
            <div key={pa.id} className={`rounded-lg border p-4 ${style.bg} group ${isDone ? 'opacity-60' : ''}`}>
              <div className="flex items-start gap-3">
                {/* Status toggle */}
                <button
                  onClick={() => toggleStatus(pa)}
                  className={`mt-0.5 w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 border-2 transition-colors ${
                    isDone
                      ? 'bg-emerald-100 border-emerald-400 text-emerald-600'
                      : 'border-slate-300 hover:border-emerald-300'
                  }`}
                  title={isDone ? 'Mark open' : 'Mark done'}
                >
                  {isDone && (
                    <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none"
                      stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                      <polyline points="20 6 9 17 4 12" />
                    </svg>
                  )}
                </button>

                <div className="flex-1 min-w-0">
                  {/* Action text */}
                  {editingId === pa.id ? (
                    <div className="space-y-1">
                      <textarea
                        autoFocus
                        value={editDraft.action ?? ''}
                        onChange={(e) => setEditDraft({ ...editDraft, action: e.target.value })}
                        rows={2}
                        className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
                      />
                      <div className="flex gap-2">
                        <button onClick={() => saveEdit(pa)} className="btn-primary text-xs px-2 py-0.5">Save</button>
                        <button onClick={() => setEditingId(null)} className="btn-secondary text-xs px-2 py-0.5">Cancel</button>
                      </div>
                    </div>
                  ) : (
                    <div className="flex items-start gap-1.5 group/text">
                      <p className={`text-sm font-medium leading-relaxed flex-1 ${isDone ? 'line-through text-slate-500' : 'text-slate-900'}`}>
                        {pa.action}
                      </p>
                      <button
                        onClick={() => { setEditingId(pa.id); setEditDraft({ action: pa.action }); }}
                        className="text-xs text-slate-400 hover:text-slate-700 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0"
                        title="Edit action"
                      >
                        ✎
                      </button>
                    </div>
                  )}

                  {pa.reasoning && (
                    <p className="text-xs text-slate-500 mt-1 leading-relaxed">{pa.reasoning}</p>
                  )}

                  <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 mt-2 text-xs text-slate-400">
                    {/* Priority badge — clickable to cycle */}
                    <button
                      onClick={() => cyclePriority(pa)}
                      className={`font-medium ${style.icon} hover:opacity-70 transition-opacity`}
                      title="Click to change priority"
                    >
                      {style.label} priority
                    </button>

                    {/* Wait until — editable */}
                    {editingDateId === pa.id ? (
                      <div className="flex items-center gap-1">
                        <input
                          type="date"
                          autoFocus
                          value={dateDraft}
                          onChange={(e) => setDateDraft(e.target.value)}
                          onKeyDown={(e) => { if (e.key === 'Enter') saveDate(pa); if (e.key === 'Escape') setEditingDateId(null); }}
                          className="px-1 py-0.5 text-xs border border-slate-300 rounded"
                        />
                        <button onClick={() => saveDate(pa)} className="text-blue-600">Save</button>
                        <button onClick={() => setEditingDateId(null)} className="text-slate-400">✕</button>
                      </div>
                    ) : (
                      <span className="flex items-center gap-1">
                        {pa.wait_until && <span>Wait until {formatDate(pa.wait_until)}</span>}
                        <button
                          onClick={() => { setEditingDateId(pa.id); setDateDraft(pa.wait_until?.slice(0, 10) ?? ''); }}
                          className="text-slate-400 hover:text-slate-600 opacity-0 group-hover:opacity-100 transition-opacity"
                          title="Edit wait until"
                        >
                          ✎
                        </button>
                      </span>
                    )}

                    {pa.assignee && (
                      <Link to={`/contacts/${encodeURIComponent(pa.assignee)}`} className="text-blue-600 hover:underline">
                        {pa.assignee}
                      </Link>
                    )}

                    {pa.source === 'human' && (
                      <span className="text-[10px] font-medium text-purple-600 bg-purple-50 px-1 py-0.5 rounded">human</span>
                    )}
                  </div>
                </div>

                {/* Delete */}
                <button
                  onClick={() => remove(pa)}
                  className="text-xs text-slate-400 hover:text-red-600 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0 mt-0.5"
                  title="Delete action"
                >
                  ✕
                </button>
              </div>
            </div>
          );
        })}
      </div>

      {/* Add action */}
      {addingNew ? (
        <div className="border border-slate-200 rounded-lg p-3 bg-slate-50 space-y-2 mt-2">
          <textarea
            autoFocus
            value={newAction}
            onChange={(e) => setNewAction(e.target.value)}
            placeholder="Describe the action…"
            rows={2}
            className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
          />
          <div className="grid grid-cols-2 gap-2">
            <label className="block">
              <span className="text-xs text-slate-500">Priority</span>
              <select
                value={newPriority}
                onChange={(e) => setNewPriority(e.target.value)}
                className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
              >
                <option value="high">High</option>
                <option value="medium">Medium</option>
                <option value="low">Low</option>
              </select>
            </label>
            <label className="block">
              <span className="text-xs text-slate-500">Wait until (optional)</span>
              <input
                type="date"
                value={newWaitUntil}
                onChange={(e) => setNewWaitUntil(e.target.value)}
                className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
              />
            </label>
          </div>
          <div className="flex gap-2">
            <button onClick={addAction} disabled={saving || !newAction.trim()} className="btn-primary text-xs px-3 py-1">
              {saving ? 'Saving…' : 'Save'}
            </button>
            <button onClick={() => { setAddingNew(false); setNewAction(''); }} disabled={saving} className="btn-secondary text-xs px-3 py-1">
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <button
          onClick={() => setAddingNew(true)}
          className="text-xs text-blue-600 hover:text-blue-800 mt-1"
        >
          + Add action
        </button>
      )}
    </div>
  );
}

const DOMAIN_COLORS: Record<string, string> = {
  investment: 'bg-blue-100 text-blue-700',
  'investor-relations': 'bg-indigo-100 text-indigo-700',
  'pharma-deal': 'bg-purple-100 text-purple-700',
  scheduling: 'bg-sky-100 text-sky-700',
  'contract-negotiation': 'bg-amber-100 text-amber-700',
  partnership: 'bg-teal-100 text-teal-700',
  hiring: 'bg-pink-100 text-pink-700',
  'internal-decision': 'bg-slate-100 text-slate-700',
  'board-discussion': 'bg-orange-100 text-orange-700',
  'vendor-selection': 'bg-lime-100 text-lime-700',
  'support-issue': 'bg-red-100 text-red-700',
  newsletter: 'bg-gray-100 text-gray-600',
  other: 'bg-gray-100 text-gray-600',
};

function EventRow({
  ev, onThreadClick, onRefresh,
}: {
  ev: EventLedgerEntry;
  onThreadClick: (threadId: string, sourceEmailId?: string | null) => void;
  onRefresh: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [draft, setDraft] = useState({
    type: ev.type,
    actor: ev.actor ?? '',
    target: ev.target ?? '',
    event_date: ev.event_date ?? '',
    detail: ev.detail ?? '',
  });

  async function save() {
    setSaving(true);
    try {
      await api.updateEvent(ev.id, {
        type: draft.type,
        actor: draft.actor || null,
        target: draft.target || null,
        event_date: draft.event_date || null,
        detail: draft.detail || null,
      });
      setEditing(false);
      onRefresh();
    } catch (e) {
      alert(`Save failed: ${(e as Error).message}`);
    } finally {
      setSaving(false);
    }
  }

  async function remove() {
    if (!confirm(`Delete event "${ev.type}: ${ev.detail ?? ''}"?`)) return;
    try {
      await api.deleteEvent(ev.id);
      onRefresh();
    } catch (e) {
      alert(`Delete failed: ${(e as Error).message}`);
    }
  }

  if (editing) {
    return (
      <div className="border border-slate-200 rounded-lg p-3 bg-slate-50 space-y-2 text-sm">
        <div className="grid grid-cols-2 gap-2">
          <label className="block">
            <span className="text-xs text-slate-500">Type</span>
            <input
              value={draft.type}
              onChange={(e) => setDraft({ ...draft, type: e.target.value })}
              className="w-full px-2 py-1 border border-slate-300 rounded"
            />
          </label>
          <label className="block">
            <span className="text-xs text-slate-500">Date</span>
            <input
              type="date"
              value={draft.event_date?.slice(0, 10) ?? ''}
              onChange={(e) => setDraft({ ...draft, event_date: e.target.value })}
              className="w-full px-2 py-1 border border-slate-300 rounded"
            />
          </label>
          <label className="block">
            <span className="text-xs text-slate-500">Actor</span>
            <input
              value={draft.actor}
              onChange={(e) => setDraft({ ...draft, actor: e.target.value })}
              className="w-full px-2 py-1 border border-slate-300 rounded"
            />
          </label>
          <label className="block">
            <span className="text-xs text-slate-500">Target</span>
            <input
              value={draft.target}
              onChange={(e) => setDraft({ ...draft, target: e.target.value })}
              className="w-full px-2 py-1 border border-slate-300 rounded"
            />
          </label>
        </div>
        <label className="block">
          <span className="text-xs text-slate-500">Detail</span>
          <textarea
            value={draft.detail}
            onChange={(e) => setDraft({ ...draft, detail: e.target.value })}
            rows={2}
            className="w-full px-2 py-1 border border-slate-300 rounded"
          />
        </label>
        <div className="flex gap-2">
          <button onClick={save} disabled={saving} className="btn-primary text-xs px-3 py-1">
            {saving ? 'Saving…' : 'Save'}
          </button>
          <button onClick={() => setEditing(false)} disabled={saving} className="btn-secondary text-xs px-3 py-1">
            Cancel
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex items-start gap-2 group">
      <span className={`inline-flex px-1.5 py-0.5 rounded text-xs font-medium flex-shrink-0 ${DOMAIN_COLORS[ev.domain] ?? DOMAIN_COLORS.other}`}>
        {ev.type.replace(/_/g, ' ')}
      </span>
      <span className="text-sm text-slate-600 leading-relaxed flex-1">
        {ev.detail || `${ev.actor ?? ''} ${ev.target ? `→ ${ev.target}` : ''}`}
      </span>
      {ev.confidence != null && ev.confidence < 0.7 && (
        <span className="text-xs text-amber-500 flex-shrink-0">
          {Math.round(ev.confidence * 100)}%
        </span>
      )}
      {ev.thread_id && (
        <button
          onClick={() => onThreadClick(ev.thread_id!, ev.source_email_id)}
          className="text-xs text-blue-500 hover:text-blue-700 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
          title="View source email"
        >
          view email
        </button>
      )}
      <button
        onClick={() => setEditing(true)}
        className="text-xs text-slate-400 hover:text-slate-700 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
        title="Edit event"
      >
        ✎
      </button>
      <button
        onClick={remove}
        className="text-xs text-slate-400 hover:text-red-600 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
        title="Delete event"
      >
        ✕
      </button>
    </div>
  );
}

function EventTimeline({
  events, onThreadClick, onRefresh,
}: {
  events: EventLedgerEntry[];
  onThreadClick: (threadId: string, sourceEmailId?: string | null) => void;
  onRefresh: () => void;
}) {
  if (events.length === 0) return null;

  // Group events by date
  const byDate: Record<string, EventLedgerEntry[]> = {};
  for (const ev of events) {
    const date = ev.event_date ?? 'Unknown';
    if (!byDate[date]) byDate[date] = [];
    byDate[date].push(ev);
  }

  return (
    <div className="relative">
      <div className="absolute left-3.5 top-0 bottom-0 w-px bg-slate-200" />
      <div className="space-y-4">
        {Object.entries(byDate).map(([date, dateEvents]) => (
          <div key={date} className="relative pl-9">
            <div className="absolute left-0 w-7 h-7 rounded-full bg-white border-2 border-slate-300 flex items-center justify-center flex-shrink-0">
              <div className="w-2 h-2 rounded-full bg-slate-400" />
            </div>
            <div className="pb-2">
              <div className="text-xs font-medium text-slate-500 mb-2">{formatDate(date)}</div>
              <div className="space-y-1.5">
                {dateEvents.map((ev) => (
                  <EventRow key={ev.id} ev={ev} onThreadClick={onThreadClick} onRefresh={onRefresh} />
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ThreadRow({
  thread, onClick, onMove, onRemove,
}: {
  thread: Thread;
  onClick: () => void;
  onMove?: () => void;
  onRemove?: () => void;
}) {
  return (
    <div
      className="py-3 border-b border-slate-100 last:border-0 cursor-pointer hover:bg-slate-50 -mx-2 px-2 rounded transition-colors group"
      onClick={onClick}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="font-medium text-slate-900 truncate">
            {thread.subject ?? '(no subject)'}
          </div>
          {thread.summary && (
            <p className="text-sm text-slate-500 mt-0.5 line-clamp-2">{thread.summary}</p>
          )}
          <div className="flex items-center gap-3 mt-1 text-xs text-slate-400">
            {thread.first_date && <span>{formatDate(thread.first_date)}</span>}
            {thread.last_date && thread.last_date !== thread.first_date && (
              <span>– {formatDate(thread.last_date)}</span>
            )}
            {thread.participants.length > 0 && (
              <span>{thread.participants.length} participants</span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {onMove && (
            <button
              onClick={(e) => { e.stopPropagation(); onMove(); }}
              className="text-xs text-slate-500 hover:text-blue-600 opacity-0 group-hover:opacity-100 transition-opacity"
              title="Move to another discussion"
            >
              Move
            </button>
          )}
          {onRemove && (
            <button
              onClick={(e) => { e.stopPropagation(); onRemove(); }}
              className="text-xs text-slate-500 hover:text-red-600 opacity-0 group-hover:opacity-100 transition-opacity"
              title="Remove from this discussion"
            >
              Remove
            </button>
          )}
          <span className="text-sm font-medium text-slate-600 bg-slate-100 px-2 py-0.5 rounded">
            {thread.email_count} email{thread.email_count !== 1 ? 's' : ''}
          </span>
        </div>
      </div>
    </div>
  );
}

export default function DiscussionDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const [data, setData] = useState<DiscussionDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDoneActions, setShowDoneActions] = useState(false);
  const [selectedThread, setSelectedThread] = useState<Thread | null>(null);
  const [highlightMessageId, setHighlightMessageId] = useState<string | null>(null);
  const [mergeOpen, setMergeOpen] = useState(false);
  const [movingThread, setMovingThread] = useState<Thread | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    setError(null);
    api
      .getDiscussion(parseInt(id, 10))
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="p-4 sm:p-8">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-slate-200 rounded w-24" />
          <div className="h-8 bg-slate-200 rounded w-2/3" />
          <div className="h-4 bg-slate-200 rounded w-48" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="p-4 sm:p-8">
        <button onClick={() => navigate('/discussions')} className="btn-secondary mb-6">
          ← Back
        </button>
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">{error ?? 'Discussion not found'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-8 max-w-4xl">
      <Breadcrumbs
        current={data.title}
        defaultTrail={[{ label: 'Discussions', path: '/discussions' }]}
      />

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-start gap-3 mb-2">
          <div className="flex-1">
            <InlineEdit
              value={data.title}
              onSave={async (next) => {
                await api.updateDiscussion(data.id, { title: next });
                setData({ ...data, title: next });
              }}
              editLabel="Edit title"
            >
              <h1 className="text-3xl font-bold text-slate-900 inline">{data.title}</h1>
            </InlineEdit>
          </div>
          <button
            onClick={() => setMergeOpen(true)}
            className="btn-secondary text-sm flex-shrink-0"
            title="Merge another discussion into this one"
          >
            Merge…
          </button>
        </div>

        <div className="flex flex-wrap items-center gap-2 mb-3">
          {data.current_state && <Badge label={data.current_state} variant="state" />}
          {data.category && <Badge label={data.category} variant="category" />}
        </div>

        {/* Parent discussion card (for sub-discussions) */}
        {data.parent && (
          <Link
            to={`/discussions/${data.parent.id}`}
            state={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })}
            className="block mb-4 p-3 border border-blue-200 bg-blue-50/50 rounded-lg hover:border-blue-300 hover:bg-blue-50 transition-colors"
          >
            <div className="text-xs text-blue-500 font-medium uppercase tracking-wider mb-1">Parent Discussion</div>
            <div className="flex items-start justify-between gap-2">
              <div className="flex-1 min-w-0">
                <span className="font-medium text-slate-900">{data.parent.title}</span>
                {data.parent.summary && (
                  <p className="text-sm text-slate-500 mt-0.5 line-clamp-1">{data.parent.summary}</p>
                )}
              </div>
              <div className="flex gap-1 flex-shrink-0">
                {data.parent.category && <Badge label={data.parent.category} variant="category" />}
                {data.parent.current_state && <Badge label={data.parent.current_state} variant="state" />}
              </div>
            </div>
          </Link>
        )}

        {data.company_name && data.company_id && (
          <Link
            to={`/companies/${data.company_id}`}
            state={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })}
            className="text-blue-600 hover:text-blue-700 font-medium"
          >
            {data.company_name} →
          </Link>
        )}
      </div>

      {/* Dates row */}
      <div className="flex flex-wrap gap-6 mb-6 text-sm text-slate-500">
        {data.first_seen && (
          <div>
            <span className="font-medium text-slate-700">Started:</span> {formatDate(data.first_seen)}
          </div>
        )}
        {data.last_seen && (
          <div>
            <span className="font-medium text-slate-700">Last active:</span> {formatDate(data.last_seen)}
          </div>
        )}
        {data.updated_at && (
          <div>
            <span className="font-medium text-slate-700">Updated:</span> {formatDate(data.updated_at)}
          </div>
        )}
      </div>

      {/* Summary */}
      <div className="card p-6 mb-6">
        <h2 className="text-base font-semibold text-slate-900 mb-3">Summary</h2>
        <InlineEdit
          value={data.summary ?? ''}
          multiline
          placeholder="No summary yet — click edit to add one."
          onSave={async (next) => {
            await api.updateDiscussion(data.id, { summary: next });
            setData({ ...data, summary: next });
          }}
          editLabel="Edit summary"
        >
          {data.summary ? (
            <p className="text-slate-700 leading-relaxed inline">{data.summary}</p>
          ) : (
            <span className="text-slate-400 italic text-sm">No summary yet.</span>
          )}
        </InlineEdit>
      </div>

      {/* Sub-discussions */}
      {data.children && data.children.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Sub-Discussions
            <span className="ml-2 text-sm font-normal text-slate-500">({data.children.length})</span>
          </h2>
          <div className="divide-y divide-slate-100">
            {data.children.map((child: Discussion) => (
              <Link
                key={child.id}
                to={`/discussions/${child.id}`}
                state={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })}
                className="flex items-start justify-between gap-3 py-3 hover:bg-slate-50 -mx-2 px-2 rounded transition-colors block"
              >
                <div className="flex-1 min-w-0">
                  <h4 className="font-medium text-slate-900">{child.title}</h4>
                  {child.summary && (
                    <p className="text-sm text-slate-500 mt-0.5 line-clamp-2">{child.summary}</p>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1 flex-shrink-0">
                  {child.current_state && <Badge label={child.current_state} variant="state" />}
                  {child.category && <Badge label={child.category} variant="category" />}
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* Proposed Actions (next steps) */}
      <div className="card p-6 mb-6">
        <h2 className="text-base font-semibold text-slate-900 mb-3">
          Next Steps
        </h2>
        <ProposedActionsList actions={data.proposed_actions ?? []} discussionId={data.id} />
      </div>

      {/* Milestones */}
      <div className="card p-6 mb-6">
        <h2 className="text-base font-semibold text-slate-900 mb-3">Milestones</h2>
        <MilestoneTracker milestones={data.milestones ?? []} discussionId={data.id} />
      </div>

      {/* Participants */}
      {data.participants.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Participants
            <span className="ml-2 text-sm font-normal text-slate-500">({data.participants.length})</span>
          </h2>
          <div className="flex flex-wrap gap-2">
            {data.participants.map((p) => (
              <Link
                key={p}
                to={`/contacts/${encodeURIComponent(p)}`}
                className="inline-flex items-center px-3 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm rounded-lg transition-colors"
              >
                {p}
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* State history */}
      {data.state_history.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-5">State History</h2>
          <StateTimeline history={data.state_history} />
        </div>
      )}

      {/* Event Timeline */}
      {data.events && data.events.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">
            Event Timeline
            <span className="ml-2 text-sm font-normal text-slate-500">({data.events.length})</span>
          </h2>
          <EventTimeline
            events={data.events}
            onRefresh={async () => {
              const fresh = await api.getDiscussion(data.id);
              setData(fresh);
            }}
            onThreadClick={(threadId, sourceEmailId) => {
              setHighlightMessageId(sourceEmailId ?? null);
              const thread = data.threads.find((t) => t.thread_id === threadId);
              if (thread) {
                setSelectedThread(thread);
              } else {
                setSelectedThread({
                  id: 0,
                  thread_id: threadId,
                  subject: null,
                  email_count: 0,
                  first_date: null,
                  last_date: null,
                  participants: [],
                  summary: null,
                });
              }
            }}
          />
        </div>
      )}

      {/* Actions */}
      {data.actions && data.actions.length > 0 && (() => {
        const openActions = data.actions.filter(a => a.status !== 'done');
        const doneActions = data.actions.filter(a => a.status === 'done');
        const visibleActions = showDoneActions ? data.actions : openActions;
        return (
          <div className="card p-6 mb-6">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-base font-semibold text-slate-900">
                Actions
                <span className="ml-2 text-sm font-normal text-slate-500">
                  ({openActions.length} open, {doneActions.length} done)
                </span>
              </h2>
              {doneActions.length > 0 && (
                <button
                  onClick={() => setShowDoneActions(!showDoneActions)}
                  className="text-sm text-blue-600 hover:text-blue-700 transition-colors"
                >
                  {showDoneActions ? 'Hide done' : `Show ${doneActions.length} done`}
                </button>
              )}
            </div>
            <div>
              {visibleActions.map((action) => (
                <ActionRow key={action.id} action={action} linkState={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })} />
              ))}
              {visibleActions.length === 0 && (
                <p className="text-sm text-slate-400 py-2">No open actions</p>
              )}
            </div>
          </div>
        );
      })()}

      {/* Calendar Events */}
      {data.calendar_events && data.calendar_events.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Calendar Events
            <span className="ml-2 text-sm font-normal text-slate-500">({data.calendar_events.length})</span>
          </h2>
          <div className="divide-y divide-slate-100">
            {data.calendar_events.map((evt: CalendarEvent) => (
              <div key={evt.id} className="py-3 first:pt-0 last:pb-0">
                <div className="flex items-start justify-between gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-slate-900">
                      {evt.title || '(No title)'}
                      {evt.html_link && (
                        <a
                          href={evt.html_link}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="ml-2 text-blue-500 hover:text-blue-600 text-xs"
                        >
                          ↗
                        </a>
                      )}
                    </div>
                    <div className="flex items-center gap-3 mt-1 text-xs text-slate-400">
                      <span>
                        {evt.all_day
                          ? formatDate(evt.start_time)
                          : `${formatDateTime(evt.start_time)} – ${formatDateTime(evt.end_time)}`
                        }
                      </span>
                      {evt.location && <span>{evt.location}</span>}
                      {evt.attendees.length > 0 && (
                        <span>{evt.attendees.length} attendee{evt.attendees.length !== 1 ? 's' : ''}</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* HubSpot Notes */}
      {data.notes && data.notes.length > 0 && (
        <div className="card p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            HubSpot Notes
            <span className="ml-2 text-sm font-normal text-slate-500">({data.notes.length})</span>
          </h2>
          <div className="space-y-3">
            {data.notes.map((note: HubSpotNote) => (
              <div key={note.id} className="border border-slate-200 rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs text-slate-500">{note.created_at ? formatDate(note.created_at) : 'Unknown date'}</span>
                  {note.hs_url && (
                    <a href={note.hs_url} target="_blank" rel="noopener noreferrer" className="text-xs text-blue-600 hover:underline">
                      View in HubSpot
                    </a>
                  )}
                </div>
                {note.body ? (
                  <div
                    className="text-sm text-slate-700 prose prose-sm max-w-none [&_p]:my-1 [&_br.hs-trailingbreak]:hidden"
                    dangerouslySetInnerHTML={{ __html: note.body }}
                  />
                ) : (
                  <p className="text-sm text-slate-400 italic">No content</p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Threads */}
      {data.threads.length > 0 && (
        <div className="card p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Email Threads
            <span className="ml-2 text-sm font-normal text-slate-500">({data.threads.length})</span>
          </h2>
          <div>
            {data.threads.map((thread) => (
              <ThreadRow
                key={thread.id}
                thread={thread}
                onClick={() => { setHighlightMessageId(null); setSelectedThread(thread); }}
                onMove={() => setMovingThread(thread)}
                onRemove={async () => {
                  if (!confirm(`Remove thread "${thread.subject ?? '(no subject)'}" from this discussion?`)) return;
                  try {
                    await api.removeThreadFromDiscussion(data.id, thread.thread_id);
                    const fresh = await api.getDiscussion(data.id);
                    setData(fresh);
                  } catch (e) {
                    alert(`Remove failed: ${(e as Error).message}`);
                  }
                }}
              />
            ))}
          </div>
        </div>
      )}

      {selectedThread && (
        <ThreadModal
          thread={selectedThread}
          highlightMessageId={highlightMessageId}
          discussionId={id ? parseInt(id, 10) : undefined}
          onClose={() => { setSelectedThread(null); setHighlightMessageId(null); }}
        />
      )}

      {mergeOpen && (
        <Modal title="Merge another discussion into this one" onClose={() => setMergeOpen(false)}>
          <p className="text-sm text-slate-600 mb-4">
            All events, threads, actions, and state history from the selected discussion will be
            moved into <span className="font-medium text-slate-900">{data.title}</span>. The
            selected discussion will be deleted.
          </p>
          <DiscussionPicker
            excludeId={data.id}
            companyId={data.company_id}
            onSelect={async (d) => {
              if (!confirm(`Merge "${d.title}" into "${data.title}"? This is irreversible.`)) return;
              try {
                await api.mergeDiscussions(data.id, d.id);
                setMergeOpen(false);
                // Refresh discussion data to reflect merged threads/events/actions.
                const fresh = await api.getDiscussion(data.id);
                setData(fresh);
              } catch (e) {
                alert(`Merge failed: ${(e as Error).message}`);
              }
            }}
          />
        </Modal>
      )}

      {movingThread && (
        <Modal title="Move thread to another discussion" onClose={() => setMovingThread(null)}>
          <p className="text-sm text-slate-600 mb-4">
            Moving <span className="font-medium text-slate-900">"{movingThread.subject ?? '(no subject)'}"</span>
            {' '}out of this discussion.
          </p>
          <DiscussionPicker
            excludeId={data.id}
            companyId={data.company_id}
            onSelect={async (d) => {
              try {
                await api.addThreadToDiscussion(d.id, movingThread.thread_id, data.id);
                setMovingThread(null);
                const fresh = await api.getDiscussion(data.id);
                setData(fresh);
              } catch (e) {
                alert(`Move failed: ${(e as Error).message}`);
              }
            }}
          />
        </Modal>
      )}
    </div>
  );
}
