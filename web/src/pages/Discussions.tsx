import { useEffect, useState, useCallback, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { Discussion, CategoryConfig, ProposedAction } from '../types';
import Badge from '../components/Badge';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 20;

// ── Shared card used in both list and kanban views ─────────────────────────

const PRIORITY_PILL: Record<string, string> = {
  high: 'text-white bg-red-500',
  medium: 'text-white bg-amber-500',
  low: 'text-slate-600 bg-slate-200',
};

const PRIORITY_DETAIL: Record<string, string> = {
  high: 'text-red-600 bg-red-50',
  medium: 'text-amber-600 bg-amber-50',
  low: 'text-slate-500 bg-slate-100',
};

function NextStepsPanel({ discussionId }: { discussionId: number }) {
  const [actions, setActions] = useState<ProposedAction[] | null>(null);

  useEffect(() => {
    api.getProposedActions(discussionId).then(setActions);
  }, [discussionId]);

  if (!actions) {
    return <div className="px-5 pb-4 text-xs text-slate-400">Loading...</div>;
  }

  return (
    <div className="px-5 pb-4">
      <div className="border-t border-slate-100 pt-3 space-y-2">
        {actions.map((a) => (
          <div key={a.id} className="flex items-start gap-2 text-sm">
            <span className={`text-xs font-medium px-1.5 py-0.5 rounded flex-shrink-0 ${PRIORITY_DETAIL[a.priority] ?? PRIORITY_DETAIL.low}`}>
              {a.priority}
            </span>
            <div className="min-w-0">
              <p className="text-slate-700">{a.action}</p>
              {a.reasoning && (
                <p className="text-xs text-slate-400 mt-0.5">{a.reasoning}</p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function DiscussionCard({ disc, onClick, compact, showActions = false }: { disc: Discussion; onClick: () => void; compact?: boolean; showActions?: boolean }) {
  const highCount = disc.high_priority_count ?? 0;
  const medCount = disc.med_priority_count ?? 0;
  const hasPills = highCount > 0 || medCount > 0;
  const [localToggle, setLocalToggle] = useState<boolean | null>(null);
  // Reset local override when page-level toggle changes
  useEffect(() => setLocalToggle(null), [showActions]);
  const expanded = hasPills && (localToggle ?? showActions);

  if (compact) {
    return (
      <div
        onClick={onClick}
        className="bg-white rounded-lg border border-slate-200 p-3 cursor-pointer hover:shadow-md hover:border-slate-300 transition-all"
      >
        <h4 className="text-sm font-medium text-slate-900 leading-snug line-clamp-2">{disc.title}</h4>
        {disc.company_name && (
          <p className="text-xs text-blue-600 mt-1 truncate">{disc.company_name}</p>
        )}
      </div>
    );
  }

  return (
    <div className="card hover:shadow-md hover:border-slate-300 transition-all">
      <div
        onClick={onClick}
        className="p-5 cursor-pointer"
      >
        <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2 sm:gap-4">
          <div className="flex-1 min-w-0">
            <h3 className="font-semibold text-slate-900 leading-snug">{disc.title}</h3>

            {disc.company_name && (
              <p className="text-sm text-blue-600 mt-0.5 hover:underline">
                {disc.company_name}
              </p>
            )}

            {disc.summary && (
              <p className="text-sm text-slate-600 mt-2 line-clamp-2 leading-relaxed">
                {disc.summary}
              </p>
            )}
          </div>

          <div className="flex flex-wrap sm:flex-col sm:items-end gap-1.5 flex-shrink-0">
            {disc.current_state && <Badge label={disc.current_state} variant="state" />}
            {disc.category && <Badge label={disc.category} variant="category" />}
          </div>
        </div>

        <div className="mt-3 flex items-center gap-4 text-xs text-slate-400">
          {hasPills && (
            <span
              onClick={(e) => { e.stopPropagation(); setLocalToggle(!expanded); }}
              className="flex items-center gap-1.5 cursor-pointer"
            >
              {highCount > 0 && (
                <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold ${PRIORITY_PILL.high}`}>
                  {highCount} high
                </span>
              )}
              {medCount > 0 && (
                <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold ${PRIORITY_PILL.medium}`}>
                  {medCount} med
                </span>
              )}
            </span>
          )}
          {disc.participants.length > 0 && (
            <span>{disc.participants.length} participant{disc.participants.length !== 1 ? 's' : ''}</span>
          )}
          {disc.last_seen && (
            <span>Last active {formatDate(disc.last_seen)}</span>
          )}
          {disc.first_seen && (
            <span>Started {formatDate(disc.first_seen)}</span>
          )}
        </div>
      </div>

      {expanded && <NextStepsPanel discussionId={disc.id} />}
    </div>
  );
}

// ── Kanban board ───────────────────────────────────────────────────────────

function KanbanBoard({
  items,
  columns,
  onClickDiscussion,
}: {
  items: Discussion[];
  columns: string[];
  onClickDiscussion: (id: number) => void;
}) {
  const byState = useMemo(() => {
    const map: Record<string, Discussion[]> = {};
    for (const col of columns) map[col] = [];
    for (const disc of items) {
      const s = disc.current_state ?? '';
      if (map[s]) map[s].push(disc);
    }
    return map;
  }, [items, columns]);

  return (
    <div className="flex gap-4 overflow-x-auto pb-4 -mx-4 px-4 sm:-mx-8 sm:px-8">
      {columns.map((col) => (
        <div key={col} className="flex-shrink-0 w-72">
          <div className="flex items-center gap-2 mb-3 px-1">
            <Badge label={col} variant="state" />
            <span className="text-xs text-slate-400 font-medium">{byState[col].length}</span>
          </div>
          <div className="space-y-3 min-h-[100px]">
            {byState[col].length === 0 ? (
              <div className="rounded-lg border-2 border-dashed border-slate-200 p-4 text-center text-xs text-slate-400">
                No discussions
              </div>
            ) : (
              byState[col].map((disc) => (
                <DiscussionCard
                  key={disc.id}
                  disc={disc}
                  compact
                  onClick={() => onClickDiscussion(disc.id)}
                />
              ))
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

// ── View toggle icons ──────────────────────────────────────────────────────

function ListIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="8" y1="6" x2="21" y2="6" /><line x1="8" y1="12" x2="21" y2="12" /><line x1="8" y1="18" x2="21" y2="18" />
      <line x1="3" y1="6" x2="3.01" y2="6" /><line x1="3" y1="12" x2="3.01" y2="12" /><line x1="3" y1="18" x2="3.01" y2="18" />
    </svg>
  );
}

function KanbanIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="5" height="18" rx="1" /><rect x="10" y="3" width="5" height="12" rx="1" /><rect x="17" y="3" width="5" height="15" rx="1" />
    </svg>
  );
}

// ── Main page ──────────────────────────────────────────────────────────────

export default function DiscussionsPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<Discussion[]>([]);
  const [total, setTotal] = useState(0);
  const [categories, setCategories] = useState<string[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [categoryConfig, setCategoryConfig] = useState<CategoryConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showNextSteps, setShowNextSteps] = useState(true);

  const q = searchParams.get('q') ?? '';
  const category = searchParams.get('category') ?? '';
  const state = searchParams.get('state') ?? '';
  const sort = searchParams.get('sort') ?? 'last_seen';
  const order = searchParams.get('order') ?? 'desc';
  const page = parseInt(searchParams.get('page') ?? '1', 10);
  const view = searchParams.get('view') ?? 'list';
  const hideTerminal = searchParams.get('hideTerminal') === '1';

  // Load category config once from meta
  useEffect(() => {
    api.getMeta().then((meta) => {
      if (meta.categoryConfig?.length) {
        setCategoryConfig(meta.categoryConfig);
      }
    }).catch(() => {});
  }, []);

  // Build lookup maps from config
  const configByCategory = useMemo(() => {
    const map: Record<string, CategoryConfig> = {};
    for (const c of categoryConfig) map[c.name] = c;
    return map;
  }, [categoryConfig]);

  const terminalStatesStr = useMemo(() => {
    if (!category || !configByCategory[category]) return '';
    const ts = configByCategory[category].terminal_states;
    return ts.length > 0 ? ts.join(',') : '';
  }, [category, configByCategory]);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    const exclude_states = hideTerminal && terminalStatesStr ? terminalStatesStr : undefined;
    api
      .getDiscussions({ q, category, state, exclude_states, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
        setCategories(data.categories);
        setStates(data.states);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, category, state, sort, order, page, hideTerminal, terminalStatesStr]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Filter states for the dropdown based on selected category
  const filteredStates = useMemo(() => {
    if (!category || !configByCategory[category]) return states;
    return configByCategory[category].states.filter((s) => states.includes(s));
  }, [category, configByCategory, states]);

  // Compute kanban columns from config
  const kanbanColumns = useMemo(() => {
    if (!category || !configByCategory[category]) return [];
    const cfg = configByCategory[category];
    const terminal = new Set(cfg.terminal_states);
    if (hideTerminal) {
      return cfg.states.filter((s) => !terminal.has(s));
    }
    return cfg.states;
  }, [category, configByCategory, hideTerminal]);

  const terminalStates = useMemo(() => {
    if (!terminalStatesStr) return new Set<string>();
    return new Set(terminalStatesStr.split(','));
  }, [terminalStatesStr]);

  function updateParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    if (key === 'category') {
      next.delete('state');
    }
    if (key !== 'page') next.delete('page');
    setSearchParams(next);
  }

  function setView(v: 'list' | 'kanban') {
    const next = new URLSearchParams(searchParams);
    if (v === 'list') {
      next.delete('view');
    } else {
      next.set('view', 'kanban');
    }
    setSearchParams(next);
  }

  const isKanban = view === 'kanban';
  const canKanban = !!category && kanbanColumns.length > 0;

  return (
    <div className="p-4 sm:p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Discussions</h1>
          {!loading && (
            <p className="text-sm text-slate-500 mt-0.5">{total.toLocaleString()} total</p>
          )}
        </div>

        {/* View toggle */}
        <div className="flex items-center gap-1 bg-slate-100 rounded-lg p-0.5">
          <button
            onClick={() => setView('list')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
              !isKanban ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <ListIcon /> List
          </button>
          <button
            onClick={() => setView('kanban')}
            title={!canKanban ? 'Select a category to use board view' : undefined}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
              isKanban ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500 hover:text-slate-700'
            } ${!canKanban ? 'opacity-50 cursor-not-allowed' : ''}`}
            disabled={!canKanban}
          >
            <KanbanIcon /> Board
          </button>
        </div>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-3 mb-6">
        <SearchBar
          value={q}
          onChange={(v) => updateParam('q', v)}
          placeholder="Search title, summary or company..."
          className="w-full sm:w-72"
        />

        <select
          value={category}
          onChange={(e) => updateParam('category', e.target.value)}
          className="filter-input"
        >
          <option value="">All categories</option>
          {categories.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>

        <select
          value={state}
          onChange={(e) => updateParam('state', e.target.value)}
          className="filter-input"
          disabled={!category}
        >
          <option value="">{category ? 'All states' : 'Select a category first'}</option>
          {filteredStates.map((s) => (
            <option key={s} value={s}>
              {s}{terminalStates.has(s) ? ' (terminal)' : ''}
            </option>
          ))}
        </select>

        <select
          value={`${sort}:${order}`}
          onChange={(e) => {
            const [s, o] = e.target.value.split(':');
            const next = new URLSearchParams(searchParams);
            next.set('sort', s);
            next.set('order', o);
            next.delete('page');
            setSearchParams(next);
          }}
          className="filter-input"
        >
          <option value="last_seen:desc">Most recent</option>
          <option value="first_seen:asc">Oldest first</option>
          <option value="title:asc">Title A-Z</option>
          <option value="title:desc">Title Z-A</option>
        </select>

        <label className="flex items-center gap-2 text-sm text-slate-600 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={showNextSteps}
            onChange={(e) => setShowNextSteps(e.target.checked)}
            className="rounded border-slate-300 text-blue-600 focus:ring-blue-500"
          />
          Next steps
        </label>

        {terminalStates.size > 0 && (
          <label className="flex items-center gap-2 text-sm text-slate-600 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={hideTerminal}
              onChange={(e) => {
                const next = new URLSearchParams(searchParams);
                if (e.target.checked) {
                  next.set('hideTerminal', '1');
                } else {
                  next.delete('hideTerminal');
                }
                next.delete('page');
                setSearchParams(next);
              }}
              className="rounded border-slate-300 text-blue-600 focus:ring-blue-500"
            />
            Hide terminal states
          </label>
        )}
      </div>

      {/* Content */}
      {error ? (
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">Failed to load discussions</p>
          <p className="text-sm mt-1 text-red-500">{error}</p>
          <button onClick={fetchData} className="mt-3 btn-secondary">
            Retry
          </button>
        </div>
      ) : loading ? (
        <div className="space-y-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="card p-5 animate-pulse">
              <div className="h-5 bg-slate-200 rounded w-2/3 mb-3" />
              <div className="h-4 bg-slate-200 rounded w-full mb-2" />
              <div className="h-4 bg-slate-200 rounded w-4/5" />
            </div>
          ))}
        </div>
      ) : items.length === 0 ? (
        <div className="card">
          <EmptyState />
        </div>
      ) : (
        <>
          {isKanban && canKanban ? (
            <KanbanBoard
              items={items}
              columns={kanbanColumns}
              onClickDiscussion={(id) => navigate(`/discussions/${id}`, { state: { breadcrumbs: [{ label: 'Discussions', path: searchParams.toString() ? `/discussions?${searchParams.toString()}` : '/discussions' }] } })}
            />
          ) : (
            <div className="space-y-4">
              {items.map((disc) => (
                <DiscussionCard
                  key={disc.id}
                  disc={disc}
                  showActions={showNextSteps}
                  onClick={() => navigate(`/discussions/${disc.id}`, { state: { breadcrumbs: [{ label: 'Discussions', path: searchParams.toString() ? `/discussions?${searchParams.toString()}` : '/discussions' }] } })}
                />
              ))}
            </div>
          )}

          <div className="mt-4">
            <Pagination
              page={page}
              total={total}
              limit={LIMIT}
              onPageChange={(p) => updateParam('page', String(p))}
            />
          </div>
        </>
      )}
    </div>
  );
}
