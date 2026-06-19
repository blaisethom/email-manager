import { useEffect, useState, useCallback } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { HubSpotTask } from '../types';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 50;

const STATUS_LABEL: Record<string, string> = {
  NOT_STARTED: 'Not Started',
  IN_PROGRESS: 'In Progress',
  WAITING: 'Waiting',
  DEFERRED: 'Deferred',
  COMPLETED: 'Completed',
};

const STATUS_CLASS: Record<string, string> = {
  NOT_STARTED: 'bg-red-100 text-red-700',
  IN_PROGRESS: 'bg-amber-100 text-amber-700',
  WAITING: 'bg-blue-100 text-blue-700',
  DEFERRED: 'bg-slate-100 text-slate-600',
  COMPLETED: 'bg-green-100 text-green-700',
};

const PRIORITY_CLASS: Record<string, string> = {
  HIGH: 'bg-red-50 text-red-700 border border-red-200',
  MEDIUM: 'bg-amber-50 text-amber-700 border border-amber-200',
  LOW: 'bg-slate-50 text-slate-600 border border-slate-200',
};

function isDueSoon(dueDate: string | null): boolean {
  if (!dueDate) return false;
  const d = new Date(dueDate);
  const now = new Date();
  const diff = (d.getTime() - now.getTime()) / (1000 * 60 * 60 * 24);
  return diff <= 3 && diff >= 0;
}

function isOverdue(dueDate: string | null): boolean {
  if (!dueDate) return false;
  return new Date(dueDate) < new Date();
}

function DueDateCell({ date, status }: { date: string | null; status: string | null }) {
  if (!date) return <span className="text-slate-400">—</span>;
  const over = isOverdue(date) && status !== 'COMPLETED';
  const soon = isDueSoon(date) && status !== 'COMPLETED';
  const cls = over ? 'text-red-600 font-medium' : soon ? 'text-amber-600 font-medium' : 'text-slate-600';
  return <span className={cls}>{formatDate(date)}</span>;
}

function StatusBadge({ status }: { status: string | null }) {
  const s = status ?? 'NOT_STARTED';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${STATUS_CLASS[s] ?? 'bg-slate-100 text-slate-600'}`}>
      {STATUS_LABEL[s] ?? s}
    </span>
  );
}

function PriorityBadge({ priority }: { priority: string | null }) {
  if (!priority || !PRIORITY_CLASS[priority]) return null;
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium ${PRIORITY_CLASS[priority]}`}>
      {priority}
    </span>
  );
}

export default function TasksPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [tasks, setTasks] = useState<HubSpotTask[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [threadData, setThreadData] = useState<Record<string, any>>({});

  const q = searchParams.get('q') ?? '';
  const status = searchParams.get('status') ?? 'open';
  const page = Math.max(1, parseInt(searchParams.get('page') ?? '1'));

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    api.getTasks({ q, status, page, limit: LIMIT })
      .then(r => { setTasks(r.items); setTotal(r.total); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [q, status, page]);

  useEffect(() => { load(); }, [load]);

  function setParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    next.set(key, value);
    if (key !== 'page') next.set('page', '1');
    setSearchParams(next);
  }

  async function toggleExpand(id: string) {
    if (expandedId === id) {
      setExpandedId(null);
      return;
    }
    setExpandedId(id);
    if (!threadData[id]) {
      try {
        const detail = await api.getTask(id);
        setThreadData(prev => ({ ...prev, [id]: detail.threads }));
      } catch {
        setThreadData(prev => ({ ...prev, [id]: [] }));
      }
    }
  }

  const statusTabs = [
    { value: 'open', label: 'Open' },
    { value: 'all', label: 'All' },
    { value: 'completed', label: 'Completed' },
  ];

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-900">Tasks</h1>
        <p className="text-slate-500 text-sm mt-1">HubSpot tasks assigned to you</p>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-3 mb-5">
        {/* Status tabs */}
        <div className="flex items-center bg-slate-100 rounded-lg p-0.5 gap-0.5">
          {statusTabs.map(tab => (
            <button
              key={tab.value}
              onClick={() => setParam('status', tab.value)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                status === tab.value
                  ? 'bg-white text-slate-900 shadow-sm'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="flex-1 min-w-[200px] max-w-xs">
          <SearchBar
            value={q}
            onChange={v => setParam('q', v)}
            placeholder="Search tasks…"
          />
        </div>

        <span className="text-sm text-slate-500 ml-auto">{total.toLocaleString()} task{total !== 1 ? 's' : ''}</span>
      </div>

      {error && (
        <div className="rounded-lg bg-red-50 border border-red-200 p-4 text-red-700 text-sm mb-4">{error}</div>
      )}

      {loading ? (
        <div className="text-slate-500 text-sm py-12 text-center">Loading…</div>
      ) : tasks.length === 0 ? (
        <EmptyState
          title="No tasks found"
          message={status === 'open' ? 'No open tasks. Run "hubspot tasks --sync" to pull from HubSpot.' : 'No tasks match your filter.'}
        />
      ) : (
        <div className="space-y-1.5">
          {tasks.map(task => {
            const expanded = expandedId === task.id;
            const threads = threadData[task.id] as any[] | undefined;
            return (
              <div key={task.id} className="bg-white rounded-lg border border-slate-200 overflow-hidden">
                {/* Task row */}
                <div
                  className="flex items-start gap-3 px-4 py-3 cursor-pointer hover:bg-slate-50 transition-colors"
                  onClick={() => toggleExpand(task.id)}
                >
                  {/* Expand chevron */}
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    className={`mt-1 flex-shrink-0 text-slate-400 transition-transform ${expanded ? 'rotate-90' : ''}`}
                  >
                    <polyline points="9 18 15 12 9 6" />
                  </svg>

                  {/* Main content */}
                  <div className="flex-1 min-w-0">
                    {/* Title: Company — Subject */}
                    <p className="text-sm font-medium text-slate-900 leading-snug">
                      {task.companies.length > 0 && (() => {
                        const co = task.companies[0];
                        const label = co.name ?? co.domain ?? co.id;
                        const linkProps = co.local_id != null
                          ? { as: 'internal', to: `/companies/${co.local_id}` }
                          : co.hs_url
                          ? { as: 'external', href: co.hs_url }
                          : null;
                        return (
                          <>
                            {linkProps?.as === 'internal' ? (
                              <Link
                                to={(linkProps as any).to}
                                onClick={e => e.stopPropagation()}
                                className="text-violet-700 hover:text-violet-900 hover:underline"
                              >
                                {label}
                              </Link>
                            ) : linkProps?.as === 'external' ? (
                              <a
                                href={(linkProps as any).href}
                                target="_blank"
                                rel="noopener noreferrer"
                                onClick={e => e.stopPropagation()}
                                className="text-violet-700 hover:text-violet-900 hover:underline"
                              >
                                {label}
                              </a>
                            ) : (
                              <span className="text-violet-700">{label}</span>
                            )}
                            <span className="text-slate-400 mx-1.5">—</span>
                          </>
                        );
                      })()}
                      {task.subject ?? '(no subject)'}
                    </p>
                    {/* Contacts */}
                    {task.contacts.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 mt-1.5">
                        {task.contacts.map(c => (
                          <Link
                            key={c.id}
                            to={c.email ? `/contacts/${encodeURIComponent(c.email)}` : '#'}
                            onClick={e => e.stopPropagation()}
                            className="text-xs text-blue-600 hover:text-blue-800 hover:underline"
                          >
                            {c.name ?? c.email ?? c.id}
                          </Link>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* Right meta */}
                  <div className="flex items-center gap-4 flex-shrink-0 text-sm">
                    {task.thread_count > 0 && (
                      <span className="text-xs text-slate-500 bg-slate-100 rounded px-1.5 py-0.5">
                        {task.thread_count} thread{task.thread_count !== 1 ? 's' : ''}
                      </span>
                    )}
                    <DueDateCell date={task.due_date} status={task.status} />
                    {task.hs_url && (
                      <a
                        href={task.hs_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        onClick={e => e.stopPropagation()}
                        className="text-slate-400 hover:text-orange-500"
                        title="Open in HubSpot"
                      >
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                          <polyline points="15 3 21 3 21 9" />
                          <line x1="10" y1="14" x2="21" y2="3" />
                        </svg>
                      </a>
                    )}
                  </div>
                </div>

                {/* Expanded: body + threads */}
                {expanded && (
                  <div className="border-t border-slate-100 px-4 py-3 bg-slate-50">
                    <div className="flex flex-wrap items-center gap-2 mb-3">
                      <StatusBadge status={task.status} />
                      <PriorityBadge priority={task.priority} />
                      {task.type && task.type !== 'TODO' && (
                        <span className="text-xs text-slate-500 uppercase tracking-wide">{task.type}</span>
                      )}
                    </div>
                    {task.body && (
                      <div
                        className="task-body text-sm text-slate-700 mb-3"
                        dangerouslySetInnerHTML={{ __html: task.body }}
                      />
                    )}
                    {threads === undefined ? (
                      <p className="text-xs text-slate-400">Loading threads…</p>
                    ) : threads.length === 0 ? (
                      <p className="text-xs text-slate-400">
                        No linked email threads.
                        {task.contacts.length > 0
                          ? ' Run "hubspot enrich-tasks" to find related emails.'
                          : ' This task has no associated contacts.'}
                      </p>
                    ) : (
                      <div>
                        <p className="text-xs font-medium text-slate-500 uppercase tracking-wide mb-2">Related email threads</p>
                        <div className="space-y-1">
                          {threads.map((t: any) => (
                            <div key={t.thread_id} className="flex items-center gap-3 text-sm py-1">
                              <span className="text-xs text-slate-400 w-16 flex-shrink-0">{t.email_count} emails</span>
                              <span className="flex-1 text-slate-700 truncate">{t.subject ?? '(no subject)'}</span>
                              <span className="text-xs text-slate-400 flex-shrink-0">{formatDate(t.last_date)}</span>
                              <span className="text-xs text-blue-600 flex-shrink-0">{t.contact_email}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {total > LIMIT && (
        <div className="mt-6">
          <Pagination
            page={page}
            total={total}
            limit={LIMIT}
            onPageChange={(p: number) => setParam('page', String(p))}
          />
        </div>
      )}
    </div>
  );
}
