import { useEffect, useState, useCallback, useRef } from 'react';
import { useNavigate, useSearchParams, Link } from 'react-router-dom';
import { api } from '../api';
import type { Action } from '../types';
import Badge from '../components/Badge';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 25;

const DEFAULTS = {
  status: 'open',
  sort: 'source_date',
  order: 'desc',
};

function AssigneeMultiSelect({
  selected,
  options,
  onChange,
}: {
  selected: string[];
  options: string[];
  onChange: (selected: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  function toggle(email: string) {
    const next = selected.includes(email)
      ? selected.filter((e) => e !== email)
      : [...selected, email];
    onChange(next);
  }

  const label =
    selected.length === 0
      ? 'All assignees'
      : selected.length === 1
        ? selected[0]
        : `${selected.length} assignees`;

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="filter-input flex items-center gap-1.5 text-left min-w-[10rem]"
      >
        <span className="truncate flex-1">{label}</span>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="12"
          height="12"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="flex-shrink-0"
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>
      {open && (
        <div className="absolute z-20 mt-1 w-72 max-h-64 overflow-y-auto bg-white border border-slate-200 rounded-lg shadow-lg">
          {selected.length > 0 && (
            <button
              type="button"
              onClick={() => onChange([])}
              className="w-full px-3 py-2 text-left text-xs text-blue-600 hover:bg-slate-50 border-b border-slate-100"
            >
              Clear all
            </button>
          )}
          {options.map((email) => (
            <label
              key={email}
              className="flex items-center gap-2.5 px-3 py-2 hover:bg-slate-50 cursor-pointer text-sm"
            >
              <input
                type="checkbox"
                checked={selected.includes(email)}
                onChange={() => toggle(email)}
                className="rounded border-slate-300 text-blue-600 focus:ring-blue-500"
              />
              <span className="truncate text-slate-700">{email}</span>
            </label>
          ))}
          {options.length === 0 && (
            <p className="px-3 py-2 text-sm text-slate-400">No assignees found</p>
          )}
        </div>
      )}
    </div>
  );
}

function ActionCard({ action }: { action: Action }) {
  return (
    <div className="card p-5 hover:shadow-md hover:border-slate-300 transition-all">
      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2 sm:gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <Badge label={action.status} variant="state" />
            {action.target_date && (
              <span className="text-xs text-slate-500">
                Due {formatDate(action.target_date)}
              </span>
            )}
            {action.completed_date && (
              <span className="text-xs text-green-600">
                Completed {formatDate(action.completed_date)}
              </span>
            )}
          </div>

          <p className="text-sm text-slate-900 font-medium leading-relaxed mt-1.5">
            {action.description}
          </p>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-2 text-xs text-slate-500">
            {action.assignee_emails.length > 0 && (
              <span className="flex flex-wrap gap-x-2 gap-y-0.5">
                {action.assignee_emails.map((email) => (
                  <Link
                    key={email}
                    to={`/contacts/${encodeURIComponent(email)}`}
                    state={{ breadcrumbs: [{ label: 'Actions', path: '/actions' }] }}
                    className="text-blue-600 hover:text-blue-700 hover:underline"
                    onClick={(e) => e.stopPropagation()}
                  >
                    {email}
                  </Link>
                ))}
              </span>
            )}
            {action.discussion_title && (
              <Link
                to={`/discussions/${action.discussion_id}`}
                state={{ breadcrumbs: [{ label: 'Actions', path: '/actions' }] }}
                className="text-blue-600 hover:text-blue-700 hover:underline truncate max-w-xs"
                onClick={(e) => e.stopPropagation()}
              >
                ↗ {action.discussion_title}
              </Link>
            )}
            {action.company_name && action.company_id && (
              <Link
                to={`/companies/${action.company_id}`}
                state={{ breadcrumbs: [{ label: 'Actions', path: '/actions' }] }}
                className="text-slate-400 hover:text-slate-600 hover:underline"
                onClick={(e) => e.stopPropagation()}
              >
                {action.company_name}
              </Link>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function ActionsPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<Action[]>([]);
  const [total, setTotal] = useState(0);
  const [statuses, setStatuses] = useState<string[]>([]);
  const [assigneeOptions, setAssigneeOptions] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [defaultsApplied, setDefaultsApplied] = useState(false);

  // Apply defaults on first load when no URL params are set
  useEffect(() => {
    if (defaultsApplied) return;

    // If the URL already has params, respect them
    const hasParams = Array.from(searchParams.keys()).some((k) =>
      ['status', 'sort', 'order', 'assignee', 'q'].includes(k)
    );
    if (hasParams) {
      setDefaultsApplied(true);
      return;
    }

    // Fetch user emails from meta and apply defaults
    api.getMeta().then((meta) => {
      const next = new URLSearchParams(searchParams);
      next.set('status', DEFAULTS.status);
      next.set('sort', DEFAULTS.sort);
      next.set('order', DEFAULTS.order);
      if (meta.userEmails.length > 0) {
        next.set('assignee', meta.userEmails.join(','));
      }
      setSearchParams(next, { replace: true });
      setDefaultsApplied(true);
    }).catch(() => {
      // If meta fails, still apply non-email defaults
      const next = new URLSearchParams(searchParams);
      next.set('status', DEFAULTS.status);
      next.set('sort', DEFAULTS.sort);
      next.set('order', DEFAULTS.order);
      setSearchParams(next, { replace: true });
      setDefaultsApplied(true);
    });
  }, [searchParams, setSearchParams, defaultsApplied]);

  const q = searchParams.get('q') ?? '';
  const status = searchParams.get('status') ?? '';
  const assignee = searchParams.get('assignee') ?? '';
  const selectedAssignees = assignee ? assignee.split(',') : [];
  const sort = searchParams.get('sort') ?? DEFAULTS.sort;
  const order = searchParams.get('order') ?? DEFAULTS.order;
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    if (!defaultsApplied) return;
    setLoading(true);
    setError(null);
    api
      .getActions({ q, status, assignee, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
        setStatuses(data.statuses);
        setAssigneeOptions(data.assignees);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, status, assignee, sort, order, page, defaultsApplied]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  function updateParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    if (key !== 'page') next.delete('page');
    setSearchParams(next);
  }

  function updateAssignees(emails: string[]) {
    updateParam('assignee', emails.join(','));
  }

  return (
    <div className="p-4 sm:p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Actions</h1>
          {!loading && (
            <p className="text-sm text-slate-500 mt-0.5">{total.toLocaleString()} total</p>
          )}
        </div>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap gap-3 mb-6">
        <SearchBar
          value={q}
          onChange={(v) => updateParam('q', v)}
          placeholder="Search actions..."
          className="w-full sm:w-72"
        />

        <select
          value={status}
          onChange={(e) => updateParam('status', e.target.value)}
          className="filter-input"
        >
          <option value="">All statuses</option>
          {statuses.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        <AssigneeMultiSelect
          selected={selectedAssignees}
          options={assigneeOptions}
          onChange={updateAssignees}
        />

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
          <option value="source_date:desc">Most recent</option>
          <option value="source_date:asc">Oldest first</option>
          <option value="status:asc">Open first</option>
          <option value="status:desc">Done first</option>
          <option value="target_date:asc">Due date (earliest)</option>
          <option value="target_date:desc">Due date (latest)</option>
        </select>
      </div>

      {/* Content */}
      {error ? (
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">Failed to load actions</p>
          <p className="text-sm mt-1 text-red-500">{error}</p>
          <button onClick={fetchData} className="mt-3 btn-secondary">
            Retry
          </button>
        </div>
      ) : loading ? (
        <div className="space-y-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="card p-5 animate-pulse">
              <div className="h-4 bg-slate-200 rounded w-16 mb-3" />
              <div className="h-5 bg-slate-200 rounded w-3/4 mb-2" />
              <div className="h-4 bg-slate-200 rounded w-1/2" />
            </div>
          ))}
        </div>
      ) : items.length === 0 ? (
        <div className="card">
          <EmptyState />
        </div>
      ) : (
        <>
          <div className="space-y-3">
            {items.map((action) => (
              <ActionCard key={action.id} action={action} />
            ))}
          </div>

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
