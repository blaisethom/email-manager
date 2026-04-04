import { useEffect, useState, useCallback } from 'react';
import { useNavigate, useSearchParams, Link } from 'react-router-dom';
import { api } from '../api';
import type { Discussion } from '../types';
import Badge from '../components/Badge';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 20;

function DiscussionCard({ disc, onClick }: { disc: Discussion; onClick: () => void }) {
  return (
    <div
      onClick={onClick}
      className="card p-5 cursor-pointer hover:shadow-md hover:border-slate-300 transition-all"
    >
      <div className="flex items-start justify-between gap-4">
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

        <div className="flex flex-col items-end gap-1.5 flex-shrink-0">
          {disc.current_state && <Badge label={disc.current_state} variant="state" />}
          {disc.category && <Badge label={disc.category} variant="category" />}
        </div>
      </div>

      <div className="mt-3 flex items-center gap-4 text-xs text-slate-400">
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
  );
}

export default function DiscussionsPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<Discussion[]>([]);
  const [total, setTotal] = useState(0);
  const [categories, setCategories] = useState<string[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const q = searchParams.get('q') ?? '';
  const category = searchParams.get('category') ?? '';
  const state = searchParams.get('state') ?? '';
  const sort = searchParams.get('sort') ?? 'last_seen';
  const order = searchParams.get('order') ?? 'desc';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    api
      .getDiscussions({ q, category, state, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
        setCategories(data.categories);
        setStates(data.states);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, category, state, sort, order, page]);

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

  return (
    <div className="p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Discussions</h1>
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
          placeholder="Search title or summary…"
          className="w-72"
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
        >
          <option value="">All states</option>
          {states.map((s) => (
            <option key={s} value={s}>{s}</option>
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
          <option value="title:asc">Title A–Z</option>
          <option value="title:desc">Title Z–A</option>
        </select>
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
          <div className="space-y-4">
            {items.map((disc) => (
              <DiscussionCard
                key={disc.id}
                disc={disc}
                onClick={() => navigate(`/discussions/${disc.id}`)}
              />
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
