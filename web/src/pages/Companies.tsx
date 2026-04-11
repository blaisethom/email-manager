import { useEffect, useState, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { Company } from '../types';
import Badge from '../components/Badge';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 25;

export default function CompaniesPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<Company[]>([]);
  const [total, setTotal] = useState(0);
  const [allLabels, setAllLabels] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const q = searchParams.get('q') ?? '';
  const label = searchParams.get('label') ?? '';
  const sort = searchParams.get('sort') ?? 'email_count';
  const order = searchParams.get('order') ?? 'desc';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    api
      .getCompanies({ q, label, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
        setAllLabels(data.labels);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, label, sort, order, page]);

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
    <div className="p-4 sm:p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Companies</h1>
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
          placeholder="Search name or domain…"
          className="w-full sm:w-64"
        />

        <select
          value={label}
          onChange={(e) => updateParam('label', e.target.value)}
          className="filter-input"
        >
          <option value="">All labels</option>
          {allLabels.map((l) => (
            <option key={l} value={l}>{l}</option>
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
          <option value="email_count:desc">Most emails</option>
          <option value="name:asc">Name A–Z</option>
          <option value="name:desc">Name Z–A</option>
          <option value="last_seen:desc">Recently active</option>
          <option value="last_seen:asc">Least recent</option>
        </select>
      </div>

      {/* Content */}
      {error ? (
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">Failed to load companies</p>
          <p className="text-sm mt-1 text-red-500">{error}</p>
          <button onClick={fetchData} className="mt-3 btn-secondary">
            Retry
          </button>
        </div>
      ) : loading ? (
        <div className="card">
          <div className="animate-pulse">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="flex items-center gap-4 px-6 py-4 border-b border-slate-100 last:border-0">
                <div className="h-4 bg-slate-200 rounded w-48" />
                <div className="h-4 bg-slate-200 rounded w-24 ml-auto" />
                <div className="h-4 bg-slate-200 rounded w-16" />
              </div>
            ))}
          </div>
        </div>
      ) : items.length === 0 ? (
        <div className="card">
          <EmptyState />
        </div>
      ) : (
        <div className="card overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50">
                <th className="text-left px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Company
                </th>
                <th className="hidden sm:table-cell text-left px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Labels
                </th>
                <th className="text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Emails
                </th>
                <th className="hidden sm:table-cell text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Last active
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {items.map((company) => (
                <tr
                  key={company.id}
                  onClick={() => navigate(`/companies/${company.id}`, { state: { breadcrumbs: [{ label: 'Companies', path: '/companies' }] } })}
                  className="table-row-clickable"
                >
                  <td className="px-6 py-4">
                    <div className="font-medium text-slate-900">{company.name}</div>
                    {company.domain && (
                      <div className="text-xs text-slate-500 mt-0.5">{company.domain}</div>
                    )}
                  </td>
                  <td className="hidden sm:table-cell px-6 py-4">
                    <div className="flex flex-wrap gap-1">
                      {company.labels.slice(0, 4).map((l) => (
                        <Badge key={l} label={l} variant="label" />
                      ))}
                      {company.labels.length > 4 && (
                        <span className="text-xs text-slate-400">+{company.labels.length - 4}</span>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 text-right font-medium text-slate-700">
                    {company.email_count.toLocaleString()}
                  </td>
                  <td className="hidden sm:table-cell px-6 py-4 text-right text-slate-500">
                    {formatDate(company.last_seen)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="px-6 border-t border-slate-100">
            <Pagination
              page={page}
              total={total}
              limit={LIMIT}
              onPageChange={(p) => updateParam('page', String(p))}
            />
          </div>
        </div>
      )}
    </div>
  );
}
