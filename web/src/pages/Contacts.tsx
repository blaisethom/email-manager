import { useEffect, useState, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { Contact } from '../types';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 25;

export default function ContactsPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<Contact[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const q = searchParams.get('q') ?? '';
  const company = searchParams.get('company') ?? '';
  const sort = searchParams.get('sort') ?? 'email_count';
  const order = searchParams.get('order') ?? 'desc';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    api
      .getContacts({ q, company, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, company, sort, order, page]);

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
          <h1 className="text-2xl font-bold text-slate-900">Contacts</h1>
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
          placeholder="Search name or email…"
          className="w-64"
        />

        <input
          type="text"
          value={company}
          onChange={(e) => updateParam('company', e.target.value)}
          placeholder="Filter by company…"
          className="filter-input w-48"
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
          <p className="font-medium">Failed to load contacts</p>
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
                <div className="h-4 bg-slate-200 rounded w-32 ml-auto" />
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
        <div className="card overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50">
                <th className="text-left px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Name / Email
                </th>
                <th className="text-left px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Company
                </th>
                <th className="text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Emails
                </th>
                <th className="text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Sent
                </th>
                <th className="text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Received
                </th>
                <th className="text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Last active
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {items.map((contact) => (
                <tr
                  key={contact.id}
                  onClick={() => navigate(`/contacts/${encodeURIComponent(contact.email)}`)}
                  className="table-row-clickable"
                >
                  <td className="px-6 py-4">
                    {contact.name && (
                      <div className="font-medium text-slate-900">{contact.name}</div>
                    )}
                    <div className={`text-slate-500 ${contact.name ? 'text-xs mt-0.5' : 'font-medium'}`}>
                      {contact.email}
                    </div>
                  </td>
                  <td className="px-6 py-4 text-slate-600">
                    {contact.company ?? <span className="text-slate-300">—</span>}
                  </td>
                  <td className="px-6 py-4 text-right font-medium text-slate-700">
                    {contact.email_count.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 text-right text-slate-500">
                    {contact.sent_count.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 text-right text-slate-500">
                    {contact.received_count.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 text-right text-slate-500">
                    {formatDate(contact.last_seen)}
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
