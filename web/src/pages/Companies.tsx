import { useEffect, useState, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { Organization, EntitySource } from '../types';
import Badge from '../components/Badge';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate } from '../utils';

const LIMIT = 25;

const SOURCE_STYLES: Record<EntitySource, string> = {
  email: 'bg-blue-50 text-blue-700 border-blue-200',
  homepage: 'bg-purple-50 text-purple-700 border-purple-200',
  hubspot: 'bg-orange-50 text-orange-700 border-orange-200',
};

function SourceBadges({ sources }: { sources: EntitySource[] }) {
  if (!sources?.length) return null;
  return (
    <div className="flex flex-wrap gap-1">
      {sources.map((s) => (
        <span
          key={s}
          className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium border uppercase tracking-wider ${SOURCE_STYLES[s]}`}
          title={`Data from ${s}`}
        >
          {s}
        </span>
      ))}
    </div>
  );
}

function StatusPill({ company }: { company: Organization }) {
  // HubSpot-only companies have no email traffic, so analysis status doesn't apply.
  if (!company.sources.includes('email')) {
    return (
      <span
        className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium bg-slate-50 text-slate-600 border border-slate-200"
        title="No email activity yet"
      >
        no email
      </span>
    );
  }
  if (!company.last_analysed_at) {
    return (
      <span
        className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium bg-red-50 text-red-700 border border-red-200"
        title="Never analysed"
      >
        <span className="w-1.5 h-1.5 rounded-full bg-red-500" />
        never analysed
      </span>
    );
  }
  if (company.is_stale) {
    return (
      <span
        className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200"
        title="New emails since last analysis"
      >
        <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
        stale
      </span>
    );
  }
  return (
    <span
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium bg-green-50 text-green-700 border border-green-200"
      title="Analysis is up to date"
    >
      <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
      up to date
    </span>
  );
}

export default function CompaniesPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<Organization[]>([]);
  const [total, setTotal] = useState(0);
  const [allLabels, setAllLabels] = useState<string[]>([]);
  const [staleCount, setStaleCount] = useState(0);
  const [upToDateCount, setUpToDateCount] = useState(0);
  const [neverAnalysedCount, setNeverAnalysedCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const q = searchParams.get('q') ?? '';
  const label = searchParams.get('label') ?? '';
  const stale = searchParams.get('stale') ?? '';
  const source = searchParams.get('source') ?? '';
  const sort = searchParams.get('sort') ?? 'email_count';
  const order = searchParams.get('order') ?? 'desc';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    api
      .getOrganizations({ q, label, stale, source, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
        setAllLabels(data.labels);
        setStaleCount(data.stale_count);
        setUpToDateCount(data.up_to_date_count);
        setNeverAnalysedCount(data.never_analysed_count);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, label, stale, source, sort, order, page]);

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
            <p className="text-sm text-slate-500 mt-0.5">
              {total.toLocaleString()} total
              {!stale && (
                <span className="ml-2">
                  {upToDateCount > 0 && <span className="text-green-600">{upToDateCount} up to date</span>}
                  {staleCount > 0 && <span className="text-amber-600 ml-1.5">{staleCount} stale</span>}
                  {neverAnalysedCount > 0 && <span className="text-red-600 ml-1.5">{neverAnalysedCount} never analysed</span>}
                </span>
              )}
            </p>
          )}
        </div>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap gap-3 mb-6">
        <SearchBar
          value={q}
          onChange={(v) => updateParam('q', v)}
          placeholder="Search name or domain..."
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
          <option value="name:asc">Name A-Z</option>
          <option value="name:desc">Name Z-A</option>
          <option value="last_seen:desc">Recently active</option>
          <option value="last_seen:asc">Least recent</option>
        </select>

        <select
          value={stale}
          onChange={(e) => updateParam('stale', e.target.value)}
          className="filter-input"
        >
          <option value="">All statuses</option>
          <option value="1">Stale only</option>
          <option value="0">Up to date only</option>
          <option value="never">Never analysed</option>
        </select>

        <select
          value={source}
          onChange={(e) => updateParam('source', e.target.value)}
          className="filter-input"
        >
          <option value="">All sources</option>
          <option value="email_only">Email only</option>
          <option value="hubspot_only">HubSpot only</option>
          <option value="both">In both</option>
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
                <th className="hidden lg:table-cell text-left px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Sources
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
                <th className="hidden md:table-cell text-right px-6 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                  Analysed
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {items.map((company) => {
                // Email-backed orgs route to the legacy CompanyDetail page (rich
                // discussions/threads/insights). HubSpot-only orgs route to the
                // minimal organization detail page.
                const target = company.email_company_id
                  ? `/companies/${company.email_company_id}`
                  : `/organizations/${company.org_id}`;
                return (
                  <tr
                    key={company.org_id}
                    onClick={() => navigate(target, { state: { breadcrumbs: [{ label: 'Companies', path: '/companies' }] } })}
                    className="table-row-clickable"
                  >
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-2">
                        <div className="font-medium text-slate-900">{company.name ?? company.domain ?? '—'}</div>
                        <StatusPill company={company} />
                      </div>
                      {company.domain && (
                        <div className="text-xs text-slate-500 mt-0.5">{company.domain}</div>
                      )}
                      {company.industry && (
                        <div className="text-xs text-slate-400 mt-0.5">
                          {company.industry.replace(/_/g, ' ').toLowerCase()}
                          {company.country ? ` · ${company.country}` : ''}
                        </div>
                      )}
                    </td>
                    <td className="hidden lg:table-cell px-6 py-4">
                      <SourceBadges sources={company.sources} />
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
                    <td className="hidden md:table-cell px-6 py-4 text-right text-slate-500">
                      {company.last_analysed_at ? formatDate(company.last_analysed_at) : (
                        <span className="text-slate-400 italic">never</span>
                      )}
                    </td>
                  </tr>
                );
              })}
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
