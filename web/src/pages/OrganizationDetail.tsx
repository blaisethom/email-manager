import { useEffect, useState } from 'react';
import { useParams, useLocation } from 'react-router-dom';
import { api } from '../api';
import type { OrganizationDetail, EntitySource } from '../types';
import { formatDate } from '../utils';

const SOURCE_STYLES: Record<EntitySource, string> = {
  email: 'bg-blue-50 text-blue-700 border-blue-200',
  homepage: 'bg-purple-50 text-purple-700 border-purple-200',
  hubspot: 'bg-orange-50 text-orange-700 border-orange-200',
};

/**
 * Detail page for organizations that have no email-derived row backing them.
 * Mostly HubSpot-only companies — no discussions, no labels, no analysis.
 * Email-backed orgs route to the richer /companies/:id page instead.
 */
export default function OrganizationDetailPage() {
  const { id } = useParams<{ id: string }>();
  const location = useLocation();
  const [org, setOrg] = useState<OrganizationDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    api
      .getOrganization(parseInt(id, 10))
      .then(setOrg)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id]);

  const breadcrumbs = (location.state as { breadcrumbs?: Array<{ label: string; path: string }> } | null)?.breadcrumbs ?? [];

  if (loading) return <div className="p-8 text-slate-500">Loading…</div>;
  if (error) return <div className="p-8 text-red-600">Error: {error}</div>;
  if (!org) return <div className="p-8 text-slate-500">Not found.</div>;

  return (
    <div className="p-4 sm:p-8 max-w-5xl">
      {breadcrumbs.length > 0 && (
        <div className="text-sm text-slate-500 mb-4">
          {breadcrumbs.map((b, i) => (
            <span key={i}>
              <a href={b.path} className="hover:text-slate-700 hover:underline">{b.label}</a>
              {' / '}
            </span>
          ))}
          <span className="text-slate-700">{org.name ?? org.domain ?? `#${org.org_id}`}</span>
        </div>
      )}

      <div className="card p-6 mb-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">{org.name ?? org.domain ?? `Organization #${org.org_id}`}</h1>
            {org.domain && (
              <a
                href={`https://${org.domain}`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm text-blue-600 hover:underline mt-1 inline-block"
              >
                {org.domain}
              </a>
            )}
            {org.description && (
              <p className="text-sm text-slate-600 mt-3 leading-relaxed">{org.description}</p>
            )}
          </div>
          <div className="flex flex-col items-end gap-2 shrink-0">
            <div className="flex flex-wrap gap-1">
              {org.sources.map((s) => (
                <span
                  key={s}
                  className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border uppercase tracking-wider ${SOURCE_STYLES[s]}`}
                >
                  {s}
                </span>
              ))}
            </div>
            {org.hubspot_url && (
              <a
                href={org.hubspot_url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-slate-500 hover:text-slate-700 hover:underline"
              >
                Open in HubSpot ↗
              </a>
            )}
          </div>
        </div>

        {/* HubSpot CRM fields */}
        {org.sources.includes('hubspot') && (
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 mt-6 pt-6 border-t border-slate-100 text-sm">
            {org.industry && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Industry</div>
                <div className="text-slate-700 mt-0.5">{org.industry.replace(/_/g, ' ').toLowerCase()}</div>
              </div>
            )}
            {org.lifecycle_stage && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Lifecycle</div>
                <div className="text-slate-700 mt-0.5">{org.lifecycle_stage}</div>
              </div>
            )}
            {org.country && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Location</div>
                <div className="text-slate-700 mt-0.5">
                  {[org.city, org.state, org.country].filter(Boolean).join(', ')}
                </div>
              </div>
            )}
            {org.num_employees != null && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Employees</div>
                <div className="text-slate-700 mt-0.5">{org.num_employees.toLocaleString()}</div>
              </div>
            )}
            {org.annual_revenue != null && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Revenue</div>
                <div className="text-slate-700 mt-0.5">${org.annual_revenue.toLocaleString()}</div>
              </div>
            )}
            {org.founded_year && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Founded</div>
                <div className="text-slate-700 mt-0.5">{org.founded_year}</div>
              </div>
            )}
            {org.phone && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Phone</div>
                <div className="text-slate-700 mt-0.5">{org.phone}</div>
              </div>
            )}
            {org.linkedin_url && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">LinkedIn</div>
                <a href={org.linkedin_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline mt-0.5 block truncate">
                  {org.linkedin_url}
                </a>
              </div>
            )}
            {org.twitter_handle && (
              <div>
                <div className="text-xs text-slate-400 uppercase tracking-wider">Twitter</div>
                <div className="text-slate-700 mt-0.5">@{org.twitter_handle}</div>
              </div>
            )}
          </div>
        )}

        {/* Email-derived activity */}
        {org.sources.includes('email') && (
          <div className="grid grid-cols-3 gap-4 mt-6 pt-6 border-t border-slate-100 text-sm">
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Emails</div>
              <div className="text-slate-700 mt-0.5">{org.email_count.toLocaleString()}</div>
            </div>
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">First seen</div>
              <div className="text-slate-700 mt-0.5">{formatDate(org.first_seen)}</div>
            </div>
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Last seen</div>
              <div className="text-slate-700 mt-0.5">{formatDate(org.last_seen)}</div>
            </div>
          </div>
        )}
      </div>

      {/* Identity provenance */}
      <div className="card p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Identities</h2>
        <p className="text-sm text-slate-500 mb-3">
          Source rows linked to this organization. Manual merges set <code className="text-xs bg-slate-100 px-1 py-0.5 rounded">is_manual=1</code>.
        </p>
        <table className="w-full text-sm">
          <thead className="text-xs text-slate-500 uppercase tracking-wider">
            <tr className="border-b border-slate-200">
              <th className="text-left py-2">Source</th>
              <th className="text-left py-2">Source ID</th>
              <th className="text-left py-2">Match key</th>
              <th className="text-right py-2">Confidence</th>
              <th className="text-right py-2">Manual</th>
            </tr>
          </thead>
          <tbody>
            {org.identities.map((ident, i) => (
              <tr key={i} className="border-b border-slate-100 last:border-0">
                <td className="py-2">
                  <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium border uppercase ${SOURCE_STYLES[ident.source]}`}>
                    {ident.source}
                  </span>
                </td>
                <td className="py-2 font-mono text-xs text-slate-600">{ident.source_id}</td>
                <td className="py-2 text-slate-600">{ident.match_key ?? '—'}</td>
                <td className="py-2 text-right text-slate-600">{ident.confidence?.toFixed(2) ?? '—'}</td>
                <td className="py-2 text-right">{ident.is_manual ? '✓' : ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
