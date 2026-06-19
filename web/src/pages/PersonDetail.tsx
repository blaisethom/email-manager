import { useEffect, useState } from 'react';
import { useParams, useLocation } from 'react-router-dom';
import { api } from '../api';
import type { PersonDetail } from '../types';

const SOURCE_STYLES: Record<'email' | 'hubspot', string> = {
  email: 'bg-blue-50 text-blue-700 border-blue-200',
  hubspot: 'bg-orange-50 text-orange-700 border-orange-200',
};

/**
 * Detail page for HubSpot-only people (or any person without an email
 * activity row). Email-backed people route to the legacy /contacts/:email
 * page.
 */
export default function PersonDetailPage() {
  const { id } = useParams<{ id: string }>();
  const location = useLocation();
  const [person, setPerson] = useState<PersonDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    api
      .getPerson(parseInt(id, 10))
      .then(setPerson)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id]);

  const breadcrumbs = (location.state as { breadcrumbs?: Array<{ label: string; path: string }> } | null)?.breadcrumbs ?? [];

  if (loading) return <div className="p-8 text-slate-500">Loading…</div>;
  if (error) return <div className="p-8 text-red-600">Error: {error}</div>;
  if (!person) return <div className="p-8 text-slate-500">Not found.</div>;

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
          <span className="text-slate-700">{person.name ?? person.email ?? `#${person.person_id}`}</span>
        </div>
      )}

      <div className="card p-6 mb-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">{person.name ?? person.email ?? `Person #${person.person_id}`}</h1>
            {person.email && (
              <a href={`mailto:${person.email}`} className="text-sm text-blue-600 hover:underline mt-1 inline-block">
                {person.email}
              </a>
            )}
            {person.job_title && (
              <p className="text-sm text-slate-600 mt-1">
                {person.job_title}{person.company_name ? ` · ${person.company_name}` : ''}
              </p>
            )}
          </div>
          <div className="flex flex-col items-end gap-2 shrink-0">
            <div className="flex flex-wrap gap-1">
              {person.sources.map((s) => (
                <span
                  key={s}
                  className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border uppercase tracking-wider ${SOURCE_STYLES[s]}`}
                >
                  {s}
                </span>
              ))}
            </div>
            {person.hubspot_url && (
              <a
                href={person.hubspot_url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-slate-500 hover:text-slate-700 hover:underline"
              >
                Open in HubSpot ↗
              </a>
            )}
          </div>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 mt-6 pt-6 border-t border-slate-100 text-sm">
          {person.lifecycle_stage && (
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Lifecycle</div>
              <div className="text-slate-700 mt-0.5">{person.lifecycle_stage}</div>
            </div>
          )}
          {person.lead_status && (
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Lead status</div>
              <div className="text-slate-700 mt-0.5">{person.lead_status}</div>
            </div>
          )}
          {person.country && (
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Location</div>
              <div className="text-slate-700 mt-0.5">
                {[person.city, person.state, person.country].filter(Boolean).join(', ')}
              </div>
            </div>
          )}
          {person.phone && (
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Phone</div>
              <div className="text-slate-700 mt-0.5">{person.phone}</div>
            </div>
          )}
          {person.linkedin_url && (
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">LinkedIn</div>
              <a href={person.linkedin_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline mt-0.5 block truncate">
                {person.linkedin_url}
              </a>
            </div>
          )}
          {person.twitter_handle && (
            <div>
              <div className="text-xs text-slate-400 uppercase tracking-wider">Twitter</div>
              <div className="text-slate-700 mt-0.5">@{person.twitter_handle}</div>
            </div>
          )}
        </div>
      </div>

      <div className="card p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Identities</h2>
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
            {person.identities.map((ident, i) => (
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
