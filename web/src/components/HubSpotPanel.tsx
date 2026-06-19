import type { HubSpotCompany, HubSpotContact } from '../types';

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-xs text-slate-400 uppercase tracking-wider">{label}</div>
      <div className="text-slate-700 mt-0.5 break-words">{children}</div>
    </div>
  );
}

function ExternalLink({ href, label }: { href: string; label?: string }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-blue-600 hover:underline truncate inline-block max-w-full"
    >
      {label ?? href}
    </a>
  );
}

function formatDateTime(iso: string | null): string {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch {
    return iso;
  }
}

/**
 * HubSpot data card for a company. Renders only the fields that have values
 * so the panel stays compact when the CRM record is sparse.
 */
export function HubSpotCompanyPanel({ data }: { data: HubSpotCompany }) {
  const fields: Array<[string, React.ReactNode]> = [];
  if (data.industry) fields.push(['Industry', data.industry.replace(/_/g, ' ').toLowerCase()]);
  if (data.lifecycle_stage) fields.push(['Lifecycle', data.lifecycle_stage]);
  if (data.type) fields.push(['Type', data.type]);
  if (data.country || data.state || data.city) {
    fields.push(['Location', [data.city, data.state, data.country].filter(Boolean).join(', ')]);
  }
  if (data.num_employees != null) fields.push(['Employees', data.num_employees.toLocaleString()]);
  if (data.annual_revenue != null) fields.push(['Annual revenue', `$${data.annual_revenue.toLocaleString()}`]);
  if (data.founded_year) fields.push(['Founded', data.founded_year]);
  if (data.phone) fields.push(['Phone', data.phone]);
  if (data.owner_id) fields.push(['Owner', <span className="font-mono text-xs">{data.owner_id}</span>]);
  if (data.website) fields.push(['Website', <ExternalLink href={data.website.startsWith('http') ? data.website : `https://${data.website}`} label={data.website} />]);
  if (data.linkedin_url) fields.push(['LinkedIn', <ExternalLink href={data.linkedin_url} />]);
  if (data.twitter_handle) fields.push(['Twitter', `@${data.twitter_handle}`]);

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-900 flex items-center gap-2">
          <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium border uppercase tracking-wider bg-orange-50 text-orange-700 border-orange-200">
            HubSpot
          </span>
          CRM data
        </h2>
        {data.hs_url && (
          <a
            href={data.hs_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-slate-500 hover:text-slate-700 hover:underline"
          >
            Open in HubSpot ↗
          </a>
        )}
      </div>

      {data.description && (
        <p className="text-sm text-slate-600 leading-relaxed mb-4">{data.description}</p>
      )}
      {!data.description && data.about_us && (
        <p className="text-sm text-slate-600 leading-relaxed mb-4">{data.about_us}</p>
      )}

      {fields.length > 0 && (
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 text-sm">
          {fields.map(([label, value], i) => (
            <Field key={i} label={label}>{value}</Field>
          ))}
        </div>
      )}

      {data.hs_updated_at && (
        <div className="text-xs text-slate-400 mt-4 pt-4 border-t border-slate-100">
          Last updated in HubSpot: {formatDateTime(data.hs_updated_at)}
        </div>
      )}
    </div>
  );
}

/**
 * HubSpot data card for a contact.
 */
export function HubSpotContactPanel({ data }: { data: HubSpotContact }) {
  const fields: Array<[string, React.ReactNode]> = [];
  if (data.job_title) fields.push(['Job title', data.job_title]);
  if (data.company_name) fields.push(['Company', data.company_name]);
  if (data.lifecycle_stage) fields.push(['Lifecycle', data.lifecycle_stage]);
  if (data.lead_status) fields.push(['Lead status', data.lead_status]);
  if (data.country || data.state || data.city) {
    fields.push(['Location', [data.city, data.state, data.country].filter(Boolean).join(', ')]);
  }
  if (data.address) fields.push(['Address', data.address]);
  if (data.phone) fields.push(['Phone', data.phone]);
  if (data.industry) fields.push(['Industry', data.industry]);
  if (data.owner_id) fields.push(['Owner', <span className="font-mono text-xs">{data.owner_id}</span>]);
  if (data.website) fields.push(['Website', <ExternalLink href={data.website.startsWith('http') ? data.website : `https://${data.website}`} label={data.website} />]);
  if (data.linkedin_url) fields.push(['LinkedIn', <ExternalLink href={data.linkedin_url} />]);
  if (data.twitter_handle) fields.push(['Twitter', `@${data.twitter_handle}`]);

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-900 flex items-center gap-2">
          <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium border uppercase tracking-wider bg-orange-50 text-orange-700 border-orange-200">
            HubSpot
          </span>
          CRM data
        </h2>
        {data.hs_url && (
          <a
            href={data.hs_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-slate-500 hover:text-slate-700 hover:underline"
          >
            Open in HubSpot ↗
          </a>
        )}
      </div>

      {fields.length > 0 ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 text-sm">
          {fields.map(([label, value], i) => (
            <Field key={i} label={label}>{value}</Field>
          ))}
        </div>
      ) : (
        <p className="text-sm text-slate-400 italic">No additional fields.</p>
      )}

      {data.hs_updated_at && (
        <div className="text-xs text-slate-400 mt-4 pt-4 border-t border-slate-100">
          Last updated in HubSpot: {formatDateTime(data.hs_updated_at)}
        </div>
      )}
    </div>
  );
}
