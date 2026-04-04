import { useEffect, useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { api } from '../api';
import type { CompanyDetail, CompanyLabel, DiscussionSummary } from '../types';
import Badge from '../components/Badge';
import { formatDate } from '../utils';

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="bg-slate-50 rounded-lg px-4 py-3">
      <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">{label}</div>
      <div className="text-lg font-semibold text-slate-900">{value}</div>
    </div>
  );
}

function LabelRow({ item }: { item: CompanyLabel }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="py-3 border-b border-slate-100 last:border-0">
      <div className="flex items-center gap-3">
        <Badge label={item.label} variant="label" />
        {item.confidence != null && (
          <span className="text-sm text-slate-500">{Math.round(item.confidence * 100)}% confidence</span>
        )}
        {item.reasoning && (
          <button
            onClick={() => setExpanded(!expanded)}
            className="ml-auto text-xs text-slate-400 hover:text-slate-600 transition-colors"
          >
            {expanded ? 'Hide reasoning ↑' : 'Show reasoning ↓'}
          </button>
        )}
      </div>
      {expanded && item.reasoning && (
        <p className="mt-2 text-sm text-slate-600 leading-relaxed bg-slate-50 rounded-lg p-3">
          {item.reasoning}
        </p>
      )}
    </div>
  );
}

function DiscussionCard({ disc }: { disc: DiscussionSummary }) {
  return (
    <Link
      to={`/discussions/${disc.id}`}
      className="block p-4 border border-slate-200 rounded-lg hover:border-slate-300 hover:bg-slate-50 transition-colors"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <h4 className="font-medium text-slate-900 truncate">{disc.title}</h4>
          {disc.summary && (
            <p className="text-sm text-slate-500 mt-1 line-clamp-2">{disc.summary}</p>
          )}
        </div>
        <div className="flex flex-wrap gap-1 flex-shrink-0">
          {disc.category && <Badge label={disc.category} variant="category" />}
          {disc.current_state && <Badge label={disc.current_state} variant="state" />}
        </div>
      </div>
      <div className="mt-2 flex items-center gap-3 text-xs text-slate-400">
        {disc.last_seen && <span>Last active {formatDate(disc.last_seen)}</span>}
        {disc.participants.length > 0 && (
          <span>{disc.participants.length} participant{disc.participants.length !== 1 ? 's' : ''}</span>
        )}
      </div>
    </Link>
  );
}

export default function CompanyDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [data, setData] = useState<CompanyDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    setError(null);
    api
      .getCompany(parseInt(id, 10))
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="p-8">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-slate-200 rounded w-24" />
          <div className="h-8 bg-slate-200 rounded w-64" />
          <div className="h-4 bg-slate-200 rounded w-40" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="p-8">
        <button onClick={() => navigate('/companies')} className="btn-secondary mb-6">
          ← Back
        </button>
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">{error ?? 'Company not found'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8 max-w-5xl">
      {/* Back */}
      <button
        onClick={() => navigate('/companies')}
        className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700 transition-colors mb-6"
      >
        ← Back to Companies
      </button>

      {/* Header */}
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-slate-900">{data.name}</h1>
        {data.domain && (
          <p className="text-slate-500 mt-1">
            <a
              href={`https://${data.domain}`}
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-blue-600 transition-colors"
            >
              {data.domain} ↗
            </a>
          </p>
        )}
        {data.description && (
          <p className="mt-3 text-slate-700 leading-relaxed max-w-2xl">{data.description}</p>
        )}
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-8">
        <StatCard label="Emails" value={data.email_count.toLocaleString()} />
        <StatCard label="First seen" value={formatDate(data.first_seen)} />
        <StatCard label="Last active" value={formatDate(data.last_seen)} />
        <StatCard
          label="Homepage"
          value={data.homepage_fetched_at ? 'Fetched' : 'Not fetched'}
        />
      </div>

      {/* Labels */}
      {Array.isArray(data.labels) && data.labels.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">Labels</h2>
          <div className="divide-y divide-slate-100">
            {(data.labels as CompanyLabel[]).map((item) => (
              <LabelRow key={item.label} item={item} />
            ))}
          </div>
        </div>
      )}

      {/* Contacts */}
      {data.contacts.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Contacts
            <span className="ml-2 text-sm font-normal text-slate-500">
              ({data.contacts.length})
            </span>
          </h2>
          <div className="divide-y divide-slate-100">
            {data.contacts.slice(0, 10).map((ct) => (
              <Link
                key={ct.email}
                to={`/contacts/${encodeURIComponent(ct.email)}`}
                className="flex items-center justify-between py-3 hover:bg-slate-50 -mx-2 px-2 rounded transition-colors"
              >
                <div>
                  {ct.name && <div className="font-medium text-slate-900">{ct.name}</div>}
                  <div className="text-sm text-slate-500">{ct.email}</div>
                </div>
                <div className="text-sm text-slate-500 text-right">
                  {ct.email_count.toLocaleString()} emails
                </div>
              </Link>
            ))}
          </div>
          {data.contacts.length > 10 && (
            <div className="mt-3 pt-3 border-t border-slate-100">
              <Link
                to={`/contacts?company=${encodeURIComponent(data.name)}`}
                className="text-sm text-blue-600 hover:text-blue-700"
              >
                View all {data.contacts.length} contacts →
              </Link>
            </div>
          )}
        </div>
      )}

      {/* Discussions */}
      {data.discussions.length > 0 && (
        <div className="card p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Discussions
            <span className="ml-2 text-sm font-normal text-slate-500">
              ({data.discussions.length})
            </span>
          </h2>
          <div className="space-y-2">
            {data.discussions.map((disc) => (
              <DiscussionCard key={disc.id} disc={disc} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
