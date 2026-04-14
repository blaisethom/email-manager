import { useEffect, useState, useCallback } from 'react';
import { useParams, Link, useNavigate, useLocation, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { CompanyDetail, CompanyLabel, CompanyThread, DiscussionSummary, ThreadEmail } from '../types';
import Badge from '../components/Badge';
import Breadcrumbs, { extendBreadcrumbs } from '../components/Breadcrumbs';
import Markdown from '../components/Markdown';
import CompanyInsightsTab from '../components/CompanyInsights';
import { formatDate } from '../utils';

function HomepageModal({ companyId, onClose }: { companyId: number; onClose: () => void }) {
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.getCompanyHomepage(companyId)
      .then((data) => setContent(data.content))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [companyId]);

  const handleBackdropClick = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  }, [onClose]);

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={handleBackdropClick}
    >
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[85vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200">
          <h2 className="text-lg font-semibold text-slate-900">Homepage Content</h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-600 transition-colors text-xl leading-none"
          >
            &times;
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-6">
          {loading ? (
            <div className="animate-pulse space-y-3">
              <div className="h-4 bg-slate-200 rounded w-3/4" />
              <div className="h-4 bg-slate-200 rounded w-full" />
              <div className="h-4 bg-slate-200 rounded w-5/6" />
            </div>
          ) : error ? (
            <p className="text-red-600 text-sm">{error}</p>
          ) : (
            <Markdown>{content ?? ''}</Markdown>
          )}
        </div>
      </div>
    </div>
  );
}

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

function DiscussionCard({ disc, linkState }: { disc: DiscussionSummary; linkState?: object }) {
  return (
    <Link
      to={`/discussions/${disc.id}`}
      state={linkState}
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

function ThreadRow({ thread }: { thread: CompanyThread }) {
  const [expanded, setExpanded] = useState(false);
  const [emails, setEmails] = useState<ThreadEmail[] | null>(null);
  const [loading, setLoading] = useState(false);

  const toggle = useCallback(() => {
    if (!expanded && !emails) {
      setLoading(true);
      api.getThreadEmails(thread.thread_id)
        .then((data) => setEmails(data.emails))
        .catch(() => setEmails([]))
        .finally(() => setLoading(false));
    }
    setExpanded(!expanded);
  }, [expanded, emails, thread.thread_id]);

  return (
    <div className="border-b border-slate-100 last:border-0">
      <button
        onClick={toggle}
        className="w-full text-left py-3 px-1 hover:bg-slate-50 transition-colors flex items-start gap-3"
      >
        <span className="text-slate-400 text-xs mt-1 flex-shrink-0">{expanded ? '▼' : '▶'}</span>
        <div className="flex-1 min-w-0">
          <div className="font-medium text-slate-900 truncate text-sm">
            {thread.subject || '(no subject)'}
          </div>
          <div className="flex items-center gap-3 text-xs text-slate-400 mt-0.5">
            <span>{thread.email_count} email{thread.email_count !== 1 ? 's' : ''}</span>
            {thread.first_date && <span>{formatDate(thread.first_date)} — {formatDate(thread.last_date)}</span>}
          </div>
          {thread.summary && !expanded && (
            <p className="text-xs text-slate-500 mt-1 line-clamp-1">{thread.summary}</p>
          )}
        </div>
      </button>
      {expanded && (
        <div className="pl-7 pb-3">
          {loading ? (
            <div className="animate-pulse space-y-2 py-2">
              <div className="h-3 bg-slate-200 rounded w-3/4" />
              <div className="h-3 bg-slate-200 rounded w-full" />
            </div>
          ) : emails && emails.length > 0 ? (
            <div className="space-y-3">
              {emails.map((email) => (
                <div key={email.id} className="bg-slate-50 rounded-lg p-3 text-sm">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="font-medium text-slate-700">
                      {email.from_name || email.from_address}
                    </span>
                    <span className="text-xs text-slate-400">{formatDate(email.date)}</span>
                  </div>
                  {email.subject && (
                    <div className="text-xs text-slate-500 mb-1">{email.subject}</div>
                  )}
                  <pre className="text-xs text-slate-600 whitespace-pre-wrap font-sans leading-relaxed max-h-48 overflow-y-auto">
                    {(email.body_text || '').slice(0, 1500)}
                    {(email.body_text || '').length > 1500 ? '\n...' : ''}
                  </pre>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-slate-400 py-2">No emails found</p>
          )}
        </div>
      )}
    </div>
  );
}

type Tab = 'overview' | 'insights';

export default function CompanyDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [data, setData] = useState<CompanyDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showHomepage, setShowHomepage] = useState(false);
  const activeTab = (searchParams.get('tab') as Tab) || 'overview';
  const setTab = (tab: Tab) => setSearchParams(tab === 'overview' ? {} : { tab });

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
      <div className="p-4 sm:p-8">
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
      <div className="p-4 sm:p-8">
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
    <div className="p-4 sm:p-8 max-w-5xl">
      <Breadcrumbs
        current={data.name}
        defaultTrail={[{ label: 'Companies', path: '/companies' }]}
      />

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
        {data.homepage_fetched_at ? (
          <button
            onClick={() => setShowHomepage(true)}
            className="bg-slate-50 rounded-lg px-4 py-3 text-left hover:bg-slate-100 transition-colors cursor-pointer"
          >
            <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Homepage</div>
            <div className="text-lg font-semibold text-blue-600">Fetched ↗</div>
          </button>
        ) : (
          <StatCard label="Homepage" value="Not fetched" />
        )}
      </div>

      {/* Tab bar */}
      <div className="flex border-b border-slate-200 mb-6">
        {(['overview', 'insights'] as Tab[]).map((tab) => (
          <button
            key={tab}
            onClick={() => setTab(tab)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300'
            }`}
          >
            {tab === 'overview' ? 'Overview' : 'Insights & Provenance'}
          </button>
        ))}
      </div>

      {activeTab === 'overview' && (
        <>
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
                    state={extendBreadcrumbs(location.state, { label: data.name, path: `/companies/${data.id}` })}
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
            <div className="card p-6 mb-6">
              <h2 className="text-base font-semibold text-slate-900 mb-3">
                Discussions
                <span className="ml-2 text-sm font-normal text-slate-500">
                  ({data.discussions.length})
                </span>
              </h2>
              <div className="space-y-2">
                {data.discussions.map((disc) => (
                  <DiscussionCard
                    key={disc.id}
                    disc={disc}
                    linkState={extendBreadcrumbs(location.state, { label: data.name, path: `/companies/${data.id}` })}
                  />
                ))}
              </div>
            </div>
          )}

          {/* Email threads */}
          {data.threads && data.threads.length > 0 && (
            <div className="card p-6">
              <h2 className="text-base font-semibold text-slate-900 mb-3">
                Email Threads
                <span className="ml-2 text-sm font-normal text-slate-500">
                  ({data.threads.length})
                </span>
              </h2>
              <div className="divide-y divide-slate-100">
                {data.threads.map((thread) => (
                  <ThreadRow key={thread.thread_id} thread={thread} />
                ))}
              </div>
            </div>
          )}
        </>
      )}

      {activeTab === 'insights' && (
        <CompanyInsightsTab companyId={data.id} />
      )}

      {showHomepage && data && (
        <HomepageModal companyId={data.id} onClose={() => setShowHomepage(false)} />
      )}
    </div>
  );
}
