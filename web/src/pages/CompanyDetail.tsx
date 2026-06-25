import { useEffect, useState, useCallback } from 'react';
import { useParams, Link, useNavigate, useLocation, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import type { CompanyDetail, CompanyLabel, CompanyThread, DiscussionSummary, ThreadEmail, LabelConfig } from '../types';
import Badge from '../components/Badge';
import Breadcrumbs, { extendBreadcrumbs } from '../components/Breadcrumbs';
import Markdown from '../components/Markdown';
import CompanyInsightsTab from '../components/CompanyInsights';
import { HubSpotCompanyPanel } from '../components/HubSpotPanel';
import { formatDate, formatDateTime } from '../utils';

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

function LabelsSection({
  companyId,
  labels,
  labelConfig,
  onUpdate,
}: {
  companyId: number;
  labels: CompanyLabel[];
  labelConfig: LabelConfig[];
  onUpdate: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set(labels.map(l => l.label)));
  const [reason, setReason] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedReasoning, setExpandedReasoning] = useState<string | null>(null);

  const startEdit = () => {
    setSelected(new Set(labels.map(l => l.label)));
    setReason('');
    setError(null);
    setEditing(true);
  };

  const toggle = (name: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name); else next.add(name);
      return next;
    });
  };

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      await api.saveCompanyLabels(companyId, Array.from(selected), reason.trim() || undefined);
      setEditing(false);
      onUpdate();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  if (editing) {
    return (
      <div className="card p-6 mb-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-base font-semibold text-slate-900">Labels</h2>
          <button onClick={() => setEditing(false)} className="text-sm text-slate-500 hover:text-slate-700">Cancel</button>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 mb-4">
          {labelConfig.map(lc => (
            <label key={lc.name} className="flex items-start gap-2 p-2 rounded-lg border border-slate-200 hover:bg-slate-50 cursor-pointer">
              <input
                type="checkbox"
                checked={selected.has(lc.name)}
                onChange={() => toggle(lc.name)}
                className="mt-0.5 rounded"
              />
              <div>
                <div className="text-sm font-medium text-slate-800">{lc.name}</div>
                {lc.description && (
                  <div className="text-xs text-slate-500 mt-0.5 line-clamp-2">{lc.description}</div>
                )}
              </div>
            </label>
          ))}
        </div>
        <div className="mb-3">
          <input
            type="text"
            value={reason}
            onChange={e => setReason(e.target.value)}
            placeholder="Reason (optional — will be saved as a rule for future AI runs)"
            className="w-full px-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
          />
        </div>
        {error && <p className="text-sm text-red-600 mb-3">{error}</p>}
        <button
          onClick={save}
          disabled={saving}
          className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {saving ? 'Saving…' : 'Save labels'}
        </button>
      </div>
    );
  }

  return (
    <div className="card p-6 mb-6">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-base font-semibold text-slate-900">Labels</h2>
        <button onClick={startEdit} className="text-xs text-slate-400 hover:text-slate-600 transition-colors">
          Edit
        </button>
      </div>
      {labels.length === 0 ? (
        <p className="text-sm text-slate-400">No labels assigned</p>
      ) : (
        <div className="divide-y divide-slate-100">
          {labels.map(item => (
            <div key={item.label} className="py-3 border-b border-slate-100 last:border-0">
              <div className="flex items-center gap-3">
                <Badge label={item.label} variant="label" />
                {item.confidence != null && (
                  <span className="text-sm text-slate-500">{Math.round(item.confidence * 100)}% confidence</span>
                )}
                {item.model_used === 'human' && (
                  <span className="text-xs text-purple-600 font-medium">human</span>
                )}
                {item.reasoning && (
                  <button
                    onClick={() => setExpandedReasoning(expandedReasoning === item.label ? null : item.label)}
                    className="ml-auto text-xs text-slate-400 hover:text-slate-600 transition-colors"
                  >
                    {expandedReasoning === item.label ? 'Hide ↑' : 'Reasoning ↓'}
                  </button>
                )}
              </div>
              {expandedReasoning === item.label && item.reasoning && (
                <p className="mt-2 text-sm text-slate-600 leading-relaxed bg-slate-50 rounded-lg p-3">
                  {item.reasoning}
                </p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function DiscussionFeedbackSection({
  companyId,
  companyDomain,
  onRerun,
}: {
  companyId: number;
  companyDomain: string | null;
  onRerun: (jobId: number) => void;
}) {
  const [feedback, setFeedback] = useState('');
  const [pastRules, setPastRules] = useState<{ id: number; rule_text: string; created_at: string }[]>([]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showPast, setShowPast] = useState(false);

  useEffect(() => {
    api.getCompanyDiscussionFeedback(companyId)
      .then(setPastRules)
      .catch(console.error);
  }, [companyId]);

  const submit = async () => {
    if (!feedback.trim()) return;
    setSaving(true);
    setError(null);
    try {
      await api.addCompanyDiscussionFeedback(companyId, feedback.trim());
      const updated = await api.getCompanyDiscussionFeedback(companyId);
      setPastRules(updated);
      setFeedback('');
      // Trigger a discover_discussions re-run for this company
      if (companyDomain) {
        const job = await api.createJob({
          job_type: 'analyse',
          company: companyDomain,
          stages: ['discover_discussions'],
        });
        onRerun(job.id);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="mt-4 pt-4 border-t border-slate-100">
      <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Discussion Feedback</div>
      <p className="text-xs text-slate-400 mb-3">
        Tell the AI how to restructure discussions for this company. Your feedback will be saved as a rule and the discussions will be re-analysed.
      </p>
      <div className="flex gap-2">
        <input
          type="text"
          value={feedback}
          onChange={e => setFeedback(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && submit()}
          placeholder="e.g. merge the contract threads into one discussion"
          className="flex-1 px-3 py-1.5 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
        />
        <button
          onClick={submit}
          disabled={saving || !feedback.trim()}
          className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors whitespace-nowrap"
        >
          {saving ? 'Saving…' : companyDomain ? 'Save & Re-run' : 'Save'}
        </button>
      </div>
      {error && <p className="text-xs text-red-600 mt-1">{error}</p>}
      {pastRules.length > 0 && (
        <div className="mt-3">
          <button
            onClick={() => setShowPast(!showPast)}
            className="text-xs text-slate-400 hover:text-slate-600"
          >
            {showPast ? '▲ Hide' : '▼ Show'} past feedback ({pastRules.length})
          </button>
          {showPast && (
            <ul className="mt-2 space-y-1">
              {pastRules.map(r => (
                <li key={r.id} className="text-xs text-slate-500 bg-slate-50 rounded px-2 py-1">
                  {r.rule_text}
                </li>
              ))}
            </ul>
          )}
        </div>
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
      <div className="flex items-start gap-3 py-3 px-1 hover:bg-slate-50 transition-colors">
        <button
          onClick={toggle}
          className="flex items-start gap-3 flex-1 min-w-0 text-left"
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
        {thread.discussions.length > 0 && (
          <div className="flex flex-wrap gap-1 justify-end max-w-[40%] flex-shrink-0">
            {thread.discussions.map((d) => (
              <Link
                key={d.id}
                to={`/discussions/${d.id}`}
                title={`${d.title}${d.current_state ? ` · ${d.current_state}` : ''}`}
                className="inline-flex items-center max-w-[14rem] px-1.5 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200 hover:bg-blue-100 transition-colors"
              >
                <span className="truncate">{d.title}</span>
              </Link>
            ))}
          </div>
        )}
      </div>
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
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="min-w-0">
                      <div className="text-xs text-slate-500">
                        <span className="font-medium text-slate-600">From:</span>{' '}
                        {email.from_name ? `${email.from_name} <${email.from_address}>` : email.from_address}
                      </div>
                      {email.to_addresses.length > 0 && (
                        <div className="text-xs text-slate-500">
                          <span className="font-medium text-slate-600">To:</span>{' '}
                          {email.to_addresses.join(', ')}
                        </div>
                      )}
                      {email.cc_addresses.length > 0 && (
                        <div className="text-xs text-slate-500">
                          <span className="font-medium text-slate-600">Cc:</span>{' '}
                          {email.cc_addresses.join(', ')}
                        </div>
                      )}
                    </div>
                    <span className="text-xs text-slate-400 flex-shrink-0">{formatDate(email.date)}</span>
                  </div>
                  {email.subject && (
                    <div className="text-xs font-medium text-slate-700 mb-1 mt-1">{email.subject}</div>
                  )}
                  <pre className="text-xs text-slate-600 whitespace-pre-wrap font-sans leading-relaxed max-h-48 overflow-y-auto mt-2 border-t border-slate-200 pt-2">
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

function StaleBanner({ data }: { data: CompanyDetail }) {
  const navigate = useNavigate();
  const [launching, setLaunching] = useState(false);

  const handleUpdate = async () => {
    if (!data.domain || launching) return;
    setLaunching(true);
    try {
      const job = await api.createJob({
        job_type: 'analyse',
        company: data.domain,
      });
      navigate(`/jobs/${job.id}`);
    } catch (err) {
      console.error('Failed to create job:', err);
      setLaunching(false);
    }
  };

  const lastAnalysed = data.last_analysed_at;
  const newCount = data.new_email_count;

  return (
    <div className="mb-6 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 flex items-center justify-between gap-4">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 w-2 h-2 rounded-full bg-amber-500 flex-shrink-0" />
        <div className="text-sm text-amber-800">
          <span className="font-medium">Analysis is out of date.</span>
          {' '}
          {!lastAnalysed ? (
            <span>This company has never been analysed.</span>
          ) : newCount > 0 ? (
            <span>{newCount} new email{newCount !== 1 ? 's' : ''} since last analysis ({formatDateTime(lastAnalysed)}).</span>
          ) : (
            <span>Last analysed {formatDateTime(lastAnalysed)}.</span>
          )}
        </div>
      </div>
      <button
        onClick={handleUpdate}
        disabled={launching}
        className="flex-shrink-0 px-3 py-1.5 text-sm font-medium text-amber-800 bg-amber-100 border border-amber-300 rounded-lg hover:bg-amber-200 disabled:opacity-50 transition-colors"
      >
        {launching ? 'Launching...' : 'Update Now'}
      </button>
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
  const [labelConfig, setLabelConfig] = useState<LabelConfig[]>([]);
  const activeTab = (searchParams.get('tab') as Tab) || 'overview';
  const setTab = (tab: Tab) => setSearchParams(tab === 'overview' ? {} : { tab });

  const loadCompany = useCallback(() => {
    if (!id) return;
    api
      .getCompany(parseInt(id, 10))
      .then(setData)
      .catch((err: Error) => setError(err.message));
  }, [id]);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    setError(null);
    Promise.all([
      api.getCompany(parseInt(id, 10)),
      api.getMeta(),
    ])
      .then(([company, meta]) => {
        setData(company);
        setLabelConfig(meta.labelConfig ?? []);
      })
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
        <div className="flex items-start justify-between gap-4">
          <div>
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
          </div>
          {Array.isArray(data.sources) && data.sources.length > 0 && (
            <div className="flex flex-wrap gap-1 shrink-0">
              {data.sources.map((s) => (
                <span
                  key={s}
                  className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium border uppercase tracking-wider ${
                    s === 'email' ? 'bg-blue-50 text-blue-700 border-blue-200'
                    : s === 'homepage' ? 'bg-purple-50 text-purple-700 border-purple-200'
                    : 'bg-orange-50 text-orange-700 border-orange-200'
                  }`}
                  title={`Data from ${s}`}
                >
                  {s}
                </span>
              ))}
            </div>
          )}
        </div>
        {data.description && (
          <p className="mt-3 text-slate-700 leading-relaxed max-w-2xl">{data.description}</p>
        )}
      </div>

      {/* Staleness banner */}
      {(data.is_stale || !data.last_analysed_at) && (
        <StaleBanner data={data} />
      )}

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
          {/* Labels — always show if labelConfig is available, even if no labels yet */}
          {(Array.isArray(data.labels) || labelConfig.length > 0) && (
            <LabelsSection
              companyId={data.id}
              labels={(data.labels as CompanyLabel[]) ?? []}
              labelConfig={labelConfig}
              onUpdate={loadCompany}
            />
          )}

          {/* HubSpot CRM data */}
          {data.hubspot && (
            <div className="mb-6">
              <HubSpotCompanyPanel data={data.hubspot} />
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
          {(data.discussions.length > 0 || data.last_analysed_at) && (
            <div className="card p-6 mb-6">
              <h2 className="text-base font-semibold text-slate-900 mb-3">
                Discussions
                {data.discussions.length > 0 && (
                  <span className="ml-2 text-sm font-normal text-slate-500">
                    ({data.discussions.length})
                  </span>
                )}
              </h2>
              {data.discussions.length === 0 && (
                <p className="text-sm text-slate-400 mb-4">No discussions yet.</p>
              )}
              <div className="space-y-2">
                {data.discussions.map((disc) => (
                  <DiscussionCard
                    key={disc.id}
                    disc={disc}
                    linkState={extendBreadcrumbs(location.state, { label: data.name, path: `/companies/${data.id}` })}
                  />
                ))}
              </div>
              <DiscussionFeedbackSection
                companyId={data.id}
                companyDomain={data.domain}
                onRerun={(jobId) => navigate(`/jobs/${jobId}`)}
              />
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
