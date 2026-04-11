import { useEffect, useState, useCallback } from 'react';
import { useParams, useNavigate, useLocation, Link } from 'react-router-dom';
import { api } from '../api';
import type { Discussion, DiscussionDetail, DiscussionAction, StateHistoryEntry, Thread, ThreadEmail, CalendarEvent, EventLedgerEntry, Milestone, ProposedAction } from '../types';
import Badge from '../components/Badge';
import Breadcrumbs, { extendBreadcrumbs } from '../components/Breadcrumbs';
import Markdown from '../components/Markdown';
import { formatDate, formatDateTime } from '../utils';

function StateTimeline({ history }: { history: StateHistoryEntry[] }) {
  if (history.length === 0) return null;

  return (
    <div className="relative">
      <div className="absolute left-3.5 top-0 bottom-0 w-px bg-slate-200" />
      <div className="space-y-4">
        {history.map((entry, i) => (
          <div key={entry.id} className="relative flex gap-4 pl-9">
            {/* Dot */}
            <div
              className={`absolute left-0 w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 border-2 ${
                i === history.length - 1
                  ? 'bg-blue-600 border-blue-600'
                  : 'bg-white border-slate-300'
              }`}
            >
              <div
                className={`w-2 h-2 rounded-full ${
                  i === history.length - 1 ? 'bg-white' : 'bg-slate-400'
                }`}
              />
            </div>

            <div className="flex-1 pb-4">
              <div className="flex items-center gap-3 mb-1">
                <Badge label={entry.state} variant="state" />
                {entry.entered_at && (
                  <span className="text-xs text-slate-400">{formatDateTime(entry.entered_at)}</span>
                )}
              </div>
              {entry.reasoning && (
                <p className="text-sm text-slate-600 leading-relaxed">{entry.reasoning}</p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ActionRow({ action, linkState }: { action: DiscussionAction; linkState?: object }) {
  return (
    <div className="py-3 border-b border-slate-100 last:border-0">
      <div className="flex items-start gap-3">
        <div className={`mt-0.5 w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 ${
          action.status === 'done'
            ? 'bg-green-100 text-green-600'
            : 'bg-amber-100 text-amber-600'
        }`}>
          {action.status === 'done' ? (
            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none"
              stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="20 6 9 17 4 12" />
            </svg>
          ) : (
            <div className="w-2 h-2 rounded-full bg-amber-500" />
          )}
        </div>
        <div className="flex-1 min-w-0">
          <p className={`text-sm leading-relaxed ${action.status === 'done' ? 'text-slate-500 line-through' : 'text-slate-900'}`}>
            {action.description}
          </p>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 mt-1 text-xs text-slate-400">
            {action.assignee_emails.length > 0 && (
              <span className="flex flex-wrap gap-x-2">
                {action.assignee_emails.map((email) => (
                  <Link
                    key={email}
                    to={`/contacts/${encodeURIComponent(email)}`}
                    state={linkState}
                    className="text-blue-600 hover:underline"
                  >
                    {email}
                  </Link>
                ))}
              </span>
            )}
            {action.target_date && (
              <span>Due {formatDate(action.target_date)}</span>
            )}
            {action.completed_date && (
              <span className="text-green-600">Completed {formatDate(action.completed_date)}</span>
            )}
            {action.source_date && (
              <span>From {formatDate(action.source_date)}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function MilestoneTracker({ milestones }: { milestones: Milestone[] }) {
  if (milestones.length === 0) return null;

  const achieved = milestones.filter((m) => m.achieved);
  const pending = milestones.filter((m) => !m.achieved);

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <div className="flex items-center gap-1.5 text-sm text-slate-500">
          <span className="font-semibold text-emerald-600">{achieved.length}</span>
          <span>of</span>
          <span className="font-semibold">{milestones.length}</span>
          <span>milestones</span>
        </div>
        {/* Progress bar */}
        <div className="flex-1 h-2 bg-slate-100 rounded-full overflow-hidden">
          <div
            className="h-full bg-emerald-500 rounded-full transition-all duration-500"
            style={{ width: `${(achieved.length / milestones.length) * 100}%` }}
          />
        </div>
      </div>

      <div className="space-y-2">
        {achieved.map((m) => (
          <div key={m.name} className="flex items-center gap-3 py-1.5">
            <div className="w-6 h-6 rounded-full bg-emerald-100 text-emerald-600 flex items-center justify-center flex-shrink-0">
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none"
                stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="20 6 9 17 4 12" />
              </svg>
            </div>
            <div className="flex-1 min-w-0">
              <span className="text-sm font-medium text-slate-900">
                {m.name.replace(/_/g, ' ')}
              </span>
            </div>
            {m.achieved_date && (
              <span className="text-xs text-slate-400 flex-shrink-0">{formatDate(m.achieved_date)}</span>
            )}
            {m.confidence != null && m.confidence > 0 && (
              <span className="text-xs text-slate-400 flex-shrink-0">
                {Math.round(m.confidence * 100)}%
              </span>
            )}
          </div>
        ))}
        {pending.map((m) => (
          <div key={m.name} className="flex items-center gap-3 py-1.5 opacity-50">
            <div className="w-6 h-6 rounded-full border-2 border-slate-200 flex items-center justify-center flex-shrink-0">
              <div className="w-2 h-2 rounded-full bg-slate-300" />
            </div>
            <span className="text-sm text-slate-500">
              {m.name.replace(/_/g, ' ')}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

const PRIORITY_STYLES: Record<string, { bg: string; icon: string; label: string }> = {
  high: { bg: 'bg-red-50 border-red-200', icon: 'text-red-500', label: 'High' },
  medium: { bg: 'bg-amber-50 border-amber-200', icon: 'text-amber-500', label: 'Medium' },
  low: { bg: 'bg-slate-50 border-slate-200', icon: 'text-slate-400', label: 'Low' },
};

function ProposedActionsList({ actions }: { actions: ProposedAction[] }) {
  if (actions.length === 0) return null;

  return (
    <div className="space-y-3">
      {actions.map((pa) => {
        const style = PRIORITY_STYLES[pa.priority] ?? PRIORITY_STYLES.medium;
        const isWait = !!pa.wait_until;

        return (
          <div key={pa.id} className={`rounded-lg border p-4 ${style.bg}`}>
            <div className="flex items-start gap-3">
              {/* Priority icon */}
              <div className={`mt-0.5 flex-shrink-0 ${style.icon}`}>
                {isWait ? (
                  <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none"
                    stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <circle cx="12" cy="12" r="10" />
                    <polyline points="12 6 12 12 16 14" />
                  </svg>
                ) : (
                  <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none"
                    stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
                  </svg>
                )}
              </div>

              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-slate-900 leading-relaxed">
                  {pa.action}
                </p>
                {pa.reasoning && (
                  <p className="text-xs text-slate-500 mt-1 leading-relaxed">{pa.reasoning}</p>
                )}
                <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 mt-2 text-xs text-slate-400">
                  <span className={`font-medium ${style.icon}`}>{style.label} priority</span>
                  {pa.wait_until && (
                    <span>Wait until {formatDate(pa.wait_until)}</span>
                  )}
                  {pa.assignee && (
                    <Link
                      to={`/contacts/${encodeURIComponent(pa.assignee)}`}
                      className="text-blue-600 hover:underline"
                    >
                      {pa.assignee}
                    </Link>
                  )}
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

const DOMAIN_COLORS: Record<string, string> = {
  investment: 'bg-blue-100 text-blue-700',
  'investor-relations': 'bg-indigo-100 text-indigo-700',
  'pharma-deal': 'bg-purple-100 text-purple-700',
  scheduling: 'bg-sky-100 text-sky-700',
  'contract-negotiation': 'bg-amber-100 text-amber-700',
  partnership: 'bg-teal-100 text-teal-700',
  hiring: 'bg-pink-100 text-pink-700',
  'internal-decision': 'bg-slate-100 text-slate-700',
  'board-discussion': 'bg-orange-100 text-orange-700',
  'vendor-selection': 'bg-lime-100 text-lime-700',
  'support-issue': 'bg-red-100 text-red-700',
  newsletter: 'bg-gray-100 text-gray-600',
  other: 'bg-gray-100 text-gray-600',
};

function EventTimeline({ events, onThreadClick }: { events: EventLedgerEntry[]; onThreadClick: (threadId: string, sourceEmailId?: string | null) => void }) {
  if (events.length === 0) return null;

  // Group events by date
  const byDate: Record<string, EventLedgerEntry[]> = {};
  for (const ev of events) {
    const date = ev.event_date ?? 'Unknown';
    if (!byDate[date]) byDate[date] = [];
    byDate[date].push(ev);
  }

  return (
    <div className="relative">
      <div className="absolute left-3.5 top-0 bottom-0 w-px bg-slate-200" />
      <div className="space-y-4">
        {Object.entries(byDate).map(([date, dateEvents]) => (
          <div key={date} className="relative pl-9">
            {/* Date dot */}
            <div className="absolute left-0 w-7 h-7 rounded-full bg-white border-2 border-slate-300 flex items-center justify-center flex-shrink-0">
              <div className="w-2 h-2 rounded-full bg-slate-400" />
            </div>

            <div className="pb-2">
              <div className="text-xs font-medium text-slate-500 mb-2">{formatDate(date)}</div>
              <div className="space-y-1.5">
                {dateEvents.map((ev) => (
                  <div key={ev.id} className="flex items-start gap-2 group">
                    <span className={`inline-flex px-1.5 py-0.5 rounded text-xs font-medium flex-shrink-0 ${DOMAIN_COLORS[ev.domain] ?? DOMAIN_COLORS.other}`}>
                      {ev.type.replace(/_/g, ' ')}
                    </span>
                    <span className="text-sm text-slate-600 leading-relaxed flex-1">
                      {ev.detail || `${ev.actor ?? ''} ${ev.target ? `→ ${ev.target}` : ''}`}
                    </span>
                    {ev.confidence != null && ev.confidence < 0.7 && (
                      <span className="text-xs text-amber-500 flex-shrink-0">
                        {Math.round(ev.confidence * 100)}%
                      </span>
                    )}
                    {ev.thread_id && (
                      <button
                        onClick={() => onThreadClick(ev.thread_id!, ev.source_email_id)}
                        className="text-xs text-blue-500 hover:text-blue-700 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="View source email"
                      >
                        view email
                      </button>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function splitQuotedText(body: string): { fresh: string; quoted: string } {
  const lines = body.split('\n');
  let splitIndex = lines.length;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // "On <date>, <person> wrote:" (Gmail-style)
    if (/^On .{10,80} wrote:\s*$/.test(line)) {
      splitIndex = i;
      break;
    }

    // "-----Original Message-----" (Outlook)
    if (/^-{2,}\s*Original Message\s*-{2,}$/i.test(line)) {
      splitIndex = i;
      break;
    }

    // "From: ... Sent: ..." block after a blank line
    if (/^From:\s+\S+/.test(line) && i > 0 && lines[i - 1].trim() === '') {
      splitIndex = i;
      break;
    }

    // Block of consecutive ">" quoted lines (3+)
    if (line.startsWith('>')) {
      let runEnd = i;
      while (runEnd < lines.length && lines[runEnd].trim().startsWith('>')) runEnd++;
      if (runEnd - i >= 3) {
        splitIndex = i;
        break;
      }
    }
  }

  const fresh = lines.slice(0, splitIndex).join('\n').trimEnd();
  const quoted = lines.slice(splitIndex).join('\n').trimStart();
  return { fresh, quoted };
}

function EmailBody({ body }: { body: string }) {
  const { fresh, quoted } = splitQuotedText(body);
  const [showQuoted, setShowQuoted] = useState(false);

  if (!quoted) {
    return <Markdown>{fresh}</Markdown>;
  }

  return (
    <div>
      <Markdown>{fresh}</Markdown>
      <button
        onClick={() => setShowQuoted(!showQuoted)}
        className="mt-2 flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-600 transition-colors"
      >
        <span className="inline-flex items-center justify-center w-5 h-5 border border-slate-300 rounded text-[10px]">
          {showQuoted ? '▾' : '···'}
        </span>
        <span>{showQuoted ? 'Hide quoted text' : 'Show quoted text'}</span>
      </button>
      {showQuoted && (
        <div className="mt-2 pl-3 border-l-2 border-slate-200 text-slate-400">
          <Markdown>{quoted}</Markdown>
        </div>
      )}
    </div>
  );
}

function ThreadModal({ thread, onClose, highlightMessageId }: { thread: Thread; onClose: () => void; highlightMessageId?: string | null }) {
  const [emails, setEmails] = useState<ThreadEmail[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());

  useEffect(() => {
    api.getThreadEmails(thread.thread_id)
      .then((data) => {
        setEmails(data.emails);
        // Auto-expand the highlighted email, or the last one
        const target = highlightMessageId
          ? data.emails.find((e) => e.message_id === highlightMessageId)
          : data.emails[data.emails.length - 1];
        if (target) {
          setExpandedIds(new Set([target.id]));
          // Scroll to the highlighted email after render
          if (highlightMessageId) {
            setTimeout(() => {
              document.getElementById(`email-${target.id}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }, 100);
          }
        }
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [thread.thread_id, highlightMessageId]);

  const handleBackdropClick = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  }, [onClose]);

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  function toggleEmail(id: number) {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={handleBackdropClick}
    >
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[85vh] flex flex-col">
        {/* Header */}
        <div className="flex items-start justify-between gap-4 px-6 py-4 border-b border-slate-200">
          <div className="min-w-0">
            <h2 className="text-lg font-semibold text-slate-900 leading-snug">
              {thread.subject ?? '(no subject)'}
            </h2>
            <div className="flex items-center gap-3 mt-1 text-xs text-slate-400">
              <span>{thread.email_count} email{thread.email_count !== 1 ? 's' : ''}</span>
              {thread.first_date && <span>{formatDate(thread.first_date)}</span>}
              {thread.last_date && thread.last_date !== thread.first_date && (
                <span>– {formatDate(thread.last_date)}</span>
              )}
            </div>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-600 transition-colors text-xl leading-none flex-shrink-0 mt-1"
          >
            &times;
          </button>
        </div>

        {/* Summary */}
        {thread.summary && (
          <div className="px-6 py-3 bg-slate-50 border-b border-slate-200 text-sm text-slate-600 leading-relaxed">
            {thread.summary}
          </div>
        )}

        {/* Emails */}
        <div className="flex-1 overflow-y-auto p-6 space-y-3">
          {loading ? (
            <div className="animate-pulse space-y-4">
              {Array.from({ length: 3 }).map((_, i) => (
                <div key={i} className="space-y-2">
                  <div className="h-4 bg-slate-200 rounded w-1/3" />
                  <div className="h-4 bg-slate-200 rounded w-full" />
                  <div className="h-4 bg-slate-200 rounded w-5/6" />
                </div>
              ))}
            </div>
          ) : error ? (
            <p className="text-red-600 text-sm">{error}</p>
          ) : emails.length === 0 ? (
            <p className="text-sm text-slate-400">No emails found for this thread.</p>
          ) : (
            emails.map((email) => {
              const isExpanded = expandedIds.has(email.id);
              return (
                <div key={email.id} id={`email-${email.id}`} className={`border rounded-lg overflow-hidden ${highlightMessageId && email.message_id === highlightMessageId ? 'border-blue-400 ring-2 ring-blue-100' : 'border-slate-200'}`}>
                  <button
                    onClick={() => toggleEmail(email.id)}
                    className="w-full text-left px-4 py-3 hover:bg-slate-50 transition-colors flex items-start gap-3"
                  >
                    <span className="text-xs text-slate-400 mt-0.5 flex-shrink-0 select-none">
                      {isExpanded ? '▾' : '▸'}
                    </span>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-baseline gap-2">
                        <span className="font-medium text-slate-900 text-sm truncate">
                          {email.from_name ?? email.from_address}
                        </span>
                        {email.from_name && (
                          <span className="text-xs text-slate-400 truncate">&lt;{email.from_address}&gt;</span>
                        )}
                      </div>
                      {!isExpanded && email.body_text && (
                        <p className="text-xs text-slate-400 mt-0.5 truncate">{email.body_text.slice(0, 120)}</p>
                      )}
                    </div>
                    <span className="text-xs text-slate-400 flex-shrink-0 whitespace-nowrap">
                      {formatDateTime(email.date)}
                    </span>
                  </button>
                  {isExpanded && (
                    <div className="px-4 pb-4 border-t border-slate-100">
                      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-400 py-2">
                        {email.to_addresses.length > 0 && (
                          <span>To: {email.to_addresses.join(', ')}</span>
                        )}
                        {email.cc_addresses.length > 0 && (
                          <span>Cc: {email.cc_addresses.join(', ')}</span>
                        )}
                      </div>
                      <div className="mt-1">
                        {email.body_text
                          ? <EmailBody body={email.body_text} />
                          : <p className="text-sm text-slate-400 italic">(no text content)</p>
                        }
                      </div>
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}

function ThreadRow({ thread, onClick }: { thread: Thread; onClick: () => void }) {
  return (
    <div
      className="py-3 border-b border-slate-100 last:border-0 cursor-pointer hover:bg-slate-50 -mx-2 px-2 rounded transition-colors"
      onClick={onClick}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="font-medium text-slate-900 truncate">
            {thread.subject ?? '(no subject)'}
          </div>
          {thread.summary && (
            <p className="text-sm text-slate-500 mt-0.5 line-clamp-2">{thread.summary}</p>
          )}
          <div className="flex items-center gap-3 mt-1 text-xs text-slate-400">
            {thread.first_date && <span>{formatDate(thread.first_date)}</span>}
            {thread.last_date && thread.last_date !== thread.first_date && (
              <span>– {formatDate(thread.last_date)}</span>
            )}
            {thread.participants.length > 0 && (
              <span>{thread.participants.length} participants</span>
            )}
          </div>
        </div>
        <span className="flex-shrink-0 text-sm font-medium text-slate-600 bg-slate-100 px-2 py-0.5 rounded">
          {thread.email_count} email{thread.email_count !== 1 ? 's' : ''}
        </span>
      </div>
    </div>
  );
}

export default function DiscussionDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const [data, setData] = useState<DiscussionDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDoneActions, setShowDoneActions] = useState(false);
  const [selectedThread, setSelectedThread] = useState<Thread | null>(null);
  const [highlightMessageId, setHighlightMessageId] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    setError(null);
    api
      .getDiscussion(parseInt(id, 10))
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="p-4 sm:p-8">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-slate-200 rounded w-24" />
          <div className="h-8 bg-slate-200 rounded w-2/3" />
          <div className="h-4 bg-slate-200 rounded w-48" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="p-4 sm:p-8">
        <button onClick={() => navigate('/discussions')} className="btn-secondary mb-6">
          ← Back
        </button>
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">{error ?? 'Discussion not found'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-8 max-w-4xl">
      <Breadcrumbs
        current={data.title}
        defaultTrail={[{ label: 'Discussions', path: '/discussions' }]}
      />

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-start gap-3 mb-2">
          <h1 className="text-3xl font-bold text-slate-900 flex-1">{data.title}</h1>
        </div>

        <div className="flex flex-wrap items-center gap-2 mb-3">
          {data.current_state && <Badge label={data.current_state} variant="state" />}
          {data.category && <Badge label={data.category} variant="category" />}
          {data.parent_id && (
            <Link
              to={`/discussions/${data.parent_id}`}
              state={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })}
              className="text-xs text-slate-500 hover:text-blue-600 transition-colors"
            >
              sub-discussion →
            </Link>
          )}
        </div>

        {data.company_name && data.company_id && (
          <Link
            to={`/companies/${data.company_id}`}
            state={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })}
            className="text-blue-600 hover:text-blue-700 font-medium"
          >
            {data.company_name} →
          </Link>
        )}
      </div>

      {/* Dates row */}
      <div className="flex flex-wrap gap-6 mb-6 text-sm text-slate-500">
        {data.first_seen && (
          <div>
            <span className="font-medium text-slate-700">Started:</span> {formatDate(data.first_seen)}
          </div>
        )}
        {data.last_seen && (
          <div>
            <span className="font-medium text-slate-700">Last active:</span> {formatDate(data.last_seen)}
          </div>
        )}
        {data.updated_at && (
          <div>
            <span className="font-medium text-slate-700">Updated:</span> {formatDate(data.updated_at)}
          </div>
        )}
      </div>

      {/* Summary */}
      {data.summary && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">Summary</h2>
          <p className="text-slate-700 leading-relaxed">{data.summary}</p>
        </div>
      )}

      {/* Sub-discussions */}
      {data.children && data.children.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Sub-Discussions
            <span className="ml-2 text-sm font-normal text-slate-500">({data.children.length})</span>
          </h2>
          <div className="divide-y divide-slate-100">
            {data.children.map((child: Discussion) => (
              <Link
                key={child.id}
                to={`/discussions/${child.id}`}
                state={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })}
                className="flex items-start justify-between gap-3 py-3 hover:bg-slate-50 -mx-2 px-2 rounded transition-colors block"
              >
                <div className="flex-1 min-w-0">
                  <h4 className="font-medium text-slate-900">{child.title}</h4>
                  {child.summary && (
                    <p className="text-sm text-slate-500 mt-0.5 line-clamp-2">{child.summary}</p>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1 flex-shrink-0">
                  {child.current_state && <Badge label={child.current_state} variant="state" />}
                  {child.category && <Badge label={child.category} variant="category" />}
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* Proposed Actions (next steps) */}
      {data.proposed_actions && data.proposed_actions.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Next Steps
          </h2>
          <ProposedActionsList actions={data.proposed_actions} />
        </div>
      )}

      {/* Milestones */}
      {data.milestones && data.milestones.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">Milestones</h2>
          <MilestoneTracker milestones={data.milestones} />
        </div>
      )}

      {/* Participants */}
      {data.participants.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Participants
            <span className="ml-2 text-sm font-normal text-slate-500">({data.participants.length})</span>
          </h2>
          <div className="flex flex-wrap gap-2">
            {data.participants.map((p) => (
              <Link
                key={p}
                to={`/contacts/${encodeURIComponent(p)}`}
                className="inline-flex items-center px-3 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm rounded-lg transition-colors"
              >
                {p}
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* State history */}
      {data.state_history.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-5">State History</h2>
          <StateTimeline history={data.state_history} />
        </div>
      )}

      {/* Event Timeline */}
      {data.events && data.events.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">
            Event Timeline
            <span className="ml-2 text-sm font-normal text-slate-500">({data.events.length})</span>
          </h2>
          <EventTimeline
            events={data.events}
            onThreadClick={(threadId, sourceEmailId) => {
              setHighlightMessageId(sourceEmailId ?? null);
              const thread = data.threads.find((t) => t.thread_id === threadId);
              if (thread) {
                setSelectedThread(thread);
              } else {
                setSelectedThread({
                  id: 0,
                  thread_id: threadId,
                  subject: null,
                  email_count: 0,
                  first_date: null,
                  last_date: null,
                  participants: [],
                  summary: null,
                });
              }
            }}
          />
        </div>
      )}

      {/* Actions */}
      {data.actions && data.actions.length > 0 && (() => {
        const openActions = data.actions.filter(a => a.status !== 'done');
        const doneActions = data.actions.filter(a => a.status === 'done');
        const visibleActions = showDoneActions ? data.actions : openActions;
        return (
          <div className="card p-6 mb-6">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-base font-semibold text-slate-900">
                Actions
                <span className="ml-2 text-sm font-normal text-slate-500">
                  ({openActions.length} open, {doneActions.length} done)
                </span>
              </h2>
              {doneActions.length > 0 && (
                <button
                  onClick={() => setShowDoneActions(!showDoneActions)}
                  className="text-sm text-blue-600 hover:text-blue-700 transition-colors"
                >
                  {showDoneActions ? 'Hide done' : `Show ${doneActions.length} done`}
                </button>
              )}
            </div>
            <div>
              {visibleActions.map((action) => (
                <ActionRow key={action.id} action={action} linkState={extendBreadcrumbs(location.state, { label: data.title, path: `/discussions/${data.id}` })} />
              ))}
              {visibleActions.length === 0 && (
                <p className="text-sm text-slate-400 py-2">No open actions</p>
              )}
            </div>
          </div>
        );
      })()}

      {/* Calendar Events */}
      {data.calendar_events && data.calendar_events.length > 0 && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Calendar Events
            <span className="ml-2 text-sm font-normal text-slate-500">({data.calendar_events.length})</span>
          </h2>
          <div className="divide-y divide-slate-100">
            {data.calendar_events.map((evt: CalendarEvent) => (
              <div key={evt.id} className="py-3 first:pt-0 last:pb-0">
                <div className="flex items-start justify-between gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-slate-900">
                      {evt.title || '(No title)'}
                      {evt.html_link && (
                        <a
                          href={evt.html_link}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="ml-2 text-blue-500 hover:text-blue-600 text-xs"
                        >
                          ↗
                        </a>
                      )}
                    </div>
                    <div className="flex items-center gap-3 mt-1 text-xs text-slate-400">
                      <span>
                        {evt.all_day
                          ? formatDate(evt.start_time)
                          : `${formatDateTime(evt.start_time)} – ${formatDateTime(evt.end_time)}`
                        }
                      </span>
                      {evt.location && <span>{evt.location}</span>}
                      {evt.attendees.length > 0 && (
                        <span>{evt.attendees.length} attendee{evt.attendees.length !== 1 ? 's' : ''}</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Threads */}
      {data.threads.length > 0 && (
        <div className="card p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Email Threads
            <span className="ml-2 text-sm font-normal text-slate-500">({data.threads.length})</span>
          </h2>
          <div>
            {data.threads.map((thread) => (
              <ThreadRow key={thread.id} thread={thread} onClick={() => { setHighlightMessageId(null); setSelectedThread(thread); }} />
            ))}
          </div>
        </div>
      )}

      {selectedThread && (
        <ThreadModal
          thread={selectedThread}
          highlightMessageId={highlightMessageId}
          onClose={() => { setSelectedThread(null); setHighlightMessageId(null); }}
        />
      )}
    </div>
  );
}
