import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { api } from '../api';
import type { DiscussionDetail, StateHistoryEntry, Thread } from '../types';
import Badge from '../components/Badge';
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

function ThreadRow({ thread }: { thread: Thread }) {
  return (
    <div className="py-3 border-b border-slate-100 last:border-0">
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
  const [data, setData] = useState<DiscussionDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
      <div className="p-8">
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
      <div className="p-8">
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
    <div className="p-8 max-w-4xl">
      {/* Back */}
      <button
        onClick={() => navigate('/discussions')}
        className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700 transition-colors mb-6"
      >
        ← Back to Discussions
      </button>

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-start gap-3 mb-2">
          <h1 className="text-3xl font-bold text-slate-900 flex-1">{data.title}</h1>
        </div>

        <div className="flex flex-wrap items-center gap-2 mb-3">
          {data.current_state && <Badge label={data.current_state} variant="state" />}
          {data.category && <Badge label={data.category} variant="category" />}
        </div>

        {data.company_name && data.company_id && (
          <Link
            to={`/companies/${data.company_id}`}
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

      {/* Threads */}
      {data.threads.length > 0 && (
        <div className="card p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Email Threads
            <span className="ml-2 text-sm font-normal text-slate-500">({data.threads.length})</span>
          </h2>
          <div>
            {data.threads.map((thread) => (
              <ThreadRow key={thread.id} thread={thread} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
