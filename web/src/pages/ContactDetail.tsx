import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { api } from '../api';
import type { ContactDetail, Thread } from '../types';
import Badge from '../components/Badge';
import { formatDate } from '../utils';

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
          </div>
        </div>
        <span className="flex-shrink-0 text-sm font-medium text-slate-600 bg-slate-100 px-2 py-0.5 rounded">
          {thread.email_count} email{thread.email_count !== 1 ? 's' : ''}
        </span>
      </div>
    </div>
  );
}

export default function ContactDetailPage() {
  const { email: emailParam } = useParams<{ email: string }>();
  const navigate = useNavigate();
  const [data, setData] = useState<ContactDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const email = emailParam ? decodeURIComponent(emailParam) : '';

  useEffect(() => {
    if (!email) return;
    setLoading(true);
    setError(null);
    api
      .getContact(email)
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [email]);

  if (loading) {
    return (
      <div className="p-8">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-slate-200 rounded w-24" />
          <div className="h-8 bg-slate-200 rounded w-64" />
          <div className="h-4 bg-slate-200 rounded w-48" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="p-8">
        <button onClick={() => navigate('/contacts')} className="btn-secondary mb-6">
          ← Back
        </button>
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">{error ?? 'Contact not found'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8 max-w-4xl">
      {/* Back */}
      <button
        onClick={() => navigate('/contacts')}
        className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700 transition-colors mb-6"
      >
        ← Back to Contacts
      </button>

      {/* Header */}
      <div className="mb-6">
        {data.name && (
          <h1 className="text-3xl font-bold text-slate-900">{data.name}</h1>
        )}
        <p className={`text-slate-500 ${data.name ? 'mt-1' : 'text-3xl font-bold text-slate-900'}`}>
          {data.email}
        </p>
        {data.company && (
          <p className="text-slate-600 mt-1">{data.company}</p>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-8">
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Total emails</div>
          <div className="text-lg font-semibold text-slate-900">{data.email_count.toLocaleString()}</div>
        </div>
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Sent</div>
          <div className="text-lg font-semibold text-slate-900">{data.sent_count.toLocaleString()}</div>
        </div>
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Received</div>
          <div className="text-lg font-semibold text-slate-900">{data.received_count.toLocaleString()}</div>
        </div>
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Last active</div>
          <div className="text-base font-semibold text-slate-900">{formatDate(data.last_seen)}</div>
        </div>
      </div>

      {/* Memory section */}
      {data.memory && (
        <div className="card p-6 mb-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">Contact Memory</h2>

          {data.memory.relationship && (
            <div className="mb-4">
              <span className="text-xs text-slate-500 uppercase tracking-wider font-medium block mb-2">
                Relationship
              </span>
              <Badge label={data.memory.relationship} variant="label" />
            </div>
          )}

          {data.memory.summary && (
            <div className="mb-4">
              <span className="text-xs text-slate-500 uppercase tracking-wider font-medium block mb-2">
                Summary
              </span>
              <p className="text-sm text-slate-700 leading-relaxed">{data.memory.summary}</p>
            </div>
          )}

          {data.memory.key_facts && data.memory.key_facts.length > 0 && (
            <div className="mb-4">
              <span className="text-xs text-slate-500 uppercase tracking-wider font-medium block mb-2">
                Key facts
              </span>
              <ul className="space-y-1">
                {data.memory.key_facts.map((fact, i) => (
                  <li key={i} className="flex items-start gap-2 text-sm text-slate-700">
                    <span className="text-blue-400 mt-0.5 flex-shrink-0">•</span>
                    {fact}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {data.memory.discussions && data.memory.discussions.length > 0 && (
            <div>
              <span className="text-xs text-slate-500 uppercase tracking-wider font-medium block mb-2">
                Discussion topics
              </span>
              <div className="space-y-2">
                {data.memory.discussions.map((disc, i) => (
                  <div key={i} className="flex items-center justify-between py-2 border-b border-slate-100 last:border-0">
                    <span className="text-sm text-slate-700">{disc.topic}</span>
                    {disc.status && (
                      <Badge label={disc.status} variant="state" />
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Threads */}
      {data.threads.length > 0 && (
        <div className="card p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-3">
            Recent Threads
            <span className="ml-2 text-sm font-normal text-slate-500">({data.threads.length})</span>
          </h2>
          <div>
            {data.threads.map((thread) => (
              <ThreadRow key={thread.id} thread={thread} />
            ))}
          </div>
        </div>
      )}

      {data.threads.length === 0 && !data.memory && (
        <div className="card p-6 text-center text-slate-500">
          <p className="text-sm">No additional information available for this contact.</p>
        </div>
      )}
    </div>
  );
}
