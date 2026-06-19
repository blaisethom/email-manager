import { useCallback, useEffect, useState } from 'react';
import { api } from '../api';
import type { ThreadEmail } from '../types';
import Markdown from './Markdown';
import { formatDate, formatDateTime } from '../utils';

export interface ThreadLike {
  thread_id: string;
  subject: string | null;
  email_count: number;
  first_date: string | null;
  last_date: string | null;
  summary?: string | null;
}

export function splitQuotedText(body: string): { fresh: string; quoted: string } {
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

export function EmailBody({ body }: { body: string }) {
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

interface ThreadModalProps {
  thread: ThreadLike;
  onClose: () => void;
  highlightMessageId?: string | null;
  discussionId?: number;
}

export default function ThreadModal({ thread, onClose, highlightMessageId, discussionId }: ThreadModalProps) {
  const [emails, setEmails] = useState<ThreadEmail[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());

  useEffect(() => {
    api.getThreadEmails(thread.thread_id, discussionId)
      .then((data) => {
        setEmails(data.emails);
        const target = highlightMessageId
          ? data.emails.find((e) => e.message_id === highlightMessageId)
          : data.emails[data.emails.length - 1];
        if (target) {
          setExpandedIds(new Set([target.id]));
          if (highlightMessageId) {
            setTimeout(() => {
              document.getElementById(`email-${target.id}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }, 100);
          }
        }
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [thread.thread_id, highlightMessageId, discussionId]);

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

        {thread.summary && (
          <div className="px-6 py-3 bg-slate-50 border-b border-slate-200 text-sm text-slate-600 leading-relaxed">
            {thread.summary}
          </div>
        )}

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
