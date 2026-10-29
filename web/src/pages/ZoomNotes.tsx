import { useEffect, useState, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDateTime } from '../utils';

const BASE = '/api/zoom';
const LIMIT = 20;

interface ZoomNote {
  id: number;
  meeting_uuid: string;
  meeting_id: string;
  host_email: string;
  topic: string;
  start_time: string | null;
  end_time: string | null;
  duration_mins: number | null;
  total_person_mins: number | null;
  participants_count: number | null;
  has_summary: boolean;
  notes_content: string | null;
  summary_created_at: string | null;
  summary_title: string | null;
  summary_overview: string | null;
  summary_details: Array<{ label: string; summary: string }>;
  next_steps: string[];
  audio_vtt_url: string | null;
  join_url: string | null;
}

interface ZoomRecording {
  id: number;
  meeting_uuid: string;
  recording_id: string;
  topic: string;
  start_time: string | null;
  duration_mins: number | null;
  file_type: string;
  file_size: number | null;
  status: string;
  download_url: string | null;
  play_url: string | null;
  host_email: string;
}

function formatDuration(mins: number | null): string {
  if (!mins) return '';
  if (mins < 60) return `${mins}m`;
  return `${Math.floor(mins / 60)}h ${mins % 60}m`;
}

function formatFileSize(bytes: number | null): string {
  if (!bytes) return '';
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function NoteCard({ note, expanded, onToggle }: { note: ZoomNote; expanded: boolean; onToggle: () => void }) {
  const hasSummaryContent = note.summary_overview || (note.summary_details && note.summary_details.length > 0) || (note.next_steps && note.next_steps.length > 0);
  const hasNotes = !!note.notes_content;
  return (
    <div className="card p-5 hover:shadow-md hover:border-slate-300 transition-all">
      <button className="w-full text-left" onClick={onToggle}>
        <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2">
          <div className="flex-1 min-w-0">
            <div className="flex flex-wrap items-center gap-2 mb-1">
              <span className="text-xs text-slate-500">
                {note.start_time ? formatDateTime(note.start_time) : '—'}
              </span>
              {note.duration_mins != null && (
                <span className="text-xs bg-slate-100 text-slate-600 px-1.5 py-0.5 rounded">
                  {formatDuration(note.duration_mins)}
                </span>
              )}
              {note.participants_count != null && (
                <span className="text-xs text-slate-400">{note.participants_count} people</span>
              )}
              {note.has_summary && (
                <span className="text-xs bg-blue-50 text-blue-600 px-1.5 py-0.5 rounded font-medium">AI Summary</span>
              )}
              {hasNotes && !note.has_summary && (
                <span className="text-xs bg-green-50 text-green-600 px-1.5 py-0.5 rounded font-medium">Notes</span>
              )}
            </div>
            <h3 className="text-sm font-semibold text-slate-900 leading-relaxed">
              {note.topic || '(No topic)'}
            </h3>
            {note.summary_overview && !expanded && (
              <p className="text-xs text-slate-600 mt-2 line-clamp-2">{note.summary_overview}</p>
            )}
          </div>
          <span className="text-xs text-slate-400 flex-shrink-0">{expanded ? '▲' : '▼'}</span>
        </div>
      </button>

      {expanded && (
        <div className="mt-4 space-y-4 border-t border-slate-100 pt-4">
          {hasSummaryContent || hasNotes ? (
            <>
              {hasNotes && (
                <div>
                  <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Notes</p>
                  <p className="text-sm text-slate-700 whitespace-pre-wrap">{note.notes_content}</p>
                </div>
              )}
              {note.summary_overview && (
                <div>
                  <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Overview</p>
                  <p className="text-sm text-slate-700 whitespace-pre-wrap">{note.summary_overview}</p>
                </div>
              )}
              {note.summary_details && note.summary_details.length > 0 && (
                <div>
                  <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Details</p>
                  <div className="space-y-3">
                    {note.summary_details.map((d, i) => (
                      <div key={i}>
                        <p className="text-xs font-semibold text-slate-700 mb-0.5">{d.label}</p>
                        <p className="text-sm text-slate-600 whitespace-pre-wrap">{d.summary}</p>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {note.next_steps && note.next_steps.length > 0 && (
                <div>
                  <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Next Steps</p>
                  <ul className="space-y-1">
                    {note.next_steps.map((step, i) => (
                      <li key={i} className="flex items-start gap-2 text-sm text-slate-700">
                        <span className="mt-0.5 text-blue-500 flex-shrink-0">•</span>
                        <span>{step}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </>
          ) : (
            <p className="text-xs text-slate-400 italic">No notes for this meeting.</p>
          )}

          <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-slate-400">
            {note.host_email && <span>Host: {note.host_email}</span>}
            {note.end_time && note.start_time && (
              <span>Ended {formatDateTime(note.end_time)}</span>
            )}
            {note.total_person_mins != null && (
              <span>{note.total_person_mins} person-minutes</span>
            )}
            {note.audio_vtt_url && (
              <a href={note.audio_vtt_url} target="_blank" rel="noopener noreferrer"
                className="text-blue-500 hover:underline">Transcript ↗</a>
            )}
            {note.join_url && (
              <a href={note.join_url} target="_blank" rel="noopener noreferrer"
                className="text-blue-500 hover:underline">Join link ↗</a>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function RecordingCard({ rec }: { rec: ZoomRecording }) {
  return (
    <div className="card p-4 hover:shadow-md hover:border-slate-300 transition-all">
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-2 mb-1">
            <span className="text-xs text-slate-500">
              {rec.start_time ? formatDateTime(rec.start_time) : '—'}
            </span>
            {rec.duration_mins && (
              <span className="text-xs bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded">
                {formatDuration(rec.duration_mins)}
              </span>
            )}
            <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${
              rec.file_type === 'MP4' ? 'bg-purple-50 text-purple-600' :
              rec.file_type === 'TRANSCRIPT' ? 'bg-green-50 text-green-600' :
              rec.file_type === 'CHAT' ? 'bg-yellow-50 text-yellow-600' :
              'bg-slate-100 text-slate-500'
            }`}>{rec.file_type}</span>
            {rec.file_size && (
              <span className="text-xs text-slate-400">{formatFileSize(rec.file_size)}</span>
            )}
          </div>
          <p className="text-sm font-medium text-slate-900">{rec.topic || '(No topic)'}</p>
          {rec.host_email && (
            <p className="text-xs text-slate-400 mt-0.5">Host: {rec.host_email}</p>
          )}
        </div>
        <div className="flex gap-2 flex-shrink-0">
          {rec.play_url && (
            <a href={rec.play_url} target="_blank" rel="noopener noreferrer"
              className="text-xs text-blue-600 hover:text-blue-700 hover:underline">
              Play ↗
            </a>
          )}
          {rec.download_url && (
            <a href={rec.download_url} target="_blank" rel="noopener noreferrer"
              className="text-xs text-slate-500 hover:text-slate-700 hover:underline">
              Download ↗
            </a>
          )}
        </div>
      </div>
    </div>
  );
}

export default function ZoomNotesPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [notes, setNotes] = useState<ZoomNote[]>([]);
  const [notesTotal, setNotesTotal] = useState(0);
  const [recordings, setRecordings] = useState<ZoomRecording[]>([]);
  const [recordingsTotal, setRecordingsTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const q = searchParams.get('q') ?? '';
  const from = searchParams.get('from') ?? '';
  const to = searchParams.get('to') ?? '';
  const tab = searchParams.get('tab') ?? 'notes';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);

    const qs = new URLSearchParams();
    if (q) qs.set('q', q);
    if (from) qs.set('from', from);
    if (to) qs.set('to', to);
    qs.set('page', String(page));
    qs.set('limit', String(LIMIT));

    const notesUrl = `${BASE}/notes?${qs}`;
    const recsUrl = `${BASE}/recordings?${qs}`;

    Promise.all([
      fetch(notesUrl).then(r => r.json()),
      fetch(recsUrl).then(r => r.json()),
    ])
      .then(([nd, rd]) => {
        setNotes(nd.items ?? []);
        setNotesTotal(nd.total ?? 0);
        setRecordings(rd.items ?? []);
        setRecordingsTotal(rd.total ?? 0);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, from, to, page]);

  useEffect(() => { fetchData(); }, [fetchData]);

  function updateParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value); else next.delete(key);
    if (key !== 'page') next.delete('page');
    setSearchParams(next);
  }

  const activeTotal = tab === 'notes' ? notesTotal : recordingsTotal;

  return (
    <div className="p-4 sm:p-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Zoom Notes</h1>
          {!loading && (
            <p className="text-sm text-slate-500 mt-0.5">
              {notesTotal.toLocaleString()} meeting summaries · {recordingsTotal.toLocaleString()} recordings
            </p>
          )}
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-5">
        <SearchBar
          value={q}
          onChange={(v) => updateParam('q', v)}
          placeholder="Search notes…"
          className="w-full sm:w-72"
        />
        <input type="date" value={from} onChange={e => updateParam('from', e.target.value)}
          className="filter-input" placeholder="From" />
        <input type="date" value={to} onChange={e => updateParam('to', e.target.value)}
          className="filter-input" placeholder="To" />
      </div>

      {/* Tabs */}
      <div className="flex gap-4 border-b border-slate-200 mb-5">
        {(['notes', 'recordings'] as const).map(t => (
          <button
            key={t}
            onClick={() => updateParam('tab', t)}
            className={`pb-2 text-sm font-medium capitalize border-b-2 transition-colors ${
              tab === t
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            }`}
          >
            {t === 'notes' ? `Meeting Summaries (${notesTotal})` : `Recordings (${recordingsTotal})`}
          </button>
        ))}
      </div>

      {error ? (
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">Failed to load</p>
          <p className="text-sm mt-1 text-red-500">{error}</p>
          <button onClick={fetchData} className="mt-3 btn-secondary">Retry</button>
        </div>
      ) : loading ? (
        <div className="space-y-4">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="card p-5 animate-pulse">
              <div className="h-4 bg-slate-200 rounded w-32 mb-3" />
              <div className="h-5 bg-slate-200 rounded w-2/3 mb-2" />
              <div className="h-4 bg-slate-200 rounded w-1/2" />
            </div>
          ))}
        </div>
      ) : (tab === 'notes' ? notes : recordings).length === 0 ? (
        <div className="card">
          {notesTotal === 0 && recordingsTotal === 0 && !q ? (
            <div className="p-8 text-center">
              <p className="text-slate-500 font-medium mb-2">No Zoom data synced yet</p>
              <p className="text-sm text-slate-400">
                Run <code className="bg-slate-100 px-1.5 py-0.5 rounded text-xs">email-manager zoom sync</code> to import meeting notes and recordings.
              </p>
            </div>
          ) : (
            <EmptyState />
          )}
        </div>
      ) : tab === 'notes' ? (
        <>
          <div className="space-y-3">
            {notes.map(note => (
              <NoteCard
                key={note.id}
                note={note}
                expanded={expandedId === note.id}
                onToggle={() => setExpandedId(expandedId === note.id ? null : note.id)}
              />
            ))}
          </div>
          <div className="mt-4">
            <Pagination page={page} total={notesTotal} limit={LIMIT}
              onPageChange={p => updateParam('page', String(p))} />
          </div>
        </>
      ) : (
        <>
          <div className="space-y-3">
            {recordings.map(rec => (
              <RecordingCard key={rec.id} rec={rec} />
            ))}
          </div>
          <div className="mt-4">
            <Pagination page={page} total={recordingsTotal} limit={LIMIT}
              onPageChange={p => updateParam('page', String(p))} />
          </div>
        </>
      )}
    </div>
  );
}
