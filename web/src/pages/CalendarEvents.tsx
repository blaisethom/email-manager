import { useEffect, useState, useCallback } from 'react';
import { useSearchParams, Link } from 'react-router-dom';
import { api } from '../api';
import type { CalendarEvent } from '../types';
import SearchBar from '../components/SearchBar';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import { formatDate, formatDateTime } from '../utils';

const LIMIT = 25;

function formatTimeRange(event: CalendarEvent): string {
  if (event.all_day) {
    return formatDate(event.start_time);
  }
  const start = formatDateTime(event.start_time);
  const endDate = new Date(event.end_time);
  const startDate = new Date(event.start_time);
  // Same day — only show end time
  if (startDate.toDateString() === endDate.toDateString()) {
    return `${start} – ${endDate.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}`;
  }
  return `${start} – ${formatDateTime(event.end_time)}`;
}

function EventCard({ event }: { event: CalendarEvent }) {
  return (
    <div className="card p-5 hover:shadow-md hover:border-slate-300 transition-all">
      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2 sm:gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-xs text-slate-500">{formatTimeRange(event)}</span>
            {event.all_day && (
              <span className="text-xs bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded">All day</span>
            )}
          </div>

          <h3 className="text-sm font-medium text-slate-900 leading-relaxed">
            {event.title || '(No title)'}
            {event.html_link && (
              <a
                href={event.html_link}
                target="_blank"
                rel="noopener noreferrer"
                className="ml-2 text-blue-500 hover:text-blue-600 text-xs"
                onClick={(e) => e.stopPropagation()}
              >
                ↗
              </a>
            )}
          </h3>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-2 text-xs text-slate-500">
            {event.location && <span>{event.location}</span>}
            {event.attendees.length > 0 && (
              <span>{event.attendees.length} attendee{event.attendees.length !== 1 ? 's' : ''}</span>
            )}
            {event.discussion_title && event.discussion_id && (
              <Link
                to={`/discussions/${event.discussion_id}`}
                state={{ breadcrumbs: [{ label: 'Calendar', path: '/calendar' }] }}
                className="text-blue-600 hover:text-blue-700 hover:underline truncate max-w-xs"
                onClick={(e) => e.stopPropagation()}
              >
                ↗ {event.discussion_title}
              </Link>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function CalendarEventsPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [items, setItems] = useState<CalendarEvent[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const q = searchParams.get('q') ?? '';
  const from = searchParams.get('from') ?? '';
  const to = searchParams.get('to') ?? '';
  const sort = searchParams.get('sort') ?? 'start_time';
  const order = searchParams.get('order') ?? 'desc';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    api
      .getCalendarEvents({ q, from, to, sort, order, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, from, to, sort, order, page]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  function updateParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    if (key !== 'page') next.delete('page');
    setSearchParams(next);
  }

  return (
    <div className="p-4 sm:p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Calendar Events</h1>
          {!loading && (
            <p className="text-sm text-slate-500 mt-0.5">{total.toLocaleString()} total</p>
          )}
        </div>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap gap-3 mb-6">
        <SearchBar
          value={q}
          onChange={(v) => updateParam('q', v)}
          placeholder="Search events..."
          className="w-full sm:w-72"
        />

        <input
          type="date"
          value={from}
          onChange={(e) => updateParam('from', e.target.value)}
          className="filter-input"
          placeholder="From"
        />

        <input
          type="date"
          value={to}
          onChange={(e) => updateParam('to', e.target.value)}
          className="filter-input"
          placeholder="To"
        />

        <select
          value={`${sort}:${order}`}
          onChange={(e) => {
            const [s, o] = e.target.value.split(':');
            const next = new URLSearchParams(searchParams);
            next.set('sort', s);
            next.set('order', o);
            next.delete('page');
            setSearchParams(next);
          }}
          className="filter-input"
        >
          <option value="start_time:desc">Most recent</option>
          <option value="start_time:asc">Earliest first</option>
          <option value="title:asc">Title A-Z</option>
        </select>
      </div>

      {/* Content */}
      {error ? (
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">Failed to load calendar events</p>
          <p className="text-sm mt-1 text-red-500">{error}</p>
          <button onClick={fetchData} className="mt-3 btn-secondary">
            Retry
          </button>
        </div>
      ) : loading ? (
        <div className="space-y-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="card p-5 animate-pulse">
              <div className="h-4 bg-slate-200 rounded w-32 mb-3" />
              <div className="h-5 bg-slate-200 rounded w-2/3 mb-2" />
              <div className="h-4 bg-slate-200 rounded w-1/2" />
            </div>
          ))}
        </div>
      ) : items.length === 0 ? (
        <div className="card">
          <EmptyState />
        </div>
      ) : (
        <>
          <div className="space-y-3">
            {items.map((event) => (
              <EventCard key={event.id} event={event} />
            ))}
          </div>

          <div className="mt-4">
            <Pagination
              page={page}
              total={total}
              limit={LIMIT}
              onPageChange={(p) => updateParam('page', String(p))}
            />
          </div>
        </>
      )}
    </div>
  );
}
