import { useCallback, useEffect, useRef, useState } from 'react';
import { useSearchParams, Link } from 'react-router-dom';
import { api } from '../api';
import type { SearchResult, DiscussionSearchResult } from '../types';
import { formatDate } from '../utils';
import Pagination from '../components/Pagination';
import ThreadModal, { type ThreadLike } from '../components/ThreadModal';
import Badge from '../components/Badge';

const LIMIT = 20;

function shortenUrls(text: string): string {
  // Collapse URLs to host (optionally + `/…`) so long paths don't blow out the card.
  return text.replace(
    /https?:\/\/([^\s<>"'/]+)(\/[^\s<>"']*)?/gi,
    (_m, host: string, path?: string) => {
      const cleanHost = host.replace(/^\*\*|\*\*$/g, '').replace(/^www\./i, '');
      return path && path !== '/' ? `${cleanHost}/…` : cleanHost;
    },
  );
}

function HighlightedSnippet({ html }: { html: string }) {
  // Shorten URLs first (before ** → <mark> so stray markers inside URLs don't confuse us),
  // then convert PG ts_headline ** markers into <mark>.
  const formatted = shortenUrls(html)
    .replace(/\*\*([^*]+)\*\*/g, '<mark class="bg-yellow-200 text-yellow-900 rounded px-0.5">$1</mark>');
  return (
    <p
      className="text-sm text-slate-600 leading-relaxed mt-1 break-words line-clamp-3"
      dangerouslySetInnerHTML={{ __html: formatted }}
    />
  );
}

function ResultCard({ result, onOpen }: { result: SearchResult; onOpen: () => void }) {
  const metaItems: React.ReactNode[] = [];
  if (result.company_name) {
    metaItems.push(
      <span key="co" className="text-slate-700 font-medium truncate max-w-[12rem]">
        {result.company_name}
      </span>
    );
  }
  if (result.email_count > 0) {
    metaItems.push(
      <span key="n">{result.email_count} email{result.email_count !== 1 ? 's' : ''}</span>
    );
  }
  if (result.first_date) {
    metaItems.push(
      <span key="d">
        {formatDate(result.first_date)}
        {result.last_date && result.last_date !== result.first_date && ` – ${formatDate(result.last_date)}`}
      </span>
    );
  }
  if (result.participants.length > 0) {
    metaItems.push(
      <span key="p">
        {result.participants.length} participant{result.participants.length !== 1 ? 's' : ''}
      </span>
    );
  }

  return (
    <div className="card p-4 hover:shadow-md hover:border-slate-300 transition-all">
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <button
            onClick={onOpen}
            className="text-left w-full"
          >
            <h3 className="font-medium text-slate-900 hover:text-blue-600 transition-colors break-words line-clamp-2">
              {result.subject || '(no subject)'}
            </h3>
          </button>

          <div className="flex flex-wrap items-center mt-1 text-xs text-slate-500">
            {metaItems.map((item, i) => (
              <span key={i} className="flex items-center">
                {i > 0 && <span className="mx-2 text-slate-300">·</span>}
                {item}
              </span>
            ))}
          </div>

          {result.snippet && <HighlightedSnippet html={result.snippet} />}
        </div>

        <div className="text-xs text-slate-400 flex-shrink-0 tabular-nums" title="Search score">
          {result.score.toFixed(2)}
        </div>
      </div>
    </div>
  );
}

function DiscussionResultCard({ result }: { result: DiscussionSearchResult }) {
  return (
    <Link
      to={`/discussions/${result.discussion_id}`}
      className="card p-4 hover:shadow-md hover:border-slate-300 transition-all block"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <h3 className="font-medium text-slate-900 hover:text-blue-600 transition-colors break-words line-clamp-2">
            {result.title}
          </h3>
          <div className="flex flex-wrap items-center mt-1 text-xs text-slate-500 gap-x-2 gap-y-1">
            {result.company_name && (
              <span className="text-slate-700 font-medium truncate max-w-[12rem]">
                {result.company_name}
              </span>
            )}
            {result.category && <Badge label={result.category} variant="category" />}
            {result.current_state && <Badge label={result.current_state} variant="state" />}
            {result.last_seen && (
              <span>
                {result.first_seen && result.first_seen !== result.last_seen
                  ? `${formatDate(result.first_seen)} – ${formatDate(result.last_seen)}`
                  : formatDate(result.last_seen)}
              </span>
            )}
          </div>
          {result.snippet && <HighlightedSnippet html={result.snippet} />}
        </div>
        <div className="text-xs text-slate-400 flex-shrink-0 tabular-nums" title="Search score">
          {result.score.toFixed(2)}
        </div>
      </div>
    </Link>
  );
}

export default function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [results, setResults] = useState<SearchResult[]>([]);
  const [discussionResults, setDiscussionResults] = useState<DiscussionSearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [discussionTotal, setDiscussionTotal] = useState(0);
  const [queryTimeMs, setQueryTimeMs] = useState(0);
  const [searchMode, setSearchMode] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [labels, setLabels] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [selectedThread, setSelectedThread] = useState<ThreadLike | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const q = searchParams.get('q') ?? '';
  const company = searchParams.get('company') ?? '';
  const label = searchParams.get('label') ?? '';
  const dateFrom = searchParams.get('from') ?? '';
  const dateTo = searchParams.get('to') ?? '';
  const category = searchParams.get('category') ?? '';
  const discussionOnly = searchParams.get('discussions') ?? '';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  // Load labels + categories for filters
  useEffect(() => {
    api.getMeta().then(meta => {
      setLabels(meta.labels);
      setCategories(meta.categories);
    }).catch(() => {});
  }, []);

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const doSearch = useCallback(() => {
    if (!q.trim()) {
      setResults([]);
      setDiscussionResults([]);
      setTotal(0);
      setDiscussionTotal(0);
      return;
    }
    setLoading(true);
    setError(null);
    api.search({
      q, limit: LIMIT, page,
      company: company || undefined, label: label || undefined,
      from: dateFrom || undefined, to: dateTo || undefined,
      category: category || undefined,
      discussions: discussionOnly || undefined,
    })
      .then((data) => {
        setResults(data.results);
        setDiscussionResults(data.discussion_results ?? []);
        setTotal(data.total);
        setDiscussionTotal(data.discussion_total ?? 0);
        setQueryTimeMs(data.query_time_ms);
        setSearchMode(data.search_mode ?? '');
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [q, company, label, dateFrom, dateTo, category, discussionOnly, page]);

  useEffect(() => {
    doSearch();
  }, [doSearch]);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const form = e.target as HTMLFormElement;
    const input = form.querySelector('input') as HTMLInputElement;
    const next = new URLSearchParams(searchParams);
    if (input.value.trim()) {
      next.set('q', input.value.trim());
    } else {
      next.delete('q');
    }
    next.delete('page');
    setSearchParams(next);
  }

  function updateParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    if (key !== 'page') next.delete('page');
    setSearchParams(next);
  }

  return (
    <div className="p-4 sm:p-8 max-w-4xl mx-auto">
      {/* Search box */}
      <form onSubmit={handleSubmit} className="mb-6">
        <div className="relative">
          <input
            ref={inputRef}
            type="text"
            defaultValue={q}
            placeholder="Search emails..."
            className="w-full px-4 py-3 text-lg border border-slate-300 rounded-xl bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent placeholder:text-slate-400 shadow-sm"
          />
          <button
            type="submit"
            className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none"
              stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
          </button>
        </div>
      </form>

      {/* Filters */}
      {q && (
        <div className="flex flex-wrap gap-2 mb-4">
          <select
            value={label}
            onChange={(e) => updateParam('label', e.target.value)}
            className="filter-input text-sm"
          >
            <option value="">All labels</option>
            {labels.map(l => <option key={l} value={l}>{l}</option>)}
          </select>
          <input
            type="text"
            value={company}
            onChange={(e) => updateParam('company', e.target.value)}
            placeholder="Company domain..."
            className="filter-input text-sm w-40"
          />
          <input
            type="date"
            value={dateFrom}
            onChange={(e) => updateParam('from', e.target.value)}
            className="filter-input text-sm"
            title="From date"
          />
          <input
            type="date"
            value={dateTo}
            onChange={(e) => updateParam('to', e.target.value)}
            className="filter-input text-sm"
            title="To date"
          />
          <select
            value={category || (discussionOnly ? '__any' : '')}
            onChange={(e) => {
              const v = e.target.value;
              const next = new URLSearchParams(searchParams);
              next.delete('category');
              next.delete('discussions');
              next.delete('page');
              if (v === '__any') {
                next.set('discussions', '1');
              } else if (v) {
                next.set('category', v);
              }
              setSearchParams(next);
            }}
            className="filter-input text-sm"
          >
            <option value="">All threads</option>
            <option value="__any">In a discussion</option>
            {categories.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
          {(label || company || dateFrom || dateTo || category || discussionOnly) && (
            <button
              onClick={() => {
                const next = new URLSearchParams(searchParams);
                ['label', 'company', 'from', 'to', 'category', 'discussions', 'page'].forEach(k => next.delete(k));
                setSearchParams(next);
              }}
              className="text-xs text-slate-500 hover:text-slate-700 px-2 py-1"
            >
              Clear filters
            </button>
          )}
        </div>
      )}

      {/* Results header */}
      {q && !loading && (
        <div className="flex items-center justify-between mb-4">
          <p className="text-sm text-slate-500">
            {total.toLocaleString()} result{total !== 1 ? 's' : ''} for "{q}"
          </p>
          <p className="text-xs text-slate-400">
            {queryTimeMs}ms
            {searchMode && <span className="ml-1.5 px-1.5 py-0.5 bg-slate-100 rounded text-slate-500">{searchMode}</span>}
          </p>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="card p-6 text-center text-red-600 mb-4">
          <p className="font-medium">Search failed</p>
          <p className="text-sm mt-1">{error}</p>
          <button onClick={doSearch} className="mt-3 btn-secondary">Retry</button>
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div className="space-y-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="card p-4 animate-pulse">
              <div className="h-5 bg-slate-200 rounded w-2/3 mb-2" />
              <div className="h-3 bg-slate-200 rounded w-1/3 mb-2" />
              <div className="h-4 bg-slate-200 rounded w-full" />
            </div>
          ))}
        </div>
      )}

      {/* Discussion results */}
      {!loading && discussionResults.length > 0 && (
        <div className="mb-6">
          <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
            Discussions ({discussionTotal.toLocaleString()})
          </h2>
          <div className="space-y-3">
            {discussionResults.map((result) => (
              <DiscussionResultCard key={result.discussion_id} result={result} />
            ))}
          </div>
        </div>
      )}

      {/* Thread results */}
      {!loading && results.length > 0 && (
        <>
          {discussionResults.length > 0 && (
            <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
              Threads ({total.toLocaleString()})
            </h2>
          )}
          <div className="space-y-3">
            {results.map((result) => (
              <ResultCard
                key={result.thread_id}
                result={result}
                onOpen={() => setSelectedThread({
                  thread_id: result.thread_id,
                  subject: result.subject,
                  email_count: result.email_count,
                  first_date: result.first_date,
                  last_date: result.last_date,
                })}
              />
            ))}
          </div>
          <Pagination
            page={page}
            total={total}
            limit={LIMIT}
            onPageChange={(p) => updateParam('page', String(p))}
          />
        </>
      )}

      {selectedThread && (
        <ThreadModal thread={selectedThread} onClose={() => setSelectedThread(null)} />
      )}

      {/* Empty state */}
      {!loading && q && results.length === 0 && discussionResults.length === 0 && !error && (
        <div className="text-center py-12 text-slate-500">
          <p className="text-lg font-medium">No results found</p>
          <p className="text-sm mt-1">Try different keywords or a broader search</p>
        </div>
      )}

      {/* Initial state */}
      {!q && (
        <div className="text-center py-16 text-slate-400">
          <svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
            className="mx-auto mb-4 text-slate-300">
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <p className="text-lg">Search your emails</p>
          <p className="text-sm mt-1">Full-text search across all email threads</p>
        </div>
      )}
    </div>
  );
}
