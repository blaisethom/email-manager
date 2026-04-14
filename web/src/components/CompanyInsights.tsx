import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api';
import type { CompanyInsights, DiscussionInsight, ProcessingRun, LlmCallsByStage } from '../types';
import Badge from './Badge';
import { formatDate } from '../utils';

function daysSince(dateStr: string | null): number | null {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return null;
  return Math.floor((Date.now() - d.getTime()) / (1000 * 60 * 60 * 24));
}

function FreshnessBadge({ days }: { days: number | null }) {
  if (days === null) return <span className="text-xs text-slate-400">no data</span>;
  if (days <= 7) return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700">Fresh ({days}d)</span>;
  if (days <= 30) return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-700">{days}d ago</span>;
  return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-700">Stale ({days}d)</span>;
}

function formatTokens(n: number): string {
  if (!n) return '—';
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function RunRow({ run }: { run: ProcessingRun }) {
  const totalTokens = (Number(run.input_tokens) || 0) + (Number(run.output_tokens) || 0);
  return (
    <tr className="border-b border-slate-100 last:border-0">
      <td className="py-2 pr-4 text-sm text-slate-600">{formatDate(run.started_at)}</td>
      <td className="py-2 pr-4">
        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
          run.mode === 'agent' ? 'bg-purple-100 text-purple-700' :
          run.mode === 'quick' ? 'bg-blue-100 text-blue-700' :
          run.mode === 'staged' ? 'bg-amber-100 text-amber-700' :
          'bg-slate-100 text-slate-600'
        }`}>
          {run.mode}
        </span>
      </td>
      <td className="py-2 pr-4 text-xs text-slate-500 truncate max-w-[150px]">{run.model ?? '—'}</td>
      <td className="py-2 pr-4 text-sm text-slate-600 text-right">{run.events_created}</td>
      <td className="py-2 pr-4 text-sm text-slate-600 text-right">{run.llm_calls || '—'}</td>
      <td className="py-2 text-sm text-slate-600 text-right">{formatTokens(totalTokens)}</td>
    </tr>
  );
}

function DiscussionHealthRow({ disc }: { disc: DiscussionInsight }) {
  const eventDays = daysSince(disc.latest_event_created);
  const lastSeenDays = daysSince(disc.last_seen);
  const isTerminal = ['passed', 'signed', 'closed_won', 'closed_lost', 'deal_lost', 'contract_signed', 'cancelled', 'resolved'].includes(disc.current_state ?? '');

  return (
    <tr className="border-b border-slate-100 last:border-0">
      <td className="py-2.5 pr-3">
        <Link to={`/discussions/${disc.id}`} className="text-sm font-medium text-slate-900 hover:text-blue-600 transition-colors">
          {disc.parent_id && <span className="text-slate-400 mr-1">↳</span>}
          {disc.title.length > 45 ? disc.title.slice(0, 45) + '…' : disc.title}
        </Link>
      </td>
      <td className="py-2.5 pr-3">
        {disc.category && <Badge label={disc.category} variant="category" />}
      </td>
      <td className="py-2.5 pr-3">
        {disc.current_state && <Badge label={disc.current_state} variant="state" />}
      </td>
      <td className="py-2.5 pr-3 text-right text-sm text-slate-600">{disc.event_count}</td>
      <td className="py-2.5 pr-3 text-right">
        {isTerminal ? (
          <span className="text-xs text-slate-400">terminal</span>
        ) : (
          <FreshnessBadge days={eventDays} />
        )}
      </td>
      <td className="py-2.5 pr-3 text-right text-sm text-slate-600">
        {disc.milestones_total > 0 ? (
          <span className={disc.milestones_achieved === disc.milestones_total ? 'text-green-600' : ''}>
            {disc.milestones_achieved}/{disc.milestones_total}
          </span>
        ) : '—'}
      </td>
      <td className="py-2.5 text-right text-sm text-slate-600">{disc.action_count || '—'}</td>
    </tr>
  );
}

export default function CompanyInsightsTab({ companyId }: { companyId: number }) {
  const [data, setData] = useState<CompanyInsights | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    api.getCompanyInsights(companyId)
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [companyId]);

  if (loading) {
    return (
      <div className="animate-pulse space-y-4 mt-6">
        <div className="h-4 bg-slate-200 rounded w-48" />
        <div className="h-24 bg-slate-200 rounded" />
        <div className="h-4 bg-slate-200 rounded w-64" />
        <div className="h-48 bg-slate-200 rounded" />
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="card p-6 mt-6 text-center text-red-600">
        <p>{error ?? 'Failed to load insights'}</p>
      </div>
    );
  }

  const rootDiscussions = data.discussions.filter(d => !d.parent_id);
  const subDiscussions = data.discussions.filter(d => d.parent_id);

  // Build hierarchical ordering: parent followed by its children
  const orderedDiscussions: typeof data.discussions = [];
  for (const root of rootDiscussions) {
    orderedDiscussions.push(root);
    for (const sub of subDiscussions.filter(s => s.parent_id === root.id)) {
      orderedDiscussions.push(sub);
    }
  }
  // Add orphan sub-discussions (parent not in current list)
  for (const sub of subDiscussions) {
    if (!orderedDiscussions.includes(sub)) {
      orderedDiscussions.push(sub);
    }
  }
  const totalEvents = data.events_by_domain.reduce((s, d) => s + Number(d.cnt), 0);
  const highPriority = data.proposed_actions.filter(a => a.priority === 'high');

  return (
    <div className="space-y-6 mt-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Unprocessed Threads</div>
          <div className={`text-lg font-semibold ${data.unprocessed_threads > 0 ? 'text-amber-600' : 'text-green-600'}`}>
            {data.unprocessed_threads}
          </div>
        </div>
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Total Events</div>
          <div className="text-lg font-semibold text-slate-900">{totalEvents}</div>
        </div>
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Discussions</div>
          <div className="text-lg font-semibold text-slate-900">
            {rootDiscussions.length}
            {subDiscussions.length > 0 && <span className="text-sm text-slate-400 ml-1">+{subDiscussions.length} sub</span>}
          </div>
        </div>
        <div className="bg-slate-50 rounded-lg px-4 py-3">
          <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">High Priority Actions</div>
          <div className={`text-lg font-semibold ${highPriority.length > 0 ? 'text-red-600' : 'text-slate-400'}`}>
            {highPriority.length}
          </div>
        </div>
      </div>

      {/* Events by domain */}
      {data.events_by_domain.length > 0 && (
        <div className="card p-6">
          <h3 className="text-base font-semibold text-slate-900 mb-3">Events by Domain</h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
            {data.events_by_domain.map((ed) => (
              <div key={ed.domain} className="flex items-center justify-between bg-slate-50 rounded-lg px-4 py-2.5">
                <div>
                  <Badge label={ed.domain} variant="category" />
                  <div className="text-xs text-slate-400 mt-1">Latest: {formatDate(ed.latest_event_date)}</div>
                </div>
                <span className="text-lg font-semibold text-slate-700">{ed.cnt}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Discussion health */}
      {data.discussions.length > 0 && (
        <div className="card p-6">
          <h3 className="text-base font-semibold text-slate-900 mb-3">Discussion Health</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">Discussion</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">Category</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">State</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Events</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Freshness</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Milestones</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {orderedDiscussions.map((disc) => (
                  <DiscussionHealthRow key={disc.id} disc={disc} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Proposed actions */}
      {data.proposed_actions.length > 0 && (
        <div className="card p-6">
          <h3 className="text-base font-semibold text-slate-900 mb-3">
            Next Steps
            <span className="ml-2 text-sm font-normal text-slate-500">({data.proposed_actions.length})</span>
          </h3>
          <div className="space-y-3">
            {data.proposed_actions.map((pa) => (
              <div key={pa.id} className="border border-slate-200 rounded-lg p-3">
                <div className="flex items-start gap-2">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium flex-shrink-0 ${
                    pa.priority === 'high' ? 'bg-red-100 text-red-700' :
                    pa.priority === 'medium' ? 'bg-amber-100 text-amber-700' :
                    'bg-slate-100 text-slate-600'
                  }`}>{pa.priority}</span>
                  <div className="flex-1">
                    <p className="text-sm text-slate-900">{pa.action}</p>
                    {pa.reasoning && (
                      <p className="text-xs text-slate-500 mt-1">{pa.reasoning}</p>
                    )}
                    <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-400">
                      <Link to={`/discussions/${pa.discussion_id}`} className="hover:text-blue-600">
                        {(pa as any).discussion_title}
                      </Link>
                      {pa.wait_until && <span>Wait until {pa.wait_until}</span>}
                      {pa.assignee && <span>{pa.assignee}</span>}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Token usage by stage */}
      {data.llm_calls_by_stage.length > 0 && (
        <div className="card p-6">
          <h3 className="text-base font-semibold text-slate-900 mb-3">AI Token Usage</h3>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
            <div className="bg-slate-50 rounded-lg px-4 py-3">
              <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Total Input</div>
              <div className="text-lg font-semibold text-slate-900">
                {formatTokens(data.llm_calls_by_stage.reduce((s, d) => s + Number(d.total_input), 0))}
              </div>
            </div>
            <div className="bg-slate-50 rounded-lg px-4 py-3">
              <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Total Output</div>
              <div className="text-lg font-semibold text-slate-900">
                {formatTokens(data.llm_calls_by_stage.reduce((s, d) => s + Number(d.total_output), 0))}
              </div>
            </div>
            <div className="bg-slate-50 rounded-lg px-4 py-3">
              <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Total LLM Calls</div>
              <div className="text-lg font-semibold text-slate-900">
                {data.llm_calls_by_stage.reduce((s, d) => s + Number(d.call_count), 0)}
              </div>
            </div>
            <div className="bg-slate-50 rounded-lg px-4 py-3">
              <div className="text-xs text-slate-500 font-medium uppercase tracking-wider mb-1">Processing Runs</div>
              <div className="text-lg font-semibold text-slate-900">{data.processing_runs.length}</div>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">Stage</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Calls</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Input Tokens</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Output Tokens</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Total</th>
                </tr>
              </thead>
              <tbody>
                {data.llm_calls_by_stage.map((s) => (
                  <tr key={s.stage} className="border-b border-slate-100 last:border-0">
                    <td className="py-2 pr-4 text-sm text-slate-700 font-medium">{s.stage}</td>
                    <td className="py-2 pr-4 text-sm text-slate-600 text-right">{s.call_count}</td>
                    <td className="py-2 pr-4 text-sm text-slate-600 text-right">{formatTokens(Number(s.total_input))}</td>
                    <td className="py-2 pr-4 text-sm text-slate-600 text-right">{formatTokens(Number(s.total_output))}</td>
                    <td className="py-2 text-sm text-slate-700 font-medium text-right">{formatTokens(Number(s.total_input) + Number(s.total_output))}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Processing history */}
      {data.processing_runs.length > 0 && (
        <div className="card p-6">
          <h3 className="text-base font-semibold text-slate-900 mb-3">Processing History</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">Date</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">Mode</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider">Model</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Events</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">LLM Calls</th>
                  <th className="pb-2 text-xs font-medium text-slate-500 uppercase tracking-wider text-right">Tokens</th>
                </tr>
              </thead>
              <tbody>
                {data.processing_runs.map((run) => (
                  <RunRow key={run.id} run={run} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {data.processing_runs.length === 0 && data.discussions.length === 0 && (
        <div className="card p-6 text-center text-slate-500">
          <p>No analysis data yet for this company. Run <code className="bg-slate-100 px-1 rounded">email-analyser update --company {data.company.domain}</code> to start.</p>
        </div>
      )}
    </div>
  );
}
