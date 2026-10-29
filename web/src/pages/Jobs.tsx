import { useEffect, useState, useCallback, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { api } from '../api';
import type {
  PipelineJob, JobConfig, PrefectDeployment, PrefectFlowRun,
  ProcessingRunSummary, PipelineRunsStats,
} from '../types';
import EmptyState from '../components/EmptyState';
import JobLauncher from '../components/JobLauncher';
import { formatDateTime } from '../utils';

const LIMIT = 50;

// ── Shared helpers ────────────────────────────────────────────────────────────

function fmtDuration(ms: number): string {
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

function fmtWallDuration(startedAt: string | null, completedAt: string | null): string {
  if (!startedAt) return '—';
  const start = new Date(startedAt).getTime();
  const end = completedAt ? new Date(completedAt).getTime() : Date.now();
  return fmtDuration(end - start);
}

function fmtTokens(n: number): string {
  if (!n) return '—';
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function shortStage(mode: string): string {
  return mode.replace('staged:', '').replace(/_/g, ' ');
}

// ── Status badges ─────────────────────────────────────────────────────────────

const LOCAL_STATUS_STYLES: Record<string, string> = {
  queued:    'bg-slate-100 text-slate-700',
  running:   'bg-blue-100 text-blue-700',
  completed: 'bg-green-100 text-green-800',
  failed:    'bg-red-100 text-red-800',
  cancelled: 'bg-amber-100 text-amber-800',
};

const PREFECT_STATUS_STYLES: Record<string, string> = {
  COMPLETED:  'bg-green-100 text-green-800',
  FAILED:     'bg-red-100 text-red-800',
  CRASHED:    'bg-red-100 text-red-800',
  CANCELLED:  'bg-amber-100 text-amber-800',
  CANCELLING: 'bg-amber-100 text-amber-800',
  RUNNING:    'bg-blue-100 text-blue-700',
  SCHEDULED:  'bg-slate-100 text-slate-700',
  PENDING:    'bg-slate-100 text-slate-700',
};

function StatusDot({ running }: { running: boolean }) {
  if (!running) return null;
  return <span className="w-1.5 h-1.5 rounded-full bg-blue-500 mr-1.5 animate-pulse inline-block" />;
}

// ── Activity tab: unified Prefect + local jobs ────────────────────────────────

type ActivityItem =
  | { kind: 'prefect'; run: PrefectFlowRun; time: number }
  | { kind: 'local';   job: PipelineJob;    time: number };

function describeJob(job: PipelineJob): string {
  let config: JobConfig;
  try { config = JSON.parse(job.config_json); } catch { return job.job_type; }
  if (config.job_type === 'sync') return 'Sync emails';
  const parts: string[] = [];
  if (config.stages?.length) {
    parts.push(config.stages.length <= 3
      ? config.stages.map(s => s.replace(/_/g, ' ')).join(', ')
      : `${config.stages.length} stages`);
  } else {
    parts.push('all stages');
  }
  if (config.company) parts.push(config.company);
  if (config.label) parts.push(`label: ${config.label}`);
  return parts.join(' · ');
}

function ActivityRow({ item }: { item: ActivityItem }) {
  if (item.kind === 'prefect') {
    const r = item.run;
    const style = PREFECT_STATUS_STYLES[r.state_type] ?? 'bg-slate-100 text-slate-600';
    const running = r.state_type === 'RUNNING';
    return (
      <div className="flex items-center gap-3 py-3 border-b border-slate-100 last:border-0">
        <div className="w-16 flex-shrink-0">
          <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${style}`}>
            <StatusDot running={running} />
            {r.state_name}
          </span>
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-xs px-1 py-0.5 bg-purple-50 text-purple-600 border border-purple-200 rounded font-medium flex-shrink-0">prefect</span>
            <span className="text-sm text-slate-800 font-medium truncate">{r.name}</span>
          </div>
          {r.parameters && Object.keys(r.parameters).length > 0 && (
            <p className="text-xs text-slate-400 truncate mt-0.5">
              {Object.entries(r.parameters)
                .filter(([, v]) => v !== null && v !== undefined)
                .map(([k, v]) => `${k}: ${Array.isArray(v) ? (v as string[]).join(', ') : String(v)}`)
                .join(' · ')}
            </p>
          )}
        </div>
        <div className="text-right text-xs text-slate-400 flex-shrink-0 min-w-[90px]">
          <div>{r.start_time ? formatDateTime(r.start_time) : '—'}</div>
          <div className="mt-0.5 tabular-nums">{fmtWallDuration(r.start_time, r.end_time)}</div>
        </div>
      </div>
    );
  }

  const j = item.job;
  const style = LOCAL_STATUS_STYLES[j.status] ?? 'bg-slate-100 text-slate-700';
  const running = j.status === 'running';
  return (
    <div className="flex items-center gap-3 py-3 border-b border-slate-100 last:border-0">
      <div className="w-16 flex-shrink-0">
        <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${style}`}>
          <StatusDot running={running} />
          {j.status}
        </span>
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs px-1 py-0.5 bg-slate-100 text-slate-600 border border-slate-200 rounded font-medium flex-shrink-0">local</span>
          <span className="text-sm text-slate-800 font-medium truncate">{describeJob(j)}</span>
        </div>
        {j.status === 'running' && (j.current_stage || j.current_company) && (
          <p className="text-xs text-slate-400 truncate mt-0.5">
            {[j.current_stage?.replace(/_/g, ' '), j.current_company].filter(Boolean).join(' · ')}
          </p>
        )}
        {j.error_message && j.status === 'failed' && (
          <p className="text-xs text-red-500 truncate mt-0.5">{j.error_message}</p>
        )}
      </div>
      <div className="text-right text-xs text-slate-400 flex-shrink-0 min-w-[90px]">
        <div>{formatDateTime(j.created_at)}</div>
        <div className="mt-0.5 tabular-nums">{fmtWallDuration(j.started_at, j.completed_at)}</div>
      </div>
    </div>
  );
}

function ActivityTab({ prefectEnabled }: { prefectEnabled: boolean }) {
  const [prefectRuns, setPrefectRuns] = useState<PrefectFlowRun[]>([]);
  const [localJobs, setLocalJobs] = useState<PipelineJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [stateFilter, setStateFilter] = useState('');
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    const [jobs, runs] = await Promise.all([
      api.getJobs({ limit: 50 }),
      prefectEnabled ? api.getPrefectRuns(undefined, 100).catch(() => []) : Promise.resolve([]),
    ]);
    setLocalJobs(jobs.items);
    setPrefectRuns(runs);
    setLoading(false);
  }, [prefectEnabled]);

  useEffect(() => {
    load();
  }, [load]);

  // Auto-refresh when something is running
  useEffect(() => {
    const hasRunning = localJobs.some(j => j.status === 'running' || j.status === 'queued')
      || prefectRuns.some(r => r.state_type === 'RUNNING' || r.state_type === 'SCHEDULED');
    if (hasRunning) {
      timerRef.current = setInterval(load, 8_000);
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [localJobs, prefectRuns, load]);

  const items: ActivityItem[] = [
    ...localJobs.map(j => ({
      kind: 'local' as const,
      job: j,
      time: new Date(j.started_at ?? j.created_at).getTime(),
    })),
    ...prefectRuns.map(r => ({
      kind: 'prefect' as const,
      run: r,
      time: r.start_time ? new Date(r.start_time).getTime() : 0,
    })),
  ]
    .filter(item => {
      if (!stateFilter) return true;
      const state = item.kind === 'local' ? item.job.status : item.run.state_type.toLowerCase();
      if (stateFilter === 'running') return state === 'running';
      if (stateFilter === 'failed') return state === 'failed' || state === 'crashed';
      if (stateFilter === 'completed') return state === 'completed';
      return true;
    })
    .sort((a, b) => b.time - a.time);

  if (loading) {
    return <div className="animate-pulse space-y-2">{Array.from({ length: 5 }).map((_, i) => (
      <div key={i} className="h-12 bg-slate-100 rounded" />
    ))}</div>;
  }

  return (
    <div>
      <div className="flex gap-2 mb-4">
        {['', 'running', 'failed', 'completed'].map(f => (
          <button
            key={f}
            onClick={() => setStateFilter(f)}
            className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
              stateFilter === f
                ? 'bg-slate-800 text-white'
                : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
            }`}
          >
            {f || 'All'}
          </button>
        ))}
      </div>
      {items.length === 0
        ? <EmptyState message="No pipeline runs yet" />
        : <div className="card divide-y divide-slate-100 p-0 overflow-hidden">
            {items.map(item => (
              <ActivityRow
                key={item.kind === 'local' ? `local-${item.job.id}` : `prefect-${item.run.id}`}
                item={item}
              />
            ))}
          </div>
      }
    </div>
  );
}

// ── AI Runs tab: processing_runs ──────────────────────────────────────────────

function StatsBar({ stats }: { stats: PipelineRunsStats }) {
  const total = stats.by_stage.reduce((a, s) => ({
    runs: a.runs + Number(s.run_count),
    input: a.input + Number(s.total_input),
    output: a.output + Number(s.total_output),
    calls: a.calls + Number(s.total_llm_calls),
    errors: a.errors + Number(s.error_count),
  }), { runs: 0, input: 0, output: 0, calls: 0, errors: 0 });

  return (
    <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-5">
      {[
        { label: 'Total runs', value: total.runs.toLocaleString() },
        { label: 'Tokens in', value: fmtTokens(total.input) },
        { label: 'Tokens out', value: fmtTokens(total.output) },
        { label: 'LLM calls', value: total.calls.toLocaleString() },
        { label: 'Errors', value: total.errors.toLocaleString(), red: total.errors > 0 },
      ].map(({ label, value, red }) => (
        <div key={label} className="card p-3 text-center">
          <p className={`text-xl font-bold ${red ? 'text-red-600' : 'text-slate-800'}`}>{value}</p>
          <p className="text-xs text-slate-400 mt-0.5">{label}</p>
        </div>
      ))}
    </div>
  );
}

function StageBreakdown({ stats }: { stats: PipelineRunsStats }) {
  return (
    <div className="card mb-5 overflow-hidden">
      <div className="px-4 py-2.5 border-b border-slate-100 bg-slate-50">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">By stage</p>
      </div>
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-slate-100 text-slate-400 text-left">
            <th className="px-4 py-2 font-medium">Stage</th>
            <th className="px-3 py-2 font-medium text-right">Runs</th>
            <th className="px-3 py-2 font-medium text-right">Tokens in</th>
            <th className="px-3 py-2 font-medium text-right">Tokens out</th>
            <th className="px-3 py-2 font-medium text-right">LLM calls</th>
            <th className="px-3 py-2 font-medium text-right">Errors</th>
          </tr>
        </thead>
        <tbody>
          {stats.by_stage.map(s => (
            <tr key={s.stage} className="border-b border-slate-50 hover:bg-slate-50">
              <td className="px-4 py-2 font-mono text-slate-700">{shortStage(s.stage)}</td>
              <td className="px-3 py-2 text-right tabular-nums text-slate-600">{Number(s.run_count).toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums text-slate-600">{fmtTokens(Number(s.total_input))}</td>
              <td className="px-3 py-2 text-right tabular-nums text-slate-600">{fmtTokens(Number(s.total_output))}</td>
              <td className="px-3 py-2 text-right tabular-nums text-slate-600">{Number(s.total_llm_calls).toLocaleString()}</td>
              <td className={`px-3 py-2 text-right tabular-nums font-medium ${Number(s.error_count) > 0 ? 'text-red-600' : 'text-slate-400'}`}>
                {Number(s.error_count)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AiRunRow({ run }: { run: ProcessingRunSummary }) {
  const [expanded, setExpanded] = useState(false);
  const ok = !run.error;
  const duration = run.total_llm_ms ? fmtDuration(run.total_llm_ms) : '—';
  const wallTime = fmtWallDuration(run.started_at, run.completed_at);

  return (
    <>
      <tr
        className={`border-b border-slate-50 hover:bg-slate-50 cursor-pointer text-xs ${expanded ? 'bg-slate-50' : ''}`}
        onClick={() => setExpanded(e => !e)}
      >
        <td className="px-4 py-2.5 text-slate-500 tabular-nums font-mono">{formatDateTime(run.started_at).split(' ')[0]}</td>
        <td className="px-3 py-2.5 font-medium text-slate-800 max-w-[120px] truncate">{run.company_domain}</td>
        <td className="px-3 py-2.5 font-mono text-slate-600">{shortStage(run.mode)}</td>
        <td className="px-3 py-2.5 text-slate-500">{run.model ?? '—'}</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-slate-600">{fmtTokens(run.input_tokens)}</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-slate-600">{fmtTokens(run.output_tokens)}</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-slate-500">{run.llm_calls || '—'}</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-slate-500">{duration}</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-slate-400">{wallTime}</td>
        <td className="px-4 py-2.5 text-center">
          {ok
            ? <span className="text-green-600">✓</span>
            : <span className="text-red-500" title={run.error ?? ''}>✗</span>
          }
        </td>
      </tr>
      {expanded && (
        <tr className="bg-slate-50 border-b border-slate-100">
          <td colSpan={10} className="px-4 pb-3 pt-1">
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs text-slate-600">
              <div><span className="font-medium text-slate-400 block">Events created</span>{run.events_created}</div>
              <div><span className="font-medium text-slate-400 block">Discussions</span>{run.discussions_created} new · {run.discussions_updated} updated</div>
              <div><span className="font-medium text-slate-400 block">Actions</span>{run.actions_proposed}</div>
              <div><span className="font-medium text-slate-400 block">Prompt hash</span><span className="font-mono">{run.prompt_hash?.slice(0, 8) ?? '—'}</span></div>
            </div>
            {run.error && (
              <div className="mt-2 text-xs text-red-600 bg-red-50 px-3 py-2 rounded border border-red-100">
                {run.error}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

type InProgressItem = { company_domain: string; threads_done: number; events_committed: number; last_checkpoint: string };

function AiRunsTab() {
  const [runs, setRuns] = useState<ProcessingRunSummary[]>([]);
  const [stats, setStats] = useState<PipelineRunsStats | null>(null);
  const [inProgress, setInProgress] = useState<InProgressItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [stageFilter, setStageFilter] = useState('');
  const [offset, setOffset] = useState(0);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [res, statsRes, ipRes] = await Promise.all([
        api.getPipelineRuns({ stage: stageFilter || undefined, limit: LIMIT, offset }),
        stats === null ? api.getPipelineRunStats() : Promise.resolve(null),
        api.getPipelineRunsInProgress(),
      ]);
      setRuns(res.items);
      setTotal(res.total);
      if (statsRes) setStats(statsRes);
      setInProgress(ipRes.items);
    } finally {
      setLoading(false);
    }
  }, [stageFilter, offset]);

  useEffect(() => { setOffset(0); }, [stageFilter]);
  useEffect(() => { load(); }, [load]);

  // Auto-refresh while something is in progress
  useEffect(() => {
    if (inProgress.length === 0) return;
    const t = setInterval(load, 15_000);
    return () => clearInterval(t);
  }, [inProgress.length, load]);

  const stages = stats ? [...new Set(stats.by_stage.map(s => s.stage))] : [];
  const page = Math.floor(offset / LIMIT) + 1;
  const totalPages = Math.ceil(total / LIMIT);

  return (
    <div>
      {stats && <StatsBar stats={stats} />}
      {stats && stats.by_stage.length > 1 && <StageBreakdown stats={stats} />}

      {inProgress.length > 0 && (
        <div className="mb-4 card border-blue-200 bg-blue-50 p-3">
          <p className="text-xs font-semibold text-blue-700 mb-2">In progress (incremental commits)</p>
          <div className="space-y-1.5">
            {inProgress.map(ip => (
              <div key={ip.company_domain} className="flex items-center gap-3 text-xs">
                <span className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse flex-shrink-0" />
                <span className="font-medium text-blue-800">{ip.company_domain}</span>
                <span className="text-blue-600">{ip.threads_done} threads · {ip.events_committed} events committed</span>
                <span className="text-blue-400 ml-auto">last checkpoint {formatDateTime(ip.last_checkpoint)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Stage filter */}
      <div className="flex gap-2 mb-4 flex-wrap">
        {['', ...stages].map(s => (
          <button
            key={s}
            onClick={() => setStageFilter(s)}
            className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
              stageFilter === s
                ? 'bg-slate-800 text-white'
                : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
            }`}
          >
            {s ? shortStage(s) : 'All stages'}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="animate-pulse space-y-1.5">{Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="h-10 bg-slate-100 rounded" />
        ))}</div>
      ) : runs.length === 0 ? (
        <EmptyState message="No AI runs yet" />
      ) : (
        <>
          <div className="card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-100 text-slate-400 text-left">
                    <th className="px-4 py-2.5 font-medium">Date</th>
                    <th className="px-3 py-2.5 font-medium">Company</th>
                    <th className="px-3 py-2.5 font-medium">Stage</th>
                    <th className="px-3 py-2.5 font-medium">Model</th>
                    <th className="px-3 py-2.5 font-medium text-right">Tokens in</th>
                    <th className="px-3 py-2.5 font-medium text-right">Tokens out</th>
                    <th className="px-3 py-2.5 font-medium text-right">Calls</th>
                    <th className="px-3 py-2.5 font-medium text-right">LLM time</th>
                    <th className="px-3 py-2.5 font-medium text-right">Wall time</th>
                    <th className="px-4 py-2.5 font-medium text-center">OK</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map(r => <AiRunRow key={r.id} run={r} />)}
                </tbody>
              </table>
            </div>
          </div>
          {totalPages > 1 && (
            <div className="mt-4 flex justify-center gap-2">
              <button
                disabled={page <= 1}
                onClick={() => setOffset(o => Math.max(0, o - LIMIT))}
                className="px-3 py-1 text-xs rounded bg-slate-100 text-slate-600 disabled:opacity-40"
              >
                ← Prev
              </button>
              <span className="px-3 py-1 text-xs text-slate-500">
                {page} / {totalPages} ({total.toLocaleString()} total)
              </span>
              <button
                disabled={page >= totalPages}
                onClick={() => setOffset(o => o + LIMIT)}
                className="px-3 py-1 text-xs rounded bg-slate-100 text-slate-600 disabled:opacity-40"
              >
                Next →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── Deployments tab: manage Prefect deployments ───────────────────────────────

function PrefectRunRow({ run }: { run: PrefectFlowRun }) {
  const style = PREFECT_STATUS_STYLES[run.state_type] ?? 'bg-slate-100 text-slate-600';
  const duration = run.start_time && run.end_time
    ? Math.round((new Date(run.end_time).getTime() - new Date(run.start_time).getTime()) / 1000)
    : null;
  return (
    <div className="flex items-center gap-3 py-2.5 border-b border-slate-100 last:border-0 text-sm">
      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium flex-shrink-0 ${style}`}>
        {run.state_type === 'RUNNING' && <StatusDot running />}
        {run.state_name}
      </span>
      <span className="text-slate-700 truncate flex-1">{run.name}</span>
      <span className="text-slate-400 text-xs flex-shrink-0">
        {run.start_time ? formatDateTime(run.start_time) : formatDateTime(run.end_time ?? '')}
        {duration !== null && ` · ${duration < 60 ? `${duration}s` : `${Math.floor(duration / 60)}m`}`}
      </span>
    </div>
  );
}

function PrefectDeploymentCard({
  deployment, runs, onTrigger,
}: {
  deployment: PrefectDeployment;
  runs: PrefectFlowRun[];
  onTrigger: () => void;
}) {
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const activeSchedule = deployment.schedules?.find(s => s.active);

  const handleTrigger = async () => {
    setTriggering(true);
    setError(null);
    try {
      await api.triggerPrefectDeployment(deployment.id);
      onTrigger();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed');
    } finally {
      setTriggering(false);
    }
  };

  return (
    <div className="card p-4">
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-mono font-semibold text-slate-800 text-sm">{deployment.name}</span>
            {deployment.paused && (
              <span className="text-xs px-1.5 py-0.5 bg-amber-100 text-amber-700 rounded">paused</span>
            )}
            {activeSchedule && (
              <span className="text-xs text-slate-400 font-mono">{activeSchedule.schedule.cron ?? 'scheduled'}</span>
            )}
          </div>
          <p className="text-xs text-slate-400 mt-0.5">{deployment.flow_name}</p>
          {deployment.next_scheduled_start_time && (
            <p className="text-xs text-slate-400">Next: {formatDateTime(deployment.next_scheduled_start_time)}</p>
          )}
        </div>
        <button
          onClick={handleTrigger}
          disabled={triggering}
          className="flex-shrink-0 px-3 py-1.5 text-xs font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {triggering ? 'Running…' : '▶ Run now'}
        </button>
      </div>
      {error && <p className="text-xs text-red-600 mb-2">{error}</p>}
      <div className="border-t border-slate-100 pt-2">
        {runs.length > 0
          ? runs.slice(0, 5).map(r => <PrefectRunRow key={r.id} run={r} />)
          : <p className="text-xs text-slate-400">No recent runs</p>
        }
      </div>
    </div>
  );
}

function DeploymentsTab() {
  const [deployments, setDeployments] = useState<PrefectDeployment[]>([]);
  const [runs, setRuns] = useState<PrefectFlowRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [prefectUrl, setPrefectUrl] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const [deps, recentRuns] = await Promise.all([
        api.getPrefectDeployments(),
        api.getPrefectRuns(undefined, 50),
      ]);
      setDeployments(deps);
      setRuns(recentRuns);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to connect to Prefect');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    api.getPrefectStatus().then(s => setPrefectUrl(s.url)).catch(() => {});
    load();
    const t = setInterval(load, 15_000);
    return () => clearInterval(t);
  }, [load]);

  const runsForDeployment = (depId: string) => runs.filter(r => r.deployment_id === depId);

  if (loading) {
    return (
      <div className="animate-pulse space-y-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="h-24 bg-slate-100 rounded-lg" />
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="card p-4 border-red-200 bg-red-50">
        <p className="text-sm font-medium text-red-700">Cannot reach Prefect server</p>
        <p className="text-xs text-red-500 mt-1">{error}</p>
        {prefectUrl && <p className="text-xs text-slate-500 mt-1">{prefectUrl}</p>}
        <button onClick={load} className="mt-2 text-xs text-red-600 hover:text-red-700">Retry</button>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {deployments.map(d => (
        <PrefectDeploymentCard
          key={d.id}
          deployment={d}
          runs={runsForDeployment(d.id)}
          onTrigger={load}
        />
      ))}
      {deployments.length === 0 && (
        <div className="card p-6 text-center text-slate-400 text-sm">
          No deployments found on the Prefect server.
        </div>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

type Tab = 'activity' | 'ai-runs' | 'deployments';

export default function JobsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();
  const [prefectOn, setPrefectOn] = useState(false);
  const [launcherOpen, setLauncherOpen] = useState(false);

  const activeTab = (searchParams.get('tab') as Tab) ?? 'activity';
  const setTab = (t: Tab) => {
    const next = new URLSearchParams(searchParams);
    next.set('tab', t);
    setSearchParams(next);
  };

  useEffect(() => {
    api.getPrefectStatus().then(s => setPrefectOn(s.enabled)).catch(() => {});
  }, []);

  useEffect(() => {
    if (searchParams.get('new') === '1') {
      setLauncherOpen(true);
      const next = new URLSearchParams(searchParams);
      next.delete('new');
      setSearchParams(next, { replace: true });
    }
  }, []);

  const defaultCompany = searchParams.get('company') ?? undefined;

  const tabs: Array<{ id: Tab; label: string; show?: boolean }> = [
    { id: 'activity', label: 'Activity' },
    { id: 'ai-runs', label: 'AI Runs' },
    { id: 'deployments', label: 'Deployments', show: prefectOn },
  ];

  return (
    <div className="p-4 sm:p-8">
      <div className="flex items-center justify-between mb-5">
        <h1 className="text-2xl font-bold text-slate-900">Pipelines</h1>
        <button
          onClick={() => setLauncherOpen(true)}
          className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
        >
          New Job
        </button>
      </div>

      <div className="flex border-b border-slate-200 mb-6 gap-1">
        {tabs.filter(t => t.show !== false).map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px ${
              activeTab === t.id
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === 'activity' && <ActivityTab prefectEnabled={prefectOn} />}
      {activeTab === 'ai-runs' && <AiRunsTab />}
      {activeTab === 'deployments' && prefectOn && <DeploymentsTab />}

      <JobLauncher
        open={launcherOpen}
        defaultCompany={defaultCompany}
        onClose={() => setLauncherOpen(false)}
        onCreated={jobId => {
          setLauncherOpen(false);
          navigate(`/jobs/${jobId}`);
        }}
      />
    </div>
  );
}
