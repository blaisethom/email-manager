import { useEffect, useState, useCallback, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { api } from '../api';
import type { PipelineJob, JobConfig } from '../types';
import Pagination from '../components/Pagination';
import EmptyState from '../components/EmptyState';
import JobLauncher from '../components/JobLauncher';
import { formatDateTime } from '../utils';

const LIMIT = 25;

const STATUS_STYLES: Record<string, string> = {
  queued: 'bg-slate-100 text-slate-700',
  running: 'bg-blue-100 text-blue-700',
  completed: 'bg-green-100 text-green-800',
  failed: 'bg-red-100 text-red-800',
  cancelled: 'bg-amber-100 text-amber-800',
};

function StatusBadge({ status }: { status: string }) {
  const style = STATUS_STYLES[status] ?? 'bg-slate-100 text-slate-700';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${style}`}>
      {status === 'running' && (
        <span className="w-1.5 h-1.5 rounded-full bg-blue-500 mr-1.5 animate-pulse" />
      )}
      {status}
    </span>
  );
}

function describeJob(job: PipelineJob): string {
  let config: JobConfig;
  try {
    config = JSON.parse(job.config_json);
  } catch {
    return job.job_type;
  }

  if (config.job_type === 'sync') return 'Sync emails';

  const parts: string[] = [];

  if (config.stages && config.stages.length > 0) {
    if (config.stages.length <= 3) {
      parts.push(config.stages.map(s => s.replace(/_/g, ' ')).join(', '));
    } else {
      parts.push(`${config.stages.length} stages`);
    }
  } else {
    parts.push('all stages');
  }

  if (config.company) parts.push(config.company);
  if (config.label) parts.push(`label: ${config.label}`);

  return parts.join(' - ');
}

function formatDuration(startedAt: string | null, completedAt: string | null): string {
  if (!startedAt) return '-';
  const start = new Date(startedAt).getTime();
  const end = completedAt ? new Date(completedAt).getTime() : Date.now();
  const sec = Math.round((end - start) / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  const remSec = sec % 60;
  if (min < 60) return `${min}m ${remSec}s`;
  const hr = Math.floor(min / 60);
  const remMin = min % 60;
  return `${hr}h ${remMin}m`;
}

function ProgressBar({ job }: { job: PipelineJob }) {
  if (job.status !== 'running' || !job.progress_total) return null;
  const pct = Math.round((job.progress_done / job.progress_total) * 100);
  return (
    <div className="flex items-center gap-2 mt-1.5">
      <div className="flex-1 h-1.5 bg-slate-200 rounded-full overflow-hidden">
        <div
          className="h-full bg-blue-500 rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs text-slate-500 tabular-nums">{job.progress_done}/{job.progress_total}</span>
    </div>
  );
}

function JobCard({ job, onClick }: { job: PipelineJob; onClick: () => void }) {
  const flags: string[] = [];
  try {
    const config: JobConfig = JSON.parse(job.config_json);
    if (config.force) flags.push('force');
    if (config.clean) flags.push('clean');
    if (config.per_company) flags.push('per-company');
    if (config.new_emails) flags.push('new-emails');
  } catch { /* ignore */ }

  return (
    <div
      onClick={onClick}
      className="card p-4 hover:shadow-md hover:border-slate-300 transition-all cursor-pointer"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <StatusBadge status={job.status} />
            {job.status === 'running' && job.current_stage && (
              <span className="text-xs text-blue-600 font-medium">
                {job.current_stage.replace(/_/g, ' ')}
              </span>
            )}
            {job.status === 'running' && job.current_company && (
              <span className="text-xs text-slate-500">{job.current_company}</span>
            )}
          </div>
          <p className="text-sm font-medium text-slate-900">{describeJob(job)}</p>
          {flags.length > 0 && (
            <div className="flex gap-1 mt-1">
              {flags.map(f => (
                <span key={f} className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">{f}</span>
              ))}
            </div>
          )}
          <ProgressBar job={job} />
        </div>
        <div className="text-right text-xs text-slate-500 flex-shrink-0">
          <div>{formatDateTime(job.created_at)}</div>
          {job.started_at && (
            <div className="mt-0.5">{formatDuration(job.started_at, job.completed_at)}</div>
          )}
        </div>
      </div>
      {job.error_message && job.status === 'failed' && (
        <div className="mt-2 text-xs text-red-600 bg-red-50 px-2 py-1 rounded truncate">
          {job.error_message}
        </div>
      )}
    </div>
  );
}

export default function JobsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const [items, setItems] = useState<PipelineJob[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [launcherOpen, setLauncherOpen] = useState(false);

  const status = searchParams.get('status') ?? '';
  const page = parseInt(searchParams.get('page') ?? '1', 10);

  // Auto-refresh interval ref
  const refreshTimer = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchData = useCallback(() => {
    setError(null);
    api
      .getJobs({ status: status || undefined, page, limit: LIMIT })
      .then((data) => {
        setItems(data.items);
        setTotal(data.total);
        setLoading(false);
      })
      .catch((err: Error) => {
        setError(err.message);
        setLoading(false);
      });
  }, [status, page]);

  useEffect(() => {
    setLoading(true);
    fetchData();
  }, [fetchData]);

  // Auto-refresh every 5s if any job is running
  useEffect(() => {
    const hasRunning = items.some(j => j.status === 'running' || j.status === 'queued');
    if (hasRunning) {
      refreshTimer.current = setInterval(fetchData, 5000);
    }
    return () => {
      if (refreshTimer.current) clearInterval(refreshTimer.current);
    };
  }, [items, fetchData]);

  function updateParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    if (key !== 'page') next.delete('page');
    setSearchParams(next);
  }

  // Open launcher if ?new=1 is in the URL
  useEffect(() => {
    if (searchParams.get('new') === '1') {
      setLauncherOpen(true);
      const next = new URLSearchParams(searchParams);
      next.delete('new');
      setSearchParams(next, { replace: true });
    }
  }, []);

  const defaultCompany = searchParams.get('company') ?? undefined;

  return (
    <div className="p-4 sm:p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Pipeline</h1>
          {!loading && (
            <p className="text-sm text-slate-500 mt-0.5">{total.toLocaleString()} jobs</p>
          )}
        </div>
        <button
          onClick={() => setLauncherOpen(true)}
          className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
        >
          New Job
        </button>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap gap-3 mb-6">
        <select
          value={status}
          onChange={(e) => updateParam('status', e.target.value)}
          className="filter-input"
        >
          <option value="">All statuses</option>
          <option value="queued">Queued</option>
          <option value="running">Running</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
          <option value="cancelled">Cancelled</option>
        </select>
      </div>

      {/* Content */}
      {error ? (
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">Failed to load jobs</p>
          <p className="text-sm mt-1 text-red-500">{error}</p>
          <button onClick={fetchData} className="mt-3 btn-secondary">Retry</button>
        </div>
      ) : loading ? (
        <div className="space-y-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="card p-4 animate-pulse">
              <div className="h-4 bg-slate-200 rounded w-16 mb-2" />
              <div className="h-5 bg-slate-200 rounded w-2/3 mb-1" />
              <div className="h-3 bg-slate-200 rounded w-1/3" />
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
            {items.map((job) => (
              <JobCard
                key={job.id}
                job={job}
                onClick={() => navigate(`/jobs/${job.id}`)}
              />
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

      {/* Job Launcher Modal */}
      <JobLauncher
        open={launcherOpen}
        onClose={() => setLauncherOpen(false)}
        onCreated={(jobId) => navigate(`/jobs/${jobId}`)}
        defaultCompany={defaultCompany}
      />
    </div>
  );
}
