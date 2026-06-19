import { useEffect, useState, useCallback, useRef } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { api } from '../api';
import type { PipelineJob, JobConfig } from '../types';
import LogViewer from '../components/LogViewer';
import Breadcrumbs from '../components/Breadcrumbs';
import { formatDateTime } from '../utils';

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
    <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium ${style}`}>
      {status === 'running' && (
        <span className="w-1.5 h-1.5 rounded-full bg-blue-500 mr-1.5 animate-pulse" />
      )}
      {status}
    </span>
  );
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

function ConfigSummary({ config }: { config: JobConfig }) {
  if (config.job_type === 'sync') {
    return <span className="text-sm text-slate-600">Sync emails from all accounts</span>;
  }

  return (
    <div className="text-sm text-slate-600 space-y-1">
      <div>
        <span className="font-medium">Stages:</span>{' '}
        {config.stages && config.stages.length > 0
          ? config.stages.map(s => s.replace(/_/g, ' ')).join(', ')
          : 'all stages'
        }
      </div>
      {config.company && <div><span className="font-medium">Company:</span> {config.company}</div>}
      {config.label && <div><span className="font-medium">Label:</span> {config.label}</div>}
      {(config.force || config.clean || config.per_company || config.new_emails || config.stale_model || config.stale_prompt) && (
        <div className="flex gap-1.5 flex-wrap">
          {config.force && <span className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">force</span>}
          {config.clean && <span className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">clean</span>}
          {config.per_company && <span className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">per-company</span>}
          {config.new_emails && <span className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">new-emails</span>}
          {config.stale_model && <span className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">stale-model</span>}
          {config.stale_prompt && <span className="text-xs px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">stale-prompt</span>}
        </div>
      )}
      {config.concurrency && config.concurrency > 1 && (
        <div><span className="font-medium">Concurrency:</span> {config.concurrency}</div>
      )}
    </div>
  );
}

export default function JobDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const jobId = parseInt(id ?? '', 10);

  const [job, setJob] = useState<PipelineJob | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const refreshTimer = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchJob = useCallback(() => {
    if (isNaN(jobId)) return;
    api.getJob(jobId)
      .then((data) => {
        setJob(data);
        setLoading(false);
      })
      .catch((err: Error) => {
        setError(err.message);
        setLoading(false);
      });
  }, [jobId]);

  useEffect(() => {
    fetchJob();
  }, [fetchJob]);

  // Auto-refresh header while running
  useEffect(() => {
    if (job?.status === 'running' || job?.status === 'queued') {
      refreshTimer.current = setInterval(fetchJob, 3000);
    }
    return () => {
      if (refreshTimer.current) clearInterval(refreshTimer.current);
    };
  }, [job?.status, fetchJob]);

  const handleCancel = async () => {
    if (!job || cancelling) return;
    setCancelling(true);
    try {
      const updated = await api.cancelJob(job.id);
      setJob(updated);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to cancel');
    } finally {
      setCancelling(false);
    }
  };

  const handleRerun = () => {
    if (!job) return;
    try {
      const config: JobConfig = JSON.parse(job.config_json);
      api.createJob(config).then((newJob) => {
        navigate(`/jobs/${newJob.id}`);
      }).catch(err => setError(err.message));
    } catch { /* ignore */ }
  };

  if (loading) {
    return (
      <div className="p-4 sm:p-8">
        <div className="animate-pulse">
          <div className="h-6 bg-slate-200 rounded w-48 mb-4" />
          <div className="h-4 bg-slate-200 rounded w-32 mb-6" />
          <div className="card h-96 bg-slate-100" />
        </div>
      </div>
    );
  }

  if (error || !job) {
    return (
      <div className="p-4 sm:p-8">
        <div className="card p-6 text-center text-red-600">
          <p className="font-medium">{error ?? 'Job not found'}</p>
          <Link to="/jobs" className="mt-3 btn-secondary inline-block">Back to Jobs</Link>
        </div>
      </div>
    );
  }

  let config: JobConfig;
  try {
    config = JSON.parse(job.config_json);
  } catch {
    config = { job_type: job.job_type as 'sync' | 'analyse' };
  }

  const isTerminal = job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled';
  const canCancel = job.status === 'running' || job.status === 'queued';

  return (
    <div className="p-4 sm:p-8 flex flex-col" style={{ height: 'calc(100vh - 3.5rem)' }}>
      <Breadcrumbs defaultTrail={[{ label: 'Pipeline', path: '/jobs' }]} current={`Job #${job.id}`} />

      {/* Header */}
      <div className="card p-4 mb-4 flex-shrink-0">
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-2">
              <StatusBadge status={job.status} />
              <span className="text-lg font-semibold text-slate-900">
                Job #{job.id}
              </span>
              {job.status === 'running' && job.current_stage && (
                <span className="text-sm text-blue-600">
                  {job.current_stage.replace(/_/g, ' ')}
                  {job.current_company ? ` (${job.current_company})` : ''}
                </span>
              )}
            </div>

            <ConfigSummary config={config} />

            <div className="flex flex-wrap gap-x-6 gap-y-1 mt-3 text-xs text-slate-500">
              <span>Created: {formatDateTime(job.created_at)}</span>
              {job.started_at && <span>Started: {formatDateTime(job.started_at)}</span>}
              {job.started_at && (
                <span>Duration: {formatDuration(job.started_at, job.completed_at)}</span>
              )}
              {job.pid && <span>PID: {job.pid}</span>}
            </div>

            {/* Progress bar */}
            {job.status === 'running' && job.progress_total > 0 && (
              <div className="flex items-center gap-2 mt-2">
                <div className="flex-1 max-w-xs h-2 bg-slate-200 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-blue-500 rounded-full transition-all duration-500"
                    style={{ width: `${Math.round((job.progress_done / job.progress_total) * 100)}%` }}
                  />
                </div>
                <span className="text-xs text-slate-500 tabular-nums">
                  {job.progress_done}/{job.progress_total}
                </span>
              </div>
            )}

            {job.error_message && (
              <div className="mt-2 text-sm text-red-600 bg-red-50 px-3 py-2 rounded-lg">
                {job.error_message}
              </div>
            )}
          </div>

          {/* Actions */}
          <div className="flex gap-2 flex-shrink-0">
            {canCancel && (
              <button
                onClick={handleCancel}
                disabled={cancelling}
                className="px-3 py-1.5 text-sm font-medium text-red-600 border border-red-300 rounded-lg hover:bg-red-50 disabled:opacity-50 transition-colors"
              >
                {cancelling ? 'Cancelling...' : 'Cancel'}
              </button>
            )}
            {isTerminal && (
              <button
                onClick={handleRerun}
                className="px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-300 rounded-lg hover:bg-blue-50 transition-colors"
              >
                Re-run
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Log viewer - takes remaining height */}
      <div className="card flex-1 min-h-0 overflow-hidden rounded-xl">
        <LogViewer jobId={job.id} jobStatus={job.status} />
      </div>
    </div>
  );
}
