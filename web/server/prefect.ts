/**
 * Prefect 3.x API client.
 *
 * All functions throw on HTTP error. Set PREFECT_API_URL in .env to enable.
 * Example: PREFECT_API_URL=http://172.21.1.47:4200
 */

export const PREFECT_API_URL = (() => {
  const raw = process.env.PREFECT_API_URL;
  if (!raw) return null;
  const trimmed = raw.replace(/\/+$/, '');
  // Accept both http://host:4200 and http://host:4200/api
  return trimmed.endsWith('/api') ? trimmed : trimmed + '/api';
})();

export function prefectEnabled(): boolean {
  return !!PREFECT_API_URL;
}

// ── Types ─────────────────────────────────────────────────────────────────────

export interface PrefectDeployment {
  id: string;
  name: string;
  flow_name: string;
  paused: boolean;
  schedules: Array<{ schedule: { cron?: string }; active: boolean }>;
  last_polled: string | null;
  next_scheduled_start_time: string | null;
}

export interface PrefectFlowRun {
  id: string;
  name: string;
  deployment_id: string | null;
  state_type: string;  // SCHEDULED | PENDING | RUNNING | COMPLETED | FAILED | CRASHED | CANCELLING | CANCELLED
  state_name: string;
  start_time: string | null;
  end_time: string | null;
  total_run_time: number;
  parameters: Record<string, unknown>;
  estimated_start_time_delta: number | null;
}

export interface PrefectLog {
  id: string;
  timestamp: string;
  level: number;
  message: string;
  flow_run_id: string;
  task_run_id: string | null;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function prefetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  if (!PREFECT_API_URL) throw new Error('Prefect not configured');
  const url = `${PREFECT_API_URL}${path}`;
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Prefect API ${path}: HTTP ${res.status} — ${body.slice(0, 200)}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

// ── Deployments ───────────────────────────────────────────────────────────────

export async function listDeployments(): Promise<PrefectDeployment[]> {
  return prefetchJson<PrefectDeployment[]>('/deployments/filter', {
    method: 'POST',
    body: JSON.stringify({ limit: 100, sort: 'NAME_ASC' }),
  });
}

export async function getDeploymentByName(name: string): Promise<PrefectDeployment | null> {
  const all = await listDeployments();
  // name can be "ingest" (short) or "email-manager/ingest" (full)
  return all.find(d => d.name === name || `${d.flow_name}/${d.name}` === name) ?? null;
}

export async function triggerDeployment(
  deploymentId: string,
  parameters: Record<string, unknown> = {},
): Promise<PrefectFlowRun> {
  return prefetchJson<PrefectFlowRun>(`/deployments/${deploymentId}/create_flow_run`, {
    method: 'POST',
    body: JSON.stringify({ parameters }),
  });
}

// ── Flow runs ─────────────────────────────────────────────────────────────────

export async function getFlowRun(flowRunId: string): Promise<PrefectFlowRun> {
  return prefetchJson<PrefectFlowRun>(`/flow_runs/${flowRunId}`);
}

export async function listFlowRuns(opts: {
  deploymentId?: string;
  limit?: number;
  offset?: number;
} = {}): Promise<PrefectFlowRun[]> {
  const body: Record<string, unknown> = {
    limit: opts.limit ?? 50,
    offset: opts.offset ?? 0,
    sort: 'START_TIME_DESC',
  };
  if (opts.deploymentId) {
    body.flow_runs = { deployment_id: { any_: [opts.deploymentId] } };
  }
  return prefetchJson<PrefectFlowRun[]>('/flow_runs/filter', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function cancelFlowRun(flowRunId: string): Promise<void> {
  await prefetchJson<void>(`/flow_runs/${flowRunId}/set_state`, {
    method: 'POST',
    body: JSON.stringify({
      state: { type: 'CANCELLING', name: 'Cancelling' },
      force: false,
    }),
  });
}

// ── Logs ─────────────────────────────────────────────────────────────────────

export async function getFlowRunLogs(
  flowRunId: string,
  offset = 0,
  limit = 200,
): Promise<PrefectLog[]> {
  return prefetchJson<PrefectLog[]>('/logs/filter', {
    method: 'POST',
    body: JSON.stringify({
      logs: { flow_run_id: { any_: [flowRunId] } },
      sort: 'TIMESTAMP_ASC',
      limit,
      offset,
    }),
  });
}

// ── State helpers ─────────────────────────────────────────────────────────────

export function prefectStateToJobStatus(stateType: string): string {
  switch (stateType.toUpperCase()) {
    case 'COMPLETED': return 'completed';
    case 'FAILED':
    case 'CRASHED': return 'failed';
    case 'CANCELLED': return 'cancelled';
    case 'CANCELLING': return 'cancelled';
    case 'RUNNING': return 'running';
    case 'SCHEDULED':
    case 'PENDING': return 'queued';
    default: return 'queued';
  }
}
