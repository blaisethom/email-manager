/**
 * Pipeline Job Manager
 *
 * Manages pipeline jobs: spawning child processes, capturing logs,
 * streaming via SSE, tracking progress, cancellation, crash recovery.
 * Runs one job at a time with a queued backlog.
 */

import { spawn, type ChildProcess } from 'child_process';
import { createInterface } from 'readline';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import type { Response } from 'express';
import type { Database, DbRow } from './db.js';
import {
  prefectEnabled, triggerDeployment, getDeploymentByName,
  getFlowRun, cancelFlowRun, prefectStateToJobStatus,
} from './prefect.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ── Types ──────────────────────���───────────────────────────────────────────

export interface JobConfig {
  job_type: 'sync' | 'analyse';
  stages?: string[] | null;
  company?: string | null;
  label?: string | null;
  force?: boolean;
  clean?: boolean;
  per_company?: boolean;
  concurrency?: number;
  new_emails?: boolean;
  stale_model?: boolean;
  stale_prompt?: boolean;
}

export interface PipelineJob extends DbRow {
  id: number;
  job_type: string;
  status: string;
  config_json: string;
  pid: number | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  exit_code: number | null;
  error_message: string | null;
  current_stage: string | null;
  progress_done: number;
  progress_total: number;
  current_company: string | null;
  prefect_flow_run_id: string | null;
  prefect_deployment_name: string | null;
}

export interface StageInfo {
  name: string;
  scope: 'global' | 'company';
  needs_ai: boolean;
  depends_on: string[];
}

interface ActiveJob {
  jobId: number;
  process: ChildProcess;
  logBuffer: string[];
  sseClients: Set<Response>;
  startedAt: Date;
  lastOutputAt: Date;
  logStream: fs.WriteStream;
}

// ── Constants ───────��──────────────────────────────────────────────────────

const MAX_LOG_BUFFER = 2000;
const ANALYSER_PATH = path.resolve(__dirname, '../../email-analyser/.venv/bin/email-analyser');
const ANALYSER_CWD = path.resolve(__dirname, '../../email-analyser');
const LOG_DIR = path.resolve(__dirname, '../../data/job_logs');
const DB_UPDATE_INTERVAL_MS = 5000;

// Pipeline stage metadata (mirrors stages.py)
export const PIPELINE_STAGES: StageInfo[] = [
  { name: 'extract_base', scope: 'global', needs_ai: false, depends_on: [] },
  { name: 'fetch_homepages', scope: 'company', needs_ai: false, depends_on: ['extract_base'] },
  { name: 'label_companies', scope: 'company', needs_ai: true, depends_on: ['extract_base'] },
  { name: 'extract_events', scope: 'company', needs_ai: true, depends_on: ['extract_base'] },
  { name: 'discover_discussions', scope: 'company', needs_ai: true, depends_on: ['extract_events'] },
  { name: 'analyse_discussions', scope: 'company', needs_ai: true, depends_on: ['discover_discussions'] },
  { name: 'propose_actions', scope: 'company', needs_ai: true, depends_on: ['analyse_discussions'] },
  { name: 'contact_memory', scope: 'company', needs_ai: true, depends_on: ['extract_base'] },
];

// ── Module state ──────────��────────────────────────────────────────────────

let db: Database;
let activeJob: ActiveJob | null = null;
let dbUpdateTimer: ReturnType<typeof setInterval> | null = null;
// Keep log buffers for recently finished jobs so detail page can show them
const finishedLogBuffers = new Map<number, string[]>();
const MAX_FINISHED_BUFFERS = 5;

// ── DDL ──────────��─────────────────────────────────��───────────────────────

const DDL_SQLITE = `CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type                TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'queued',
    config_json             TEXT NOT NULL,
    pid                     INTEGER,
    started_at              TEXT,
    completed_at            TEXT,
    created_at              TEXT NOT NULL,
    exit_code               INTEGER,
    error_message           TEXT,
    current_stage           TEXT,
    progress_done           INTEGER DEFAULT 0,
    progress_total          INTEGER DEFAULT 0,
    current_company         TEXT,
    prefect_flow_run_id     TEXT,
    prefect_deployment_name TEXT
)`;

const DDL_POSTGRES = `CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id                      SERIAL PRIMARY KEY,
    job_type                TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'queued',
    config_json             TEXT NOT NULL,
    pid                     INTEGER,
    started_at              TEXT,
    completed_at            TEXT,
    created_at              TEXT NOT NULL,
    exit_code               INTEGER,
    error_message           TEXT,
    current_stage           TEXT,
    progress_done           INTEGER DEFAULT 0,
    progress_total          INTEGER DEFAULT 0,
    current_company         TEXT,
    prefect_flow_run_id     TEXT,
    prefect_deployment_name TEXT
)`;

const DDL_INDEXES = `
CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status ON pipeline_jobs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_created ON pipeline_jobs(created_at)
`;

// ── Init ────────���──────────────────────────────���───────────────────────────

export async function initJobs(database: Database): Promise<void> {
  db = database;

  // Ensure log directory exists
  fs.mkdirSync(LOG_DIR, { recursive: true });

  // Create table
  if (db.backend === 'sqlite') {
    await db.exec(DDL_SQLITE);
  } else {
    await db.exec(DDL_POSTGRES);
  }
  await db.exec(DDL_INDEXES);

  // Add new columns if missing (idempotent)
  const alterCmds = [
    `ALTER TABLE pipeline_jobs ADD COLUMN IF NOT EXISTS prefect_flow_run_id TEXT`,
    `ALTER TABLE pipeline_jobs ADD COLUMN IF NOT EXISTS prefect_deployment_name TEXT`,
  ];
  for (const cmd of alterCmds) {
    await db.exec(cmd).catch(() => { /* SQLite: column may already exist */ });
  }

  // Crash recovery: mark orphaned LOCAL running jobs as failed
  const now = new Date().toISOString();
  const orphaned = await db.query<PipelineJob>(
    "SELECT id FROM pipeline_jobs WHERE status = 'running' AND prefect_flow_run_id IS NULL"
  );
  for (const job of orphaned) {
    await db.query(
      "UPDATE pipeline_jobs SET status = 'failed', completed_at = ?, error_message = 'Server restarted during execution' WHERE id = ?",
      now, job.id
    );
    console.log(`[jobs] Marked orphaned job #${job.id} as failed`);
  }

  // Add staleness_status column if missing
  await db.exec(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS staleness_status TEXT`).catch(() => {
    // SQLite doesn't support IF NOT EXISTS on ALTER TABLE; ignore if column exists
  });

  await processQueue();

  // Start Prefect status poller if configured
  if (prefectEnabled()) {
    console.log('[jobs] Prefect integration enabled, starting status poller');
    startPrefectPoller();
  }

  console.log('[jobs] Job manager initialized');
}

// ── Staleness refresh ─────────────────────────────────────────────────────

let stalenessRefreshTimer: ReturnType<typeof setTimeout> | null = null;

export async function refreshStaleness(): Promise<void> {
  const start = Date.now();

  // 1. Mark companies with no successful processing runs as 'never'
  await db.query(
    `UPDATE companies SET staleness_status = 'never'
     WHERE NOT EXISTS (
       SELECT 1 FROM processing_runs pr
       WHERE LOWER(pr.company_domain) = LOWER(companies.domain) AND pr.mode LIKE 'staged:%' AND pr.error IS NULL
     ) AND (staleness_status IS NULL OR staleness_status != 'never')`
  );

  // 2. Get all companies that HAVE been analysed (typically ~200-300)
  const analysed = await db.query<{ domain: string; email_cutoff_date: string | null }>(
    `SELECT c.domain,
            (SELECT pr.email_cutoff_date FROM processing_runs pr
             WHERE LOWER(pr.company_domain) = LOWER(c.domain) AND pr.mode = 'staged:extract_events' AND pr.error IS NULL
             ORDER BY pr.id DESC LIMIT 1
            ) AS email_cutoff_date
     FROM companies c
     WHERE EXISTS (
       SELECT 1 FROM processing_runs pr
       WHERE LOWER(pr.company_domain) = LOWER(c.domain) AND pr.mode LIKE 'staged:%' AND pr.error IS NULL
     )`
  );

  // 3. For each analysed company, check for new emails (uses idx_emails_from index)
  let staleCount = 0;
  let upToDateCount = 0;
  for (const row of analysed) {
    let hasNew = false;
    if (row.email_cutoff_date) {
      const like = `%@${row.domain}%`;
      const newer = await db.queryOne<{ x: number }>(
        `SELECT 1 AS x FROM emails
         WHERE (from_address LIKE ? OR to_addresses LIKE ?) AND date > ?
         LIMIT 1`,
        like, like, row.email_cutoff_date
      );
      hasNew = !!newer;
    }
    const status = hasNew ? 'stale' : 'up_to_date';
    await db.query(
      `UPDATE companies SET staleness_status = ? WHERE domain = ?`,
      status, row.domain
    );
    if (hasNew) staleCount++; else upToDateCount++;
  }

  console.log(`[jobs] Staleness refresh: ${staleCount} stale, ${upToDateCount} up to date, ${analysed.length} analysed (${Date.now() - start}ms)`);
}

function scheduleStaleRefresh(): void {
  // Debounce: refresh 5s after job completion
  if (stalenessRefreshTimer) clearTimeout(stalenessRefreshTimer);
  stalenessRefreshTimer = setTimeout(() => {
    refreshStaleness().catch(err => console.error('[jobs] Staleness refresh failed:', err));
  }, 5000);
}

// ── Job creation ──────────��────────────────────────────────────────────────

// Maps local job configs to Prefect deployment names.
// Returns null if the job should run locally.
function resolvePrefectDeployment(config: JobConfig): string | null {
  if (!prefectEnabled()) return null;
  if (config.job_type === 'sync') return 'ingest';
  if (config.job_type === 'analyse') {
    return config.company ? 'company-ai' : 'ai-analysis';
  }
  return null;
}

export async function createJob(config: JobConfig): Promise<PipelineJob> {
  const now = new Date().toISOString();
  const configJson = JSON.stringify(config);

  const deploymentName = resolvePrefectDeployment(config);

  if (deploymentName) {
    // ── Prefect path ────────────────────────────────────────────────────────
    let deployment = null;
    try {
      deployment = await getDeploymentByName(deploymentName);
    } catch (err) {
      console.warn(`[jobs] Prefect lookup failed (falling back to local): ${err}`);
    }

    if (deployment) {
      const parameters: Record<string, unknown> = {};
      if (config.company) parameters.domain = config.company;
      if (config.stages?.length) parameters.stages = config.stages;
      if (config.force) parameters.force = true;

      const flowRun = await triggerDeployment(deployment.id, parameters);
      console.log(`[jobs] Dispatched to Prefect: deployment=${deploymentName} flow_run=${flowRun.id}`);

      const row = await db.queryOne<PipelineJob>(
        `INSERT INTO pipeline_jobs
           (job_type, status, config_json, created_at, prefect_flow_run_id, prefect_deployment_name)
         VALUES (?, 'queued', ?, ?, ?, ?) RETURNING *`,
        config.job_type, configJson, now, flowRun.id, deploymentName,
      );
      if (!row) throw new Error('Failed to create job record');
      return row;
    }

    console.warn(`[jobs] Prefect deployment "${deploymentName}" not found — running locally`);
    // Fall through to local execution
  }

  // ── Local path ─────────────────────────────────────────────────────────────
  const row = await db.queryOne<PipelineJob>(
    `INSERT INTO pipeline_jobs (job_type, status, config_json, created_at)
     VALUES (?, 'queued', ?, ?) RETURNING *`,
    config.job_type, configJson, now
  );

  if (!row) {
    throw new Error('Failed to create job');
  }

  console.log(`[jobs] Created job #${row.id} (${config.job_type})`);

  // Kick queue
  await processQueue();

  return row;
}

// ── Prefect status poller ─────────────────────────────────────────────────────

let prefectPollTimer: ReturnType<typeof setInterval> | null = null;

function startPrefectPoller(): void {
  if (prefectPollTimer) return;
  prefectPollTimer = setInterval(pollPrefectJobs, 10_000);
}

async function pollPrefectJobs(): Promise<void> {
  try {
    const active = await db.query<{ id: number; prefect_flow_run_id: string }>(
      `SELECT id, prefect_flow_run_id FROM pipeline_jobs
       WHERE prefect_flow_run_id IS NOT NULL AND status IN ('queued', 'running')`,
    );
    if (active.length === 0) return;

    for (const job of active) {
      try {
        const run = await getFlowRun(job.prefect_flow_run_id);
        const newStatus = prefectStateToJobStatus(run.state_type);
        const now = new Date().toISOString();
        const isTerminal = ['completed', 'failed', 'cancelled'].includes(newStatus);

        await db.query(
          `UPDATE pipeline_jobs SET
             status       = ?,
             started_at   = COALESCE(started_at, ?),
             completed_at = ?,
             current_stage = ?
           WHERE id = ?`,
          newStatus,
          run.start_time ?? null,
          isTerminal ? (run.end_time ?? now) : null,
          run.state_name,
          job.id,
        );

        if (isTerminal) {
          scheduleStaleRefresh();
        }
      } catch (err) {
        console.error(`[prefect] Failed to poll flow run ${job.prefect_flow_run_id}:`, err);
      }
    }
  } catch (err) {
    console.error('[prefect] Poll error:', err);
  }
}

// ── Job queries ────────────────���───────────────────────────────────────────

export async function listJobs(params: {
  status?: string;
  page?: number;
  limit?: number;
}): Promise<{ items: PipelineJob[]; total: number }> {
  const page = Math.max(1, params.page ?? 1);
  const limit = Math.min(100, Math.max(1, params.limit ?? 25));
  const offset = (page - 1) * limit;

  const where: string[] = [];
  const queryParams: unknown[] = [];

  if (params.status) {
    where.push('status = ?');
    queryParams.push(params.status);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  const totalRow = await db.queryOne<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM pipeline_jobs ${whereClause}`,
    ...queryParams
  );
  const total = totalRow?.cnt ?? 0;

  const items = await db.query<PipelineJob>(
    `SELECT * FROM pipeline_jobs ${whereClause} ORDER BY created_at DESC LIMIT ? OFFSET ?`,
    ...queryParams, limit, offset
  );

  return { items, total };
}

export async function getJob(id: number): Promise<PipelineJob | undefined> {
  return db.queryOne<PipelineJob>('SELECT * FROM pipeline_jobs WHERE id = ?', id);
}

export function getActiveJob(): { jobId: number; lastOutputAt: Date } | null {
  if (!activeJob) return null;
  return { jobId: activeJob.jobId, lastOutputAt: activeJob.lastOutputAt };
}

// ── Cancellation ───────────────────────────────────────────────────────────

export async function cancelJob(id: number): Promise<boolean> {
  const job = await getJob(id);
  if (!job) return false;

  const now = new Date().toISOString();

  // Prefect-backed job — cancel via Prefect API
  if (job.prefect_flow_run_id && ['queued', 'running'].includes(job.status)) {
    try {
      await cancelFlowRun(job.prefect_flow_run_id);
    } catch (err) {
      console.error(`[prefect] Cancel flow run ${job.prefect_flow_run_id} failed:`, err);
    }
    await db.query(
      "UPDATE pipeline_jobs SET status = 'cancelled', completed_at = ? WHERE id = ?",
      now, id
    );
    return true;
  }

  if (job.status === 'queued') {
    await db.query(
      "UPDATE pipeline_jobs SET status = 'cancelled', completed_at = ? WHERE id = ?",
      now, id
    );
    return true;
  }

  if (job.status === 'running' && activeJob?.jobId === id) {
    // Send SIGTERM first
    activeJob.process.kill('SIGTERM');

    // SIGKILL after 5s if still alive
    const proc = activeJob.process;
    setTimeout(() => {
      try { proc.kill('SIGKILL'); } catch { /* already dead */ }
    }, 5000);

    // Update DB immediately — the exit handler will also fire
    await db.query(
      "UPDATE pipeline_jobs SET status = 'cancelled', completed_at = ? WHERE id = ?",
      now, id
    );

    // Notify SSE clients
    broadcastSSE(id, { type: 'status', status: 'cancelled' });

    return true;
  }

  return false;
}

// ── Queue processing ─────────────────────────────────────���─────────────────

async function processQueue(): Promise<void> {
  if (activeJob) return; // already running one

  const next = await db.queryOne<PipelineJob>(
    "SELECT * FROM pipeline_jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1"
  );

  if (next) {
    await startJob(next);
  }
}

// ── Job execution ─────────────��────────────────────────────────────────────

function buildArgs(config: JobConfig): string[] {
  if (config.job_type === 'sync') {
    return ['sync'];
  }

  const args = ['analyse'];

  if (config.stages && config.stages.length > 0) {
    for (const s of config.stages) {
      args.push('-s', s);
    }
  }

  if (config.company) args.push('--company', config.company);
  if (config.label) args.push('--label', config.label);
  if (config.force) args.push('--force');
  if (config.clean) args.push('--clean');
  if (config.per_company) args.push('--per-company');
  if (config.concurrency && config.concurrency > 1) {
    args.push('--concurrency', String(config.concurrency));
  }
  if (config.new_emails) args.push('--new-emails');
  if (config.stale_model) args.push('--stale-model');
  if (config.stale_prompt) args.push('--stale-prompt');

  return args;
}

async function startJob(job: PipelineJob): Promise<void> {
  const config: JobConfig = JSON.parse(job.config_json);
  const args = buildArgs(config);
  const now = new Date().toISOString();

  console.log(`[jobs] Starting job #${job.id}: ${ANALYSER_PATH} ${args.join(' ')}`);

  const child = spawn(ANALYSER_PATH, args, {
    cwd: ANALYSER_CWD,
    env: {
      ...process.env,
      TERM: 'dumb',
      COLUMNS: '200',
      PYTHONUNBUFFERED: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const logFile = path.join(LOG_DIR, `job_${job.id}.log`);
  const logStream = fs.createWriteStream(logFile, { flags: 'w' });

  const active: ActiveJob = {
    jobId: job.id,
    process: child,
    logBuffer: [],
    sseClients: new Set(),
    startedAt: new Date(),
    lastOutputAt: new Date(),
    logStream,
  };
  activeJob = active;

  // Update DB
  await db.query(
    "UPDATE pipeline_jobs SET status = 'running', started_at = ?, pid = ? WHERE id = ?",
    now, child.pid ?? null, job.id
  );

  // Capture stdout
  if (child.stdout) {
    const rl = createInterface({ input: child.stdout });
    rl.on('line', (line) => handleOutputLine(active, line, 'stdout'));
  }

  // Capture stderr
  if (child.stderr) {
    const rl = createInterface({ input: child.stderr });
    rl.on('line', (line) => handleOutputLine(active, line, 'stderr'));
  }

  // Periodic DB progress updates
  dbUpdateTimer = setInterval(() => flushProgressToDb(active), DB_UPDATE_INTERVAL_MS);

  // Handle exit
  child.on('exit', (code, signal) => {
    handleJobExit(active, code, signal);
  });

  child.on('error', (err) => {
    console.error(`[jobs] Job #${job.id} process error:`, err.message);
    appendLog(active, `[ERROR] Process failed to start: ${err.message}`);
    handleJobExit(active, 1, null);
  });
}

// ── Output handling ─────────���──────────────────────────────────────────────

// Progress parsing regexes
const RE_STAGE_START = /Running stage:\s*(\S+)/;
const RE_COMPANY_HEADER = /Company\s+(\d+)\/(\d+):\s*(\S+)/;
const RE_PROGRESS = /(\w+):\s*(\d+)\/(\d+)\s+/;
const RE_STAGE_RESULT = /(\w+):\s*(generated|created|updated|labelled|analysed|proposed)\s+(\d+)/;

function handleOutputLine(active: ActiveJob, line: string, stream: 'stdout' | 'stderr'): void {
  active.lastOutputAt = new Date();
  const ts = new Date().toISOString();

  appendLog(active, line);

  // Parse progress from stdout
  if (stream === 'stdout') {
    parseProgress(active, line);
  }

  // Stream to SSE clients
  const event = { type: 'log' as const, line, stream, ts };
  broadcastSSE(active.jobId, event);
}

function appendLog(active: ActiveJob, line: string): void {
  active.logBuffer.push(line);
  if (active.logBuffer.length > MAX_LOG_BUFFER) {
    active.logBuffer.shift();
  }
  // Persist to disk
  active.logStream.write(line + '\n');
}

let pendingProgress: {
  stage?: string;
  done?: number;
  total?: number;
  company?: string;
} = {};

function parseProgress(active: ActiveJob, line: string): void {
  let match: RegExpExecArray | null;

  match = RE_STAGE_START.exec(line);
  if (match) {
    pendingProgress.stage = match[1];
    broadcastSSE(active.jobId, { type: 'progress', stage: match[1] });
    return;
  }

  match = RE_COMPANY_HEADER.exec(line);
  if (match) {
    pendingProgress.company = match[3];
    pendingProgress.done = parseInt(match[1], 10);
    pendingProgress.total = parseInt(match[2], 10);
    broadcastSSE(active.jobId, {
      type: 'progress',
      company: match[3],
      done: parseInt(match[1], 10),
      total: parseInt(match[2], 10),
    });
    return;
  }

  match = RE_PROGRESS.exec(line);
  if (match) {
    pendingProgress.stage = match[1];
    pendingProgress.done = parseInt(match[2], 10);
    pendingProgress.total = parseInt(match[3], 10);
    return;
  }

  match = RE_STAGE_RESULT.exec(line);
  if (match) {
    pendingProgress.stage = match[1];
    return;
  }
}

async function flushProgressToDb(active: ActiveJob): Promise<void> {
  try {
    await db.query(
      `UPDATE pipeline_jobs SET current_stage = ?, progress_done = ?, progress_total = ?, current_company = ? WHERE id = ?`,
      pendingProgress.stage ?? null,
      pendingProgress.done ?? 0,
      pendingProgress.total ?? 0,
      pendingProgress.company ?? null,
      active.jobId
    );
  } catch (err) {
    // Non-fatal
  }
}

// ── Job completion ────────────────────────────────��────────────────────────

async function handleJobExit(active: ActiveJob, code: number | null, signal: string | null): Promise<void> {
  if (dbUpdateTimer) {
    clearInterval(dbUpdateTimer);
    dbUpdateTimer = null;
  }

  const now = new Date().toISOString();
  const exitCode = code ?? (signal ? 128 : 1);

  // Check if already cancelled (cancel handler updates status first)
  const current = await getJob(active.jobId);
  const isCancelled = current?.status === 'cancelled';

  if (!isCancelled) {
    const status = exitCode === 0 ? 'completed' : 'failed';
    const errorMsg = exitCode !== 0
      ? `Process exited with code ${exitCode}${signal ? ` (signal: ${signal})` : ''}`
      : null;

    await db.query(
      `UPDATE pipeline_jobs SET status = ?, completed_at = ?, exit_code = ?,
       error_message = ?, current_stage = ?, progress_done = ?, progress_total = ?,
       current_company = ? WHERE id = ?`,
      status, now, exitCode, errorMsg,
      pendingProgress.stage ?? null,
      pendingProgress.done ?? 0,
      pendingProgress.total ?? 0,
      pendingProgress.company ?? null,
      active.jobId
    );

    broadcastSSE(active.jobId, { type: 'status', status, exit_code: exitCode });
  }

  // Close SSE connections and log stream
  Array.from(active.sseClients).forEach(client => {
    try { client.end(); } catch { /* ignore */ }
  });
  active.sseClients.clear();
  active.logStream.end();

  console.log(`[jobs] Job #${active.jobId} finished (exit=${exitCode}, signal=${signal})`);

  // Clear active job and reset progress
  activeJob = null;
  pendingProgress = {};

  // Refresh staleness after job completes
  scheduleStaleRefresh();

  // Process next in queue
  await processQueue();
}

// ─��� SSE streaming ──────────────────────────────��───────────────────────────

export function subscribeToLogs(jobId: number, res: Response): void {
  // Set SSE headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no', // disable nginx buffering
  });
  res.flushHeaders();

  // If job is active, send in-memory buffer then stream live
  if (activeJob?.jobId === jobId) {
    for (const line of activeJob.logBuffer) {
      res.write(`data: ${JSON.stringify({ type: 'log', line, ts: null })}\n\n`);
    }
    activeJob.sseClients.add(res);
    res.on('close', () => {
      activeJob?.sseClients.delete(res);
    });
  } else {
    // Job is finished — read log file from disk
    const logFile = path.join(LOG_DIR, `job_${jobId}.log`);
    if (fs.existsSync(logFile)) {
      const content = fs.readFileSync(logFile, 'utf8');
      const lines = content.split('\n');
      for (const line of lines) {
        if (line) {
          res.write(`data: ${JSON.stringify({ type: 'log', line, ts: null })}\n\n`);
        }
      }
    }

    // Send final status and close
    getJob(jobId).then((job) => {
      if (job) {
        res.write(`data: ${JSON.stringify({ type: 'status', status: job.status, exit_code: job.exit_code })}\n\n`);
      }
      res.end();
    }).catch(() => {
      res.end();
    });
  }
}

function broadcastSSE(jobId: number, event: Record<string, unknown>): void {
  if (!activeJob || activeJob.jobId !== jobId) return;
  const data = `data: ${JSON.stringify(event)}\n\n`;
  Array.from(activeJob.sseClients).forEach(client => {
    try {
      client.write(data);
    } catch {
      activeJob?.sseClients.delete(client);
    }
  });
}

// ── Utilities ──────────────────────────────────────────────────────────────

export function describeJob(job: PipelineJob): string {
  const config: JobConfig = JSON.parse(job.config_json);

  if (config.job_type === 'sync') {
    return 'Sync emails';
  }

  const parts: string[] = [];

  if (config.stages && config.stages.length > 0) {
    if (config.stages.length <= 3) {
      parts.push(config.stages.join(', '));
    } else {
      parts.push(`${config.stages.length} stages`);
    }
  } else {
    parts.push('all stages');
  }

  if (config.company) parts.push(`company: ${config.company}`);
  if (config.label) parts.push(`label: ${config.label}`);
  if (config.force) parts.push('force');
  if (config.clean) parts.push('clean');

  return `Analyse: ${parts.join(', ')}`;
}
