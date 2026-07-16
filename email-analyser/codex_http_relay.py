#!/usr/bin/env python3
"""HTTP relay: accepts prompt POST requests and runs codex exec locally.

Uses async job polling so HTTP connections are short-lived (no ~10 min proxy
timeout). Worker POSTs prompt → gets job_id → polls GET /codex/{job_id}.

POST /codex          {"prompt": "...", "model": "..."} → {"job_id": "..."}
GET  /codex/{job_id} → {"status": "running"|"done"|"error", "output": "...", ...}

Usage:
  nohup python3 codex_http_relay.py > /tmp/codex_http_relay.log 2>&1 &
"""

import http.server
import json
import os
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import uuid

BIND_HOST = "172.21.1.38"
BIND_PORT = 8091
JOB_TTL = 7200  # purge jobs older than 2 hours

_jobs: dict = {}  # job_id → {status, output, exit_code, stderr, created_at}
_jobs_lock = threading.Lock()


def _run_codex_job(job_id: str, prompt: str, model: str) -> None:
    """Background thread: runs codex exec and stores result in _jobs."""
    print(
        f"[codex-http-relay] job={job_id} model={model} prompt_len={len(prompt)} START",
        file=sys.stderr,
        flush=True,
    )
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
        outfile = f.name
    try:
        env = dict(os.environ)
        for var in ("ALL_PROXY", "all_proxy", "CODEX_SOCKS_PROXY"):
            env.pop(var, None)

        result = subprocess.run(
            [
                "codex", "exec",
                "--skip-git-repo-check",
                "--output-last-message", outfile,
                "-m", model,
                "-",
            ],
            input=prompt.encode(),
            capture_output=True,
            timeout=1800,
            env=env,
        )

        output = ""
        if os.path.exists(outfile) and os.path.getsize(outfile) > 0:
            with open(outfile) as f:
                output = f.read()

        stderr = result.stderr.decode("utf-8", errors="replace")
        print(
            f"[codex-http-relay] job={job_id} exit={result.returncode} out_len={len(output)} stderr_tail={stderr[-200:].strip()!r}",
            file=sys.stderr,
            flush=True,
        )

        with _jobs_lock:
            _jobs[job_id] = {
                "status": "done",
                "output": output,
                "exit_code": result.returncode,
                "stderr": stderr[-3000:] if len(stderr) > 3000 else stderr,
                "created_at": _jobs[job_id]["created_at"],
            }

    except subprocess.TimeoutExpired:
        print(f"[codex-http-relay] job={job_id} TIMEOUT", file=sys.stderr, flush=True)
        with _jobs_lock:
            _jobs[job_id] = {
                "status": "error",
                "error": "codex timeout after 1800s",
                "created_at": _jobs[job_id]["created_at"],
            }

    except Exception as e:
        print(f"[codex-http-relay] job={job_id} ERROR: {e}", file=sys.stderr, flush=True)
        with _jobs_lock:
            _jobs[job_id] = {
                "status": "error",
                "error": str(e),
                "created_at": _jobs[job_id]["created_at"],
            }

    finally:
        try:
            os.unlink(outfile)
        except Exception:
            pass


def _purge_old_jobs() -> None:
    """Purge jobs older than JOB_TTL seconds."""
    cutoff = time.monotonic() - JOB_TTL
    with _jobs_lock:
        stale = [jid for jid, j in _jobs.items() if j.get("created_at", 0) < cutoff]
        for jid in stale:
            del _jobs[jid]


class CodexRelayHandler(http.server.BaseHTTPRequestHandler):
    timeout = 30  # short socket timeout — all calls are quick

    def do_POST(self):
        if self.path != "/codex":
            self.send_error(404)
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            data = json.loads(body)
        except Exception as e:
            self.send_error(400, str(e))
            return

        prompt = data.get("prompt", "")
        model = data.get("model", "gpt-5.4-mini")

        job_id = str(uuid.uuid4())
        with _jobs_lock:
            _jobs[job_id] = {"status": "running", "created_at": time.monotonic()}

        threading.Thread(
            target=_run_codex_job,
            args=(job_id, prompt, model),
            daemon=True,
        ).start()

        _purge_old_jobs()

        response_body = json.dumps({"job_id": job_id}).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def do_GET(self):
        if not self.path.startswith("/codex/"):
            self.send_error(404)
            return

        job_id = self.path[len("/codex/"):]
        with _jobs_lock:
            job = dict(_jobs.get(job_id, {"status": "not_found"}))
        # Don't send the full created_at in response
        job.pop("created_at", None)

        response_body = json.dumps(job).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def log_message(self, fmt, *args):
        print(f"[codex-http-relay] {fmt % args}", file=sys.stderr, flush=True)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    server = ThreadedHTTPServer((BIND_HOST, BIND_PORT), CodexRelayHandler)
    print(f"Codex HTTP relay (polling mode) on {BIND_HOST}:{BIND_PORT}", file=sys.stderr, flush=True)
    server.serve_forever()
