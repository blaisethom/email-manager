#!/usr/bin/env python3
"""HTTP relay: accepts prompt POST requests and runs codex exec locally.

The Docker worker can't run codex's MCP transport via SOCKS5, so this relay
runs codex directly on the dev container (where TPROXY/cookie-injection works)
and returns the result over plain HTTP.

Usage:
  nohup python3 codex_http_relay.py > /tmp/codex_http_relay.log 2>&1 &

POST /codex
  Body: {"prompt": "...", "model": "gpt-5.4-mini"}
  Response: {"output": "...", "exit_code": 0, "stderr": "..."}
"""

import http.server
import json
import os
import socketserver
import subprocess
import sys
import tempfile

BIND_HOST = "172.21.1.38"
BIND_PORT = 8091


class CodexRelayHandler(http.server.BaseHTTPRequestHandler):
    timeout = 1800  # 30-min socket timeout — codex can take ~5 min per batch

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

        print(
            f"[codex-http-relay] model={model} prompt_len={len(prompt)}",
            file=sys.stderr,
            flush=True,
        )

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            outfile = f.name

        try:
            env = dict(os.environ)
            # Run codex locally — don't inherit any SOCKS5 proxy from caller
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
                f"[codex-http-relay] exit={result.returncode} out_len={len(output)} stderr_tail={stderr[-200:].strip()!r}",
                file=sys.stderr,
                flush=True,
            )

            response_body = json.dumps(
                {
                    "output": output,
                    "exit_code": result.returncode,
                    "stderr": stderr[-3000:] if len(stderr) > 3000 else stderr,
                }
            ).encode()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

        except subprocess.TimeoutExpired:
            error_body = json.dumps({"error": "codex timeout after 1800s"}).encode()
            self.send_response(504)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)

        except Exception as e:
            print(f"[codex-http-relay] ERROR: {e}", file=sys.stderr, flush=True)
            error_body = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)

        finally:
            try:
                os.unlink(outfile)
            except Exception:
                pass

    def log_message(self, fmt, *args):
        print(f"[codex-http-relay] {fmt % args}", file=sys.stderr, flush=True)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    server = ThreadedHTTPServer((BIND_HOST, BIND_PORT), CodexRelayHandler)
    print(f"Codex HTTP relay listening on {BIND_HOST}:{BIND_PORT}", file=sys.stderr, flush=True)
    server.serve_forever()
