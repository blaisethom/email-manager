from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import tempfile
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from email_manager.ai.base import TokenTracker, TokenUsage

# Prefect global concurrency limit slot — create once with:
#   prefect gcl create codex-calls --limit 5
_CONCURRENCY_SLOT = "codex-calls"

MAX_TOTAL_TIMEOUT = 1200  # 20 minutes hard ceiling
ACTIVITY_TIMEOUT = 600    # 10 minutes of no stdout → assume stuck
CHARS_PER_TOKEN = 4       # rough estimate for token tracking

logger = logging.getLogger("email_manager.ai.codex_cli")

_RETRY_DELAYS = [5, 15, 30, 60]


def _prefect_log_warning(msg: str) -> None:
    try:
        from prefect.logging import get_run_logger
        get_run_logger().warning(msg)
    except Exception:
        logger.warning(msg)


@contextmanager
def _sync_concurrency():
    try:
        from prefect.concurrency.sync import concurrency
        ctx = concurrency(_CONCURRENCY_SLOT, occupy=1)
    except Exception:
        yield
        return
    with ctx:
        yield


@asynccontextmanager
async def _async_concurrency():
    try:
        from prefect.concurrency.asyncio import concurrency
        ctx = concurrency(_CONCURRENCY_SLOT, occupy=1)
    except Exception:
        yield
        return
    async with ctx:
        yield


def _is_rate_limit_error(stderr: str) -> bool:
    return '"status":429' in stderr or '"status": 429' in stderr or "rate_limit" in stderr.lower()


class CodexCLIBackend:
    """LLM backend that calls the Codex CLI (codex exec) as a subprocess.

    Uses --output-last-message to capture only the final assistant response,
    avoiding the status noise on stdout (token counts, banners, etc.).
    """

    def __init__(self, model: str = "gpt-5.4-mini") -> None:
        self._model = model
        self._tracker = TokenTracker()

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def token_tracker(self) -> TokenTracker:
        return self._tracker

    # ── Sync methods ──────────────────────────────────────────────────────

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        t0 = time.monotonic()
        prompt = f"{system}\n\n{user}"
        result = self._run_sync(prompt)
        dur = int((time.monotonic() - t0) * 1000)
        self._tracker.record(TokenUsage(
            input_tokens=len(system + user) // CHARS_PER_TOKEN,
            output_tokens=len(result) // CHARS_PER_TOKEN,
            duration_ms=dur,
        ))
        return result

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        t0 = time.monotonic()
        json_system = system + "\n\nYou MUST respond with valid JSON only. No markdown fences, no other text."
        prompt = f"{json_system}\n\n{user}"
        raw = self._run_sync(prompt)
        dur = int((time.monotonic() - t0) * 1000)
        self._tracker.record(TokenUsage(
            input_tokens=len(json_system + user) // CHARS_PER_TOKEN,
            output_tokens=len(raw) // CHARS_PER_TOKEN,
            duration_ms=dur,
        ))
        return self._parse_json(raw)

    # ── Async methods ─────────────────────────────────────────────────────

    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str:
        t0 = time.monotonic()
        prompt = f"{system}\n\n{user}"
        result = await self._run_async(prompt)
        dur = int((time.monotonic() - t0) * 1000)
        self._tracker.record(TokenUsage(
            input_tokens=len(system + user) // CHARS_PER_TOKEN,
            output_tokens=len(result) // CHARS_PER_TOKEN,
            duration_ms=dur,
        ))
        return result

    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        t0 = time.monotonic()
        json_system = system + "\n\nYou MUST respond with valid JSON only. No markdown fences, no other text."
        prompt = f"{json_system}\n\n{user}"
        raw = await self._run_async(prompt)
        dur = int((time.monotonic() - t0) * 1000)
        self._tracker.record(TokenUsage(
            input_tokens=len(json_system + user) // CHARS_PER_TOKEN,
            output_tokens=len(raw) // CHARS_PER_TOKEN,
            duration_ms=dur,
        ))
        return self._parse_json(raw)

    # ── JSON parsing ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_json(raw: str) -> dict:
        cleaned = raw.strip()
        if not cleaned:
            raise ValueError("Codex CLI returned empty response")
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()
        if not cleaned.startswith("{"):
            start = cleaned.find("{")
            if start != -1:
                end = cleaned.rfind("}") + 1
                if end > start:
                    cleaned = cleaned[start:end]
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse JSON from Codex CLI. Response ({len(raw)} chars): {raw[:300]}"
            ) from e

    # ── Sync subprocess ───────────────────────────────────────────────────

    def _build_cmd(self, output_file: str) -> list[str]:
        return ["codex", "exec", "--output-last-message", output_file, "-m", self._model, "-"]

    def _run_sync(self, prompt: str) -> str:
        with _sync_concurrency():
            for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
                try:
                    return self._call_sync(prompt)
                except RuntimeError as e:
                    if "429" in str(e) or "rate_limit" in str(e).lower():
                        _prefect_log_warning(
                            f"Codex rate limit (attempt {attempt}/{len(_RETRY_DELAYS)}), "
                            f"retrying in {delay}s: {e}"
                        )
                        time.sleep(delay)
                    else:
                        raise
            return self._call_sync(prompt)  # final attempt

    def _call_sync(self, prompt: str) -> str:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_file = f.name
        try:
            cmd = self._build_cmd(output_file)
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            proc.stdin.write(prompt)
            proc.stdin.close()

            # Drain stdout (contains token counts / banner noise)
            last_activity = time.monotonic()
            read_done = threading.Event()

            def _reader():
                nonlocal last_activity
                while True:
                    chunk = proc.stdout.read(4096)
                    if not chunk:
                        break
                    last_activity = time.monotonic()
                read_done.set()

            reader_thread = threading.Thread(target=_reader, daemon=True)
            reader_thread.start()

            start = time.monotonic()
            while not read_done.is_set():
                read_done.wait(timeout=10)
                elapsed = time.monotonic() - start
                idle = time.monotonic() - last_activity

                if elapsed > MAX_TOTAL_TIMEOUT:
                    proc.kill()
                    reader_thread.join(timeout=5)
                    raise RuntimeError(
                        f"Codex CLI killed after {elapsed:.0f}s total (max {MAX_TOTAL_TIMEOUT}s)."
                    )
                if idle > ACTIVITY_TIMEOUT and elapsed > 30:
                    proc.kill()
                    reader_thread.join(timeout=5)
                    raise RuntimeError(
                        f"Codex CLI killed after {idle:.0f}s of inactivity ({elapsed:.0f}s total)."
                    )

            reader_thread.join(timeout=5)
            proc.wait(timeout=10)

            stderr = proc.stderr.read() if proc.stderr else ""

            if proc.returncode != 0:
                raise RuntimeError(
                    f"Codex CLI failed (exit {proc.returncode}): {stderr[:400]}"
                )

            output_path = Path(output_file)
            if output_path.exists() and output_path.stat().st_size > 0:
                return output_path.read_text().strip()

            if stderr:
                raise RuntimeError(f"Codex CLI produced no output. stderr: {stderr[:300]}")
            raise RuntimeError("Codex CLI produced no output.")
        finally:
            Path(output_file).unlink(missing_ok=True)

    # ── Async subprocess ──────────────────────────────────────────────────

    async def _run_async(self, prompt: str) -> str:
        async with _async_concurrency():
            for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
                try:
                    return await self._call_async(prompt)
                except RuntimeError as e:
                    if "429" in str(e) or "rate_limit" in str(e).lower():
                        _prefect_log_warning(
                            f"Codex rate limit (attempt {attempt}/{len(_RETRY_DELAYS)}), "
                            f"retrying in {delay}s: {e}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise
            return await self._call_async(prompt)  # final attempt

    async def _call_async(self, prompt: str) -> str:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_file = f.name
        try:
            cmd = self._build_cmd(output_file)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            last_activity = asyncio.get_event_loop().time()

            async def _write_stdin():
                proc.stdin.write(prompt.encode())
                await proc.stdin.drain()
                proc.stdin.close()

            async def _read_stdout():
                nonlocal last_activity
                while True:
                    chunk = await proc.stdout.read(4096)
                    if not chunk:
                        break
                    last_activity = asyncio.get_event_loop().time()

            async def _monitor():
                start = asyncio.get_event_loop().time()
                while proc.returncode is None:
                    await asyncio.sleep(10)
                    now = asyncio.get_event_loop().time()
                    elapsed = now - start
                    idle = now - last_activity
                    if elapsed > MAX_TOTAL_TIMEOUT:
                        proc.kill()
                        raise RuntimeError(
                            f"Codex CLI killed after {elapsed:.0f}s total (max {MAX_TOTAL_TIMEOUT}s)."
                        )
                    if idle > ACTIVITY_TIMEOUT and elapsed > 30:
                        proc.kill()
                        raise RuntimeError(
                            f"Codex CLI killed after {idle:.0f}s of inactivity ({elapsed:.0f}s total)."
                        )

            await _write_stdin()
            read_task = asyncio.create_task(_read_stdout())
            monitor_task = asyncio.create_task(_monitor())

            await read_task
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

            await proc.wait()

            stderr_bytes = await proc.stderr.read()
            stderr = stderr_bytes.decode()

            if proc.returncode != 0:
                raise RuntimeError(
                    f"Codex CLI failed (exit {proc.returncode}): {stderr[:400]}"
                )

            output_path = Path(output_file)
            if output_path.exists() and output_path.stat().st_size > 0:
                return output_path.read_text().strip()

            if stderr:
                raise RuntimeError(f"Codex CLI produced no output. stderr: {stderr[:300]}")
            raise RuntimeError("Codex CLI produced no output.")
        finally:
            Path(output_file).unlink(missing_ok=True)
