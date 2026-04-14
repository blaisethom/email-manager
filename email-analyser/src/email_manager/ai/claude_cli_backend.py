from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import threading
import time

from email_manager.ai.base import TokenTracker, TokenUsage

logger = logging.getLogger("email_manager.ai.claude_cli")

# Hard ceiling — kill no matter what after this
MAX_TOTAL_TIMEOUT = 1200  # 20 minutes
# If no new stdout output for this long, assume stuck and kill
ACTIVITY_TIMEOUT = 600    # 10 minutes

# Rough estimate: ~4 chars per token for English text
CHARS_PER_TOKEN = 4


class ClaudeCLIBackend:
    """LLM backend that calls the Claude CLI (claude) as a subprocess.

    Supports both sync (complete/complete_json) and async (acomplete/acomplete_json).
    The async methods use asyncio.create_subprocess_exec for genuine concurrency.
    """

    def __init__(self, model: str | None = None) -> None:
        self._model = model
        self._tracker = TokenTracker()

    @property
    def model_name(self) -> str:
        return self._model or "claude-cli (default model)"

    @property
    def token_tracker(self) -> TokenTracker:
        return self._tracker

    # ── Sync methods ──────────────────────────────────────────────────────

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        result = self._run_claude_sync(system, user)
        self._tracker.record(TokenUsage(
            input_tokens=len(system + user) // CHARS_PER_TOKEN,
            output_tokens=len(result) // CHARS_PER_TOKEN,
        ))
        return result

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No markdown fences, no other text."
        raw = self._run_claude_sync(json_system, user)
        self._tracker.record(TokenUsage(
            input_tokens=len(json_system + user) // CHARS_PER_TOKEN,
            output_tokens=len(raw) // CHARS_PER_TOKEN,
        ))
        return self._parse_json(raw)

    # ── Async methods ─────────────────────────────────────────────────────

    async def acomplete(self, system: str, user: str, temperature: float = 0.3) -> str:
        result = await self._run_claude_async(system, user)
        self._tracker.record(TokenUsage(
            input_tokens=len(system + user) // CHARS_PER_TOKEN,
            output_tokens=len(result) // CHARS_PER_TOKEN,
        ))
        return result

    async def acomplete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No markdown fences, no other text."
        raw = await self._run_claude_async(json_system, user)
        self._tracker.record(TokenUsage(
            input_tokens=len(json_system + user) // CHARS_PER_TOKEN,
            output_tokens=len(raw) // CHARS_PER_TOKEN,
        ))
        return self._parse_json(raw)

    # ── JSON parsing ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_json(raw: str) -> dict:
        cleaned = raw.strip()
        if not cleaned:
            raise ValueError("Claude CLI returned empty response")
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines).strip()
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
                f"Failed to parse JSON from Claude CLI. Response ({len(raw)} chars): {raw[:300]}"
            ) from e

    # ── Sync subprocess (with activity monitoring) ────────────────────────

    def _build_cmd(self, system: str) -> list[str]:
        cmd = ["claude", "--print", "--system-prompt", system]
        if self._model:
            cmd.extend(["--model", self._model])
        return cmd

    def _run_claude_sync(self, system: str, user: str) -> str:
        cmd = self._build_cmd(system)

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        proc.stdin.write(user)
        proc.stdin.close()

        chunks: list[str] = []
        last_activity = time.monotonic()
        read_done = threading.Event()

        def _reader():
            nonlocal last_activity
            while True:
                chunk = proc.stdout.read(4096)
                if not chunk:
                    break
                chunks.append(chunk)
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
                    f"Claude CLI killed after {elapsed:.0f}s total "
                    f"(max {MAX_TOTAL_TIMEOUT}s). Got {len(''.join(chunks))} chars."
                )
            if idle > ACTIVITY_TIMEOUT:
                proc.kill()
                reader_thread.join(timeout=5)
                raise RuntimeError(
                    f"Claude CLI killed after {idle:.0f}s of inactivity "
                    f"({elapsed:.0f}s total). Got {len(''.join(chunks))} chars."
                )

        reader_thread.join(timeout=5)
        proc.wait(timeout=10)

        if proc.returncode != 0:
            stderr = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(f"Claude CLI failed (exit {proc.returncode}): {stderr[:500]}")

        return "".join(chunks)

    # ── Async subprocess (native asyncio concurrency) ─────────────────────

    async def _run_claude_async(self, system: str, user: str) -> str:
        cmd = self._build_cmd(system)

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Write input and read output concurrently
        chunks: list[bytes] = []
        last_activity = asyncio.get_event_loop().time()

        async def _write_stdin():
            proc.stdin.write(user.encode())
            await proc.stdin.drain()
            proc.stdin.close()

        async def _read_stdout():
            nonlocal last_activity
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                chunks.append(chunk)
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
                        f"Claude CLI killed after {elapsed:.0f}s total "
                        f"(max {MAX_TOTAL_TIMEOUT}s). Got {len(b''.join(chunks))} chars."
                    )
                if idle > ACTIVITY_TIMEOUT:
                    proc.kill()
                    raise RuntimeError(
                        f"Claude CLI killed after {idle:.0f}s of inactivity "
                        f"({elapsed:.0f}s total). Got {len(b''.join(chunks))} chars."
                    )

        # Run write, read, and monitor concurrently
        await _write_stdin()

        # Read and monitor concurrently — monitor exits when process ends
        read_task = asyncio.create_task(_read_stdout())
        monitor_task = asyncio.create_task(_monitor())

        # Wait for read to finish (process closed stdout)
        await read_task
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        await proc.wait()

        if proc.returncode != 0:
            stderr = await proc.stderr.read()
            raise RuntimeError(
                f"Claude CLI failed (exit {proc.returncode}): {stderr.decode()[:500]}"
            )

        return b"".join(chunks).decode()
