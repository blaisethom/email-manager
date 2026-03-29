from __future__ import annotations

import json
import subprocess


class ClaudeCLIBackend:
    """LLM backend that calls the Claude CLI (claude) as a subprocess."""

    def __init__(self, model: str | None = None) -> None:
        self._model = model

    @property
    def model_name(self) -> str:
        return self._model or "claude-cli"

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        return self._run_claude(system, user)

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No markdown fences, no other text."
        raw = self._run_claude(json_system, user)
        # Strip any markdown fences the CLI might add
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            # Remove first line (```json) and last line (```)
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)
        return json.loads(cleaned)

    def _run_claude(self, system: str, user: str) -> str:
        cmd = ["claude", "--print", "--system-prompt", system]
        if self._model:
            cmd.extend(["--model", self._model])
        result = subprocess.run(
            cmd,
            input=user,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Claude CLI failed (exit {result.returncode}): {result.stderr[:500]}")
        return result.stdout
