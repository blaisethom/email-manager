from __future__ import annotations

import json
import subprocess


class ClaudeCLIBackend:
    """LLM backend that calls the Claude CLI (claude) as a subprocess."""

    def __init__(self, model: str | None = None) -> None:
        self._model = model

    @property
    def model_name(self) -> str:
        return self._model or "claude-cli (default model)"

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        return self._run_claude(system, user)

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No markdown fences, no other text."
        raw = self._run_claude(json_system, user)
        cleaned = raw.strip()
        if not cleaned:
            raise ValueError("Claude CLI returned empty response")
        # Strip any markdown fences the CLI might add
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines).strip()
        # Try to find JSON object in the response if there's surrounding text
        if not cleaned.startswith("{"):
            start = cleaned.find("{")
            if start != -1:
                end = cleaned.rfind("}") + 1
                if end > start:
                    cleaned = cleaned[start:end]
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from Claude CLI. Response ({len(raw)} chars): {raw[:300]}") from e

    def _run_claude(self, system: str, user: str) -> str:
        cmd = ["claude", "--print", "--system-prompt", system]
        if self._model:
            cmd.extend(["--model", self._model])
        result = subprocess.run(
            cmd,
            input=user,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Claude CLI failed (exit {result.returncode}): {result.stderr[:500]}")
        return result.stdout
