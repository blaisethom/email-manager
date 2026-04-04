from __future__ import annotations

import json

import anthropic


class ClaudeBackend:
    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6") -> None:
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    @property
    def model_name(self) -> str:
        return self._model

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return response.content[0].text

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        # Instruct the model to return JSON and use prefill
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=json_system,
            messages=[
                {"role": "user", "content": user},
                {"role": "assistant", "content": "{"},
            ],
        )
        raw = "{" + response.content[0].text
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from Claude API. Response ({len(raw)} chars): {raw[:300]}") from e
