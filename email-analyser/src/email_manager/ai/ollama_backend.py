from __future__ import annotations

import json

import httpx


class OllamaBackend:
    def __init__(
        self, model: str = "llama3.1:8b", base_url: str = "http://localhost:11434"
    ) -> None:
        self._model = model
        self._base_url = base_url.rstrip("/")

    @property
    def model_name(self) -> str:
        return self._model

    def complete(self, system: str, user: str, temperature: float = 0.3) -> str:
        response = httpx.post(
            f"{self._base_url}/api/generate",
            json={
                "model": self._model,
                "system": system,
                "prompt": user,
                "stream": False,
                "options": {"temperature": temperature},
            },
            timeout=120.0,
        )
        response.raise_for_status()
        return response.json()["response"]

    def complete_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        json_system = system + "\n\nYou MUST respond with valid JSON only. No other text."
        response = httpx.post(
            f"{self._base_url}/api/generate",
            json={
                "model": self._model,
                "system": json_system,
                "prompt": user,
                "stream": False,
                "format": "json",
                "options": {"temperature": temperature},
            },
            timeout=120.0,
        )
        response.raise_for_status()
        return json.loads(response.json()["response"])
