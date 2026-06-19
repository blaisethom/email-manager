"""Embedding backends for semantic search.

Supports Voyage AI (HTTP API) and Ollama (local). No SDKs — direct HTTP calls.
"""

from __future__ import annotations

import logging
import time
from typing import Protocol

import requests

logger = logging.getLogger("email_manager.search.embeddings")

VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
VOYAGE_BATCH_SIZE = 32  # texts per API call (conservative to avoid rate limits)


class EmbeddingBackend(Protocol):
    """Protocol for embedding backends."""

    model_name: str
    dims: int

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts. Returns list of embedding vectors."""
        ...

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query string."""
        ...


class VoyageBackend:
    """Voyage AI embedding backend via REST API."""

    # Known dimensions per model
    MODEL_DIMS = {
        "voyage-3-lite": 512,
        "voyage-3": 1024,
        "voyage-2": 1024,
    }

    def __init__(self, model: str, api_key: str) -> None:
        self.model_name = model
        self.api_key = api_key
        self.dims = self.MODEL_DIMS.get(model, 512)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed texts in batches of VOYAGE_BATCH_SIZE with retry on rate limits."""
        all_embeddings: list[list[float]] = []

        for i in range(0, len(texts), VOYAGE_BATCH_SIZE):
            chunk = texts[i : i + VOYAGE_BATCH_SIZE]
            embeddings = self._call_with_retry(chunk)
            all_embeddings.extend(embeddings)

        return all_embeddings

    def _call_with_retry(
        self, texts: list[str], max_retries: int = 5
    ) -> list[list[float]]:
        """Call API with exponential backoff on rate limits."""
        for attempt in range(max_retries):
            try:
                result = self._call_api(texts)
                # Pause between successful calls to stay under rate limit
                time.sleep(0.5)
                return result
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = 2 ** attempt + 1  # 2, 3, 5, 9, 17 seconds
                    logger.warning("Rate limited, waiting %ds (attempt %d/%d)", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                else:
                    raise
        # Final attempt without catch
        return self._call_api(texts)

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query."""
        results = self._call_api([text], input_type="query")
        return results[0]

    def _call_api(
        self, texts: list[str], input_type: str = "document"
    ) -> list[list[float]]:
        resp = requests.post(
            VOYAGE_API_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "input": texts,
                "model": self.model_name,
                "input_type": input_type,
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        # Sort by index to ensure order matches input
        items = sorted(data["data"], key=lambda x: x["index"])
        return [item["embedding"] for item in items]


class OllamaBackend:
    """Ollama local embedding backend via REST API."""

    MODEL_DIMS = {
        "nomic-embed-text": 768,
        "mxbai-embed-large": 1024,
        "all-minilm": 384,
    }

    def __init__(self, model: str, url: str = "http://localhost:11434") -> None:
        self.model_name = model
        self.url = url.rstrip("/")
        self.dims = self.MODEL_DIMS.get(model, 768)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed texts using Ollama /api/embed endpoint."""
        resp = requests.post(
            f"{self.url}/api/embed",
            json={"model": self.model_name, "input": texts},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"]

    def embed_query(self, text: str) -> list[float]:
        results = self.embed_batch([text])
        return results[0]


def get_embedding_backend(
    backend: str,
    model: str,
    api_key: str = "",
    ollama_url: str = "http://localhost:11434",
) -> EmbeddingBackend:
    """Factory for embedding backends."""
    if backend == "voyage":
        return VoyageBackend(model=model, api_key=api_key)
    elif backend == "ollama":
        return OllamaBackend(model=model, url=ollama_url)
    else:
        raise ValueError(f"Unknown embedding backend: {backend}")
