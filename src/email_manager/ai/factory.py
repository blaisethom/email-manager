from __future__ import annotations

from email_manager.ai.base import LLMBackend
from email_manager.config import Config


def _claude_cli_available() -> bool:
    """Check whether the ``claude`` CLI is on PATH."""
    import shutil

    return shutil.which("claude") is not None


def get_backend(config: Config) -> LLMBackend:
    if config.ai_backend == "claude":
        if not config.anthropic_api_key:
            if _claude_cli_available():
                from email_manager.ai.claude_cli_backend import ClaudeCLIBackend

                return ClaudeCLIBackend(model=config.claude_model if config.claude_model else None)
            raise ValueError("ANTHROPIC_API_KEY not set and claude CLI not found on PATH.")
        from email_manager.ai.claude_backend import ClaudeBackend

        return ClaudeBackend(api_key=config.anthropic_api_key, model=config.claude_model)
    elif config.ai_backend == "claude-cli":
        from email_manager.ai.claude_cli_backend import ClaudeCLIBackend

        return ClaudeCLIBackend(model=config.claude_model if config.claude_model else None)
    elif config.ai_backend == "ollama":
        from email_manager.ai.ollama_backend import OllamaBackend

        return OllamaBackend(model=config.ollama_model, base_url=config.ollama_url)
    else:
        raise ValueError(f"Unknown AI backend: {config.ai_backend}")
