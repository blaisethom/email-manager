from __future__ import annotations

from email_manager.ai.base import LLMBackend
from email_manager.config import Config


def _claude_cli_available() -> bool:
    """Check whether the ``claude`` CLI is on PATH."""
    import shutil

    return shutil.which("claude") is not None


def _codex_cli_available() -> bool:
    """Check whether the ``codex`` CLI is on PATH."""
    import shutil

    return shutil.which("codex") is not None


def get_backend(config: Config) -> LLMBackend:
    if config.ai_backend == "claude":
        from email_manager.ai.claude_backend import ClaudeBackend

        if config.anthropic_api_key:
            return ClaudeBackend(api_key=config.anthropic_api_key, model=config.claude_model)
        if config.claude_code_oauth_token:
            return ClaudeBackend(auth_token=config.claude_code_oauth_token, model=config.claude_model)
        if _claude_cli_available():
            from email_manager.ai.claude_cli_backend import ClaudeCLIBackend

            return ClaudeCLIBackend(model=config.claude_model if config.claude_model else None)
        raise ValueError(
            "No Claude auth: set ANTHROPIC_API_KEY, CLAUDE_CODE_OAUTH_TOKEN, or log in with 'claude /login'."
        )
    elif config.ai_backend == "claude-cli":
        from email_manager.ai.claude_cli_backend import ClaudeCLIBackend

        return ClaudeCLIBackend(model=config.claude_model if config.claude_model else None)
    elif config.ai_backend == "codex":
        from email_manager.ai.codex_cli_backend import CodexCLIBackend

        if not _codex_cli_available():
            raise ValueError("codex CLI not found on PATH. Install it or set AI_BACKEND to something else.")
        return CodexCLIBackend(model=config.codex_model)
    elif config.ai_backend == "ollama":
        from email_manager.ai.ollama_backend import OllamaBackend

        return OllamaBackend(model=config.ollama_model, base_url=config.ollama_url)
    else:
        raise ValueError(f"Unknown AI backend: {config.ai_backend}")
