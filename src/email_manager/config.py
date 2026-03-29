from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Email backend: "imap" or "gmail"
    email_backend: str = "imap"

    # IMAP
    imap_host: str = ""
    imap_user: str = ""
    imap_password: str = ""
    imap_port: int = 993
    imap_use_ssl: bool = True
    imap_folders: list[str] = Field(default_factory=lambda: ["INBOX", "Sent"])

    # Gmail API
    gmail_credentials_path: Path = Path("data/gmail_credentials.json")
    gmail_token_path: Path = Path("data/gmail_token.json")
    gmail_labels: list[str] = Field(default_factory=list)  # empty = all mail

    # AI
    ai_backend: str = "claude"  # "claude", "claude-cli", or "ollama"
    anthropic_api_key: str = ""
    claude_model: str = "claude-sonnet-4-20250514"
    ollama_model: str = "llama3.1:8b"
    ollama_url: str = "http://localhost:11434"
    ai_batch_size: int = 10

    # Database
    db_path: Path = Path("data/email_manager.db")

    @property
    def db_abs_path(self) -> Path:
        if self.db_path.is_absolute():
            return self.db_path
        return Path.cwd() / self.db_path
