from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EmailAccount(BaseModel):
    """Configuration for a single email account."""

    name: str = ""  # friendly label, e.g. "personal", "work"
    backend: str = "imap"  # "imap" or "gmail"

    # IMAP
    imap_host: str = ""
    imap_user: str = ""
    imap_password: str = ""
    imap_port: int = 993
    imap_use_ssl: bool = True
    imap_folders: list[str] = Field(default_factory=lambda: ["INBOX", "Sent"])

    # Gmail API
    gmail_credentials_path: Path = Path("../data/gmail_credentials.json")
    gmail_token_path: Path = Path("../data/gmail_token.json")
    gmail_labels: list[str] = Field(default_factory=list)


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Email accounts — loaded from accounts.json or fall back to legacy env vars
    accounts: list[EmailAccount] = Field(default_factory=list)

    # Legacy single-account env vars (used if no accounts.json exists)
    email_backend: str = "imap"
    imap_host: str = ""
    imap_user: str = ""
    imap_password: str = ""
    imap_port: int = 993
    imap_use_ssl: bool = True
    imap_folders: list[str] = Field(default_factory=lambda: ["INBOX", "Sent"])
    gmail_credentials_path: Path = Path("../data/gmail_credentials.json")
    gmail_token_path: Path = Path("../data/gmail_token.json")
    gmail_labels: list[str] = Field(default_factory=list)

    # AI
    ai_backend: str = "claude"  # "claude", "claude-cli", or "ollama"
    anthropic_api_key: str = ""
    claude_model: str = ""
    extract_events_model: str = ""  # model override for extract_events stage (e.g. claude-sonnet-4-6)
    ollama_model: str = "llama3.1:8b"
    ollama_url: str = "http://localhost:11434"
    ai_batch_size: int = 10

    # Memory
    memory_backend: str = "both"  # "sqlite", "markdown", or "both"
    memory_strategy: str = "default"  # "default" or "detailed"
    memory_dir: Path = Path("../data/memories")

    # Company labels
    company_labels_path: Path | None = None  # path to company_labels.yaml/.json
    homepage_max_workers: int = 10  # concurrent threads for homepage fetching

    # Discussion categories
    discussion_categories_path: Path | None = None  # path to discussion_categories.yaml

    # Database
    db_backend: str = "sqlite"  # "sqlite" or "postgres"
    db_path: Path = Path("../data/email_manager.db")  # SQLite file path
    db_url: str = ""  # PostgreSQL URL, e.g. "postgresql://user:pass@host:5432/dbname"
    postgres_url: str = ""  # Alias for db_url (deprecated)

    # Accounts config file
    accounts_path: Path = Path("accounts.json")

    @property
    def db_abs_path(self) -> Path:
        if self.db_path.is_absolute():
            return self.db_path
        return Path.cwd() / self.db_path

    def get_accounts(self) -> list[EmailAccount]:
        """Get email accounts. Loads from accounts.json if it exists, otherwise falls back to legacy env vars."""
        if self.accounts:
            return self.accounts

        # Try loading from accounts.json
        accounts_file = self.accounts_path if self.accounts_path.is_absolute() else Path.cwd() / self.accounts_path
        if accounts_file.exists():
            data = json.loads(accounts_file.read_text())
            return [EmailAccount(**acct) for acct in data]

        # Fall back to legacy single-account config
        return [
            EmailAccount(
                name=self.email_backend,
                backend=self.email_backend,
                imap_host=self.imap_host,
                imap_user=self.imap_user,
                imap_password=self.imap_password,
                imap_port=self.imap_port,
                imap_use_ssl=self.imap_use_ssl,
                imap_folders=self.imap_folders,
                gmail_credentials_path=self.gmail_credentials_path,
                gmail_token_path=self.gmail_token_path,
                gmail_labels=self.gmail_labels,
            )
        ]
