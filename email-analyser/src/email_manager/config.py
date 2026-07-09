from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path
from typing import Any, Tuple, Type

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


def _prefect_variable(name: str) -> str | None:
    """Fetch a Prefect Variable value by name, or None if unavailable."""
    api_url = os.environ.get("PREFECT_API_URL", "")
    if not api_url:
        return None
    try:
        req = urllib.request.urlopen(f"{api_url}/variables/name/{name}", timeout=5)
        return json.loads(req.read()).get("value")
    except Exception:
        return None


class _PrefectVariableSource(PydanticBaseSettingsSource):
    """Settings source that reads from the 'worker_config' Prefect Variable.

    Loaded once per Config() instantiation and treated as a lower-priority
    alternative to env vars and dotenv files (i.e. only fills gaps).
    """

    def __init__(self, settings_cls: Type[BaseSettings]) -> None:
        super().__init__(settings_cls)
        raw = _prefect_variable("worker_config")
        # Prefect may return value as an already-parsed dict or as a JSON string
        parsed = raw if isinstance(raw, dict) else (json.loads(raw) if raw else {})
        self._data: dict[str, Any] = {k.lower(): v for k, v in parsed.items()}

    def get_field_value(self, field: Any, field_name: str) -> Tuple[Any, str, bool]:
        value = self._data.get(field_name)
        return value, field_name, self.field_is_complex(field)

    def __call__(self) -> dict[str, Any]:
        return {k: v for k, v in self._data.items() if v is not None}


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
    gmail_bearer_token: str = ""  # if set, use this bearer token instead of local OAuth (for proxy-managed tokens)

    # HubSpot — if set, this account's token is used for HubSpot API calls.
    # Use a placeholder like "HUBSPOT_TOKEN" and let the proxy substitute the real token.
    hubspot_bearer_token: str = ""
    hubspot_owner_email: str = ""  # HubSpot owner email to filter tasks/assignments by


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
    ai_backend: str = "claude"  # "claude", "claude-cli", "codex", or "ollama"
    anthropic_api_key: str = ""
    claude_code_oauth_token: str = ""  # OAuth token placeholder; set to CLAUDE_CODE_OAUTH_TOKEN, proxy fills the real value
    claude_model: str = ""
    extract_events_model: str = ""  # model override for extract_events stage
    codex_model: str = "gpt-5.4-mini"  # model for AI_BACKEND=codex (codex exec CLI)
    ollama_model: str = "llama3.1:8b"
    ollama_url: str = "http://localhost:11434"
    ai_batch_size: int = 10

    # Embeddings
    embedding_backend: str = "voyage"  # "voyage" or "ollama"
    voyage_api_key: str = "VOYAGER_API_KEY"  # placeholder swapped by proxy
    embedding_model_fast: str = "voyage-3-lite"  # 512 dims, cheap, all threads
    embedding_model_quality: str = "voyage-3"  # 1024 dims, important threads only

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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        # Priority: init > env > dotenv > prefect_variable > secrets/defaults
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _PrefectVariableSource(settings_cls),
            file_secret_settings,
        )

    def get_hubspot_account(self) -> "EmailAccount | None":
        """Return the first account that has a hubspot_bearer_token configured."""
        for acct in self.get_accounts():
            if acct.hubspot_bearer_token:
                return acct
        return None

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

        # Try reading from Prefect Variable (works when running in a Prefect worker)
        import os
        api_url = os.environ.get("PREFECT_API_URL", "")
        if api_url:
            try:
                import urllib.request
                req = urllib.request.urlopen(f"{api_url}/variables/name/email_accounts", timeout=5)
                payload = json.loads(req.read())
                value = payload.get("value", "")
                if value:
                    return [EmailAccount(**acct) for acct in json.loads(value)]
            except Exception:
                pass

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
