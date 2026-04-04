from __future__ import annotations

import sqlite3
from pathlib import Path

from email_manager.config import Config
from email_manager.memory.base import MemoryBackend, MemoryStrategy


def get_memory_backends(config: Config, conn: sqlite3.Connection) -> list[MemoryBackend]:
    backends = []

    if config.memory_backend in ("sqlite", "both"):
        from email_manager.memory.sqlite_backend import SQLiteMemoryBackend
        backends.append(SQLiteMemoryBackend(conn))

    if config.memory_backend in ("markdown", "both"):
        from email_manager.memory.markdown_backend import MarkdownMemoryBackend
        base_dir = config.memory_dir if config.memory_dir.is_absolute() else Path.cwd() / config.memory_dir
        backends.append(MarkdownMemoryBackend(base_dir))

    if not backends:
        from email_manager.memory.sqlite_backend import SQLiteMemoryBackend
        backends.append(SQLiteMemoryBackend(conn))

    return backends


def get_memory_strategy(config: Config) -> MemoryStrategy:
    if config.memory_strategy == "detailed":
        from email_manager.memory.strategies.detailed import DetailedStrategy
        return DetailedStrategy()
    else:
        from email_manager.memory.strategies.default import DefaultStrategy
        return DefaultStrategy()
