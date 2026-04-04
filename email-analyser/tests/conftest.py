from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from email_manager.config import Config
from email_manager.db import get_db

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def sample_email_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "simple.eml").read_bytes()


@pytest.fixture
def reply_email_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "reply.eml").read_bytes()


@pytest.fixture
def html_email_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "html_email.eml").read_bytes()


@pytest.fixture
def thread_chain_bytes(fixtures_dir: Path) -> bytes:
    return (fixtures_dir / "thread_chain.eml").read_bytes()


@pytest.fixture
def test_config(tmp_path: Path) -> Config:
    return Config(
        db_path=tmp_path / "test.db",
        imap_host="imap.test.com",
        imap_user="test@test.com",
        imap_password="test",
    )


@pytest.fixture
def test_db(test_config: Config) -> sqlite3.Connection:
    conn = get_db(test_config)
    yield conn
    conn.close()
