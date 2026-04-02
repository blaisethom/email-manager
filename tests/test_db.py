from __future__ import annotations

import sqlite3

from email_manager.db import fetchall, fetchone, execute


class TestDatabase:
    def test_schema_created(self, test_db: sqlite3.Connection) -> None:
        tables = fetchall(
            test_db,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        )
        table_names = {r["name"] for r in tables}
        expected = {
            "emails", "sync_state", "contacts", "threads",
            "projects", "email_projects", "entities", "pipeline_runs",
            "schema_version", "email_references",
        }
        assert expected.issubset(table_names)

    def test_schema_version(self, test_db: sqlite3.Connection) -> None:
        row = fetchone(test_db, "SELECT version FROM schema_version")
        assert row is not None
        assert row["version"] == 3

    def test_insert_and_query_email(self, test_db: sqlite3.Connection) -> None:
        execute(
            test_db,
            """INSERT INTO emails
            (message_id, from_address, date, fetched_at)
            VALUES (?, ?, ?, ?)""",
            ("test@msg.id", "sender@test.com", "2025-03-10T09:00:00", "2025-03-10T10:00:00"),
        )
        test_db.commit()

        row = fetchone(test_db, "SELECT * FROM emails WHERE message_id = ?", ("test@msg.id",))
        assert row is not None
        assert row["from_address"] == "sender@test.com"

    def test_unique_message_id(self, test_db: sqlite3.Connection) -> None:
        execute(
            test_db,
            """INSERT INTO emails
            (message_id, from_address, date, fetched_at)
            VALUES (?, ?, ?, ?)""",
            ("unique@msg.id", "a@test.com", "2025-03-10T09:00:00", "2025-03-10T10:00:00"),
        )
        # Duplicate should be ignored with INSERT OR IGNORE
        execute(
            test_db,
            """INSERT OR IGNORE INTO emails
            (message_id, from_address, date, fetched_at)
            VALUES (?, ?, ?, ?)""",
            ("unique@msg.id", "b@test.com", "2025-03-10T09:00:00", "2025-03-10T10:00:00"),
        )
        test_db.commit()

        rows = fetchall(test_db, "SELECT * FROM emails WHERE message_id = ?", ("unique@msg.id",))
        assert len(rows) == 1
        assert rows[0]["from_address"] == "a@test.com"
