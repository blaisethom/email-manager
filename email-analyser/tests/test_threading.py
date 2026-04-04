from __future__ import annotations

import json
import sqlite3

from email_manager.ingestion.parser import parse_raw_email, email_to_db_row
from email_manager.ingestion.threading import (
    UnionFind,
    normalise_subject,
    extract_message_ids,
    compute_threads,
    insert_email_references,
)


def _insert_email(conn: sqlite3.Connection, row: dict) -> int:
    """Insert an email row and populate email_references. Returns the email id."""
    conn.execute(
        """INSERT OR IGNORE INTO emails
        (message_id, thread_id, subject, normalised_subject, from_address, from_name,
         to_addresses, cc_addresses, date, body_text, body_html,
         raw_headers, folder, size_bytes, has_attachments, fetched_at)
        VALUES
        (:message_id, :thread_id, :subject, :normalised_subject, :from_address, :from_name,
         :to_addresses, :cc_addresses, :date, :body_text, :body_html,
         :raw_headers, :folder, :size_bytes, :has_attachments, :fetched_at)""",
        row,
    )
    inserted = conn.execute(
        "SELECT id FROM emails WHERE message_id = ?", (row["message_id"],)
    ).fetchone()
    if inserted:
        raw_headers = json.loads(row["raw_headers"]) if isinstance(row["raw_headers"], str) else row["raw_headers"]
        insert_email_references(conn, inserted[0], raw_headers)
    return inserted[0] if inserted else -1


class TestUnionFind:
    def test_basic_union(self) -> None:
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        assert uf.find("a") == uf.find("c")

    def test_disjoint_sets(self) -> None:
        uf = UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")
        assert uf.find("a") != uf.find("c")

    def test_single_element(self) -> None:
        uf = UnionFind()
        root = uf.find("x")
        assert root == "x"


class TestNormaliseSubject:
    def test_strip_re(self) -> None:
        assert normalise_subject("Re: Hello") == "hello"

    def test_strip_fwd(self) -> None:
        assert normalise_subject("Fwd: Hello") == "hello"

    def test_strip_multiple_prefixes(self) -> None:
        assert normalise_subject("Re: Re: Fwd: Hello") == "hello"

    def test_strip_numbered_re(self) -> None:
        assert normalise_subject("Re[2]: Hello") == "hello"

    def test_empty_subject(self) -> None:
        assert normalise_subject(None) == ""
        assert normalise_subject("") == ""

    def test_no_prefix(self) -> None:
        assert normalise_subject("Hello World") == "hello world"


class TestExtractMessageIds:
    def test_single_id(self) -> None:
        assert extract_message_ids("<abc@example.com>") == ["abc@example.com"]

    def test_multiple_ids(self) -> None:
        ids = extract_message_ids("<a@x.com> <b@x.com> <c@x.com>")
        assert ids == ["a@x.com", "b@x.com", "c@x.com"]

    def test_empty(self) -> None:
        assert extract_message_ids("") == []
        assert extract_message_ids(None) == []


class TestComputeThreads:
    def test_threads_from_references(
        self, test_db: sqlite3.Connection, fixtures_dir
    ) -> None:
        # Insert test emails with email_references populated
        for filename in ["simple.eml", "reply.eml", "thread_chain.eml"]:
            raw = (fixtures_dir / filename).read_bytes()
            em = parse_raw_email(raw, folder="INBOX")
            row = email_to_db_row(em)
            _insert_email(test_db, row)
        test_db.commit()

        compute_threads(test_db)

        # All three emails should be in the same thread
        rows = test_db.execute(
            "SELECT DISTINCT thread_id FROM emails"
        ).fetchall()
        thread_ids = {r["thread_id"] for r in rows}
        assert len(thread_ids) == 1, f"Expected 1 thread, got {len(thread_ids)}: {thread_ids}"

    def test_unrelated_emails_different_threads(
        self, test_db: sqlite3.Connection, fixtures_dir
    ) -> None:
        # Insert unrelated emails
        for filename in ["simple.eml", "html_email.eml"]:
            raw = (fixtures_dir / filename).read_bytes()
            em = parse_raw_email(raw, folder="INBOX")
            row = email_to_db_row(em)
            _insert_email(test_db, row)
        test_db.commit()

        compute_threads(test_db)

        rows = test_db.execute(
            "SELECT DISTINCT thread_id FROM emails"
        ).fetchall()
        thread_ids = {r["thread_id"] for r in rows}
        assert len(thread_ids) == 2, f"Expected 2 threads, got {len(thread_ids)}"


class TestIncrementalThreading:
    """Test the incremental threading path (when some emails already have thread_ids)."""

    def _make_row(self, message_id: str, subject: str, date: str,
                  references: str = "", in_reply_to: str = "") -> dict:
        """Create a minimal email row dict for testing."""
        raw_headers = json.dumps({
            "references": references,
            "in_reply_to": in_reply_to,
        })
        return {
            "message_id": message_id,
            "thread_id": None,
            "subject": subject,
            "normalised_subject": normalise_subject(subject),
            "from_address": "test@example.com",
            "from_name": "Test",
            "to_addresses": json.dumps(["other@example.com"]),
            "cc_addresses": json.dumps([]),
            "date": date,
            "body_text": "test body",
            "body_html": None,
            "raw_headers": raw_headers,
            "folder": "INBOX",
            "size_bytes": 100,
            "has_attachments": 0,
            "fetched_at": "2025-01-01T00:00:00+00:00",
        }

    def test_incremental_links_by_reference(self, test_db: sqlite3.Connection) -> None:
        """A new email referencing an existing threaded email should join that thread."""
        # Insert first email and thread it
        row1 = self._make_row("msg1@test.com", "Hello", "2025-01-01T10:00:00+00:00")
        _insert_email(test_db, row1)
        test_db.commit()

        compute_threads(test_db)

        thread1 = test_db.execute(
            "SELECT thread_id FROM emails WHERE message_id = 'msg1@test.com'"
        ).fetchone()["thread_id"]
        assert thread1 is not None

        # Now insert a reply referencing msg1
        row2 = self._make_row(
            "msg2@test.com", "Re: Hello", "2025-01-01T11:00:00+00:00",
            in_reply_to="<msg1@test.com>",
        )
        _insert_email(test_db, row2)
        test_db.commit()

        # Incremental threading should assign msg2 to the same thread
        compute_threads(test_db)

        thread2 = test_db.execute(
            "SELECT thread_id FROM emails WHERE message_id = 'msg2@test.com'"
        ).fetchone()["thread_id"]
        assert thread2 == thread1

    def test_incremental_subject_fallback(self, test_db: sqlite3.Connection) -> None:
        """Email with same subject within 90 days should join existing thread."""
        row1 = self._make_row("msg1@test.com", "Project Update", "2025-01-01T10:00:00+00:00")
        _insert_email(test_db, row1)
        test_db.commit()
        compute_threads(test_db)

        thread1 = test_db.execute(
            "SELECT thread_id FROM emails WHERE message_id = 'msg1@test.com'"
        ).fetchone()["thread_id"]

        # Same subject, no references, within 90 days
        row2 = self._make_row("msg2@test.com", "Re: Project Update", "2025-01-15T10:00:00+00:00")
        _insert_email(test_db, row2)
        test_db.commit()
        compute_threads(test_db)

        thread2 = test_db.execute(
            "SELECT thread_id FROM emails WHERE message_id = 'msg2@test.com'"
        ).fetchone()["thread_id"]
        assert thread2 == thread1

    def test_incremental_new_thread_for_unrelated(self, test_db: sqlite3.Connection) -> None:
        """Unrelated email gets its own thread."""
        row1 = self._make_row("msg1@test.com", "Topic A", "2025-01-01T10:00:00+00:00")
        _insert_email(test_db, row1)
        test_db.commit()
        compute_threads(test_db)

        row2 = self._make_row("msg2@test.com", "Totally Different", "2025-01-01T11:00:00+00:00")
        _insert_email(test_db, row2)
        test_db.commit()
        compute_threads(test_db)

        t1 = test_db.execute("SELECT thread_id FROM emails WHERE message_id = 'msg1@test.com'").fetchone()["thread_id"]
        t2 = test_db.execute("SELECT thread_id FROM emails WHERE message_id = 'msg2@test.com'").fetchone()["thread_id"]
        assert t1 != t2

    def test_incremental_merge_threads(self, test_db: sqlite3.Connection) -> None:
        """Email referencing messages in two different threads should merge them."""
        # Create two separate threads
        row1 = self._make_row("msg1@test.com", "Thread A", "2025-01-01T10:00:00+00:00")
        row2 = self._make_row("msg2@test.com", "Thread B", "2025-01-01T11:00:00+00:00")
        _insert_email(test_db, row1)
        _insert_email(test_db, row2)
        test_db.commit()
        compute_threads(test_db)

        t1 = test_db.execute("SELECT thread_id FROM emails WHERE message_id = 'msg1@test.com'").fetchone()["thread_id"]
        t2 = test_db.execute("SELECT thread_id FROM emails WHERE message_id = 'msg2@test.com'").fetchone()["thread_id"]
        assert t1 != t2

        # New email references both threads
        row3 = self._make_row(
            "msg3@test.com", "Connecting", "2025-01-02T10:00:00+00:00",
            references="<msg1@test.com> <msg2@test.com>",
        )
        _insert_email(test_db, row3)
        test_db.commit()
        compute_threads(test_db)

        # All three should now be in the same thread
        threads = test_db.execute("SELECT DISTINCT thread_id FROM emails").fetchall()
        assert len(threads) == 1, f"Expected 1 thread after merge, got {len(threads)}"

    def test_thread_summaries_updated(self, test_db: sqlite3.Connection) -> None:
        """Thread table should be updated with correct counts."""
        row1 = self._make_row("msg1@test.com", "Hello", "2025-01-01T10:00:00+00:00")
        row2 = self._make_row(
            "msg2@test.com", "Re: Hello", "2025-01-01T11:00:00+00:00",
            in_reply_to="<msg1@test.com>",
        )
        _insert_email(test_db, row1)
        _insert_email(test_db, row2)
        test_db.commit()
        compute_threads(test_db)

        thread = test_db.execute("SELECT * FROM threads").fetchone()
        assert thread is not None
        assert thread["email_count"] == 2

    def test_subject_fallback_respects_90_day_window(self, test_db: sqlite3.Connection) -> None:
        """Emails with same subject but >90 days apart should NOT be grouped."""
        row1 = self._make_row("msg1@test.com", "Weekly Meeting", "2025-01-01T10:00:00+00:00")
        _insert_email(test_db, row1)
        test_db.commit()
        compute_threads(test_db)

        # 120 days later — same subject, should NOT match
        row2 = self._make_row("msg2@test.com", "Weekly Meeting", "2025-05-01T10:00:00+00:00")
        _insert_email(test_db, row2)
        test_db.commit()
        compute_threads(test_db)

        t1 = test_db.execute("SELECT thread_id FROM emails WHERE message_id = 'msg1@test.com'").fetchone()["thread_id"]
        t2 = test_db.execute("SELECT thread_id FROM emails WHERE message_id = 'msg2@test.com'").fetchone()["thread_id"]
        assert t1 != t2

    def test_force_rebuild(self, test_db: sqlite3.Connection) -> None:
        """force_rebuild should reassign all threads from scratch."""
        row1 = self._make_row("msg1@test.com", "Hello", "2025-01-01T10:00:00+00:00")
        row2 = self._make_row(
            "msg2@test.com", "Re: Hello", "2025-01-01T11:00:00+00:00",
            in_reply_to="<msg1@test.com>",
        )
        _insert_email(test_db, row1)
        _insert_email(test_db, row2)
        test_db.commit()

        # First thread normally
        compute_threads(test_db)

        # Force rebuild
        updated = compute_threads(test_db, force_rebuild=True)
        assert updated == 2

        threads = test_db.execute("SELECT DISTINCT thread_id FROM emails").fetchall()
        assert len(threads) == 1
