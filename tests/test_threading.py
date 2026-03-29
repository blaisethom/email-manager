from __future__ import annotations

import sqlite3

from email_manager.ingestion.parser import parse_raw_email, email_to_db_row
from email_manager.ingestion.threading import (
    UnionFind,
    normalise_subject,
    extract_message_ids,
    compute_threads,
)


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
        # Insert test emails
        for filename in ["simple.eml", "reply.eml", "thread_chain.eml"]:
            raw = (fixtures_dir / filename).read_bytes()
            em = parse_raw_email(raw, folder="INBOX")
            row = email_to_db_row(em)
            test_db.execute(
                """INSERT OR IGNORE INTO emails
                (message_id, thread_id, subject, from_address, from_name,
                 to_addresses, cc_addresses, date, body_text, body_html,
                 raw_headers, folder, size_bytes, has_attachments, fetched_at)
                VALUES
                (:message_id, :thread_id, :subject, :from_address, :from_name,
                 :to_addresses, :cc_addresses, :date, :body_text, :body_html,
                 :raw_headers, :folder, :size_bytes, :has_attachments, :fetched_at)""",
                row,
            )
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
            test_db.execute(
                """INSERT OR IGNORE INTO emails
                (message_id, thread_id, subject, from_address, from_name,
                 to_addresses, cc_addresses, date, body_text, body_html,
                 raw_headers, folder, size_bytes, has_attachments, fetched_at)
                VALUES
                (:message_id, :thread_id, :subject, :from_address, :from_name,
                 :to_addresses, :cc_addresses, :date, :body_text, :body_html,
                 :raw_headers, :folder, :size_bytes, :has_attachments, :fetched_at)""",
                row,
            )
        test_db.commit()

        compute_threads(test_db)

        rows = test_db.execute(
            "SELECT DISTINCT thread_id FROM emails"
        ).fetchall()
        thread_ids = {r["thread_id"] for r in rows}
        assert len(thread_ids) == 2, f"Expected 2 threads, got {len(thread_ids)}"
