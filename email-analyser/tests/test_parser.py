from __future__ import annotations

from email_manager.ingestion.parser import parse_raw_email, html_to_text


class TestParseRawEmail:
    def test_simple_email(self, sample_email_bytes: bytes) -> None:
        email = parse_raw_email(sample_email_bytes, folder="INBOX")
        assert email.message_id == "msg001@example.com"
        assert email.from_address == "alice@example.com"
        assert email.from_name == "Alice Smith"
        assert email.subject == "Project Alpha kickoff"
        assert email.folder == "INBOX"
        assert "bob@example.com" in email.to_addresses
        assert "Project Alpha" in (email.body_text or "")
        assert email.has_attachments is False

    def test_reply_email(self, reply_email_bytes: bytes) -> None:
        email = parse_raw_email(reply_email_bytes)
        assert email.message_id == "msg002@example.com"
        assert email.from_address == "bob@example.com"
        assert email.subject == "Re: Project Alpha kickoff"
        assert email.raw_headers["in_reply_to"] == "<msg001@example.com>"
        assert "<msg001@example.com>" in email.raw_headers["references"]

    def test_html_email(self, html_email_bytes: bytes) -> None:
        email = parse_raw_email(html_email_bytes)
        assert email.message_id == "msg003@company.org"
        assert email.from_address == "carol@company.org"
        assert len(email.to_addresses) == 2
        assert "bob@example.com" in email.to_addresses
        assert "dave@example.com" in email.cc_addresses
        # Should have both plain text and HTML
        assert email.body_text is not None
        assert email.body_html is not None
        assert "Revenue" in email.body_text

    def test_thread_chain(self, thread_chain_bytes: bytes) -> None:
        email = parse_raw_email(thread_chain_bytes)
        assert email.message_id == "msg004@example.com"
        refs = email.raw_headers["references"]
        assert "<msg001@example.com>" in refs
        assert "<msg002@example.com>" in refs


class TestHtmlToText:
    def test_basic_html(self) -> None:
        html = "<p>Hello <strong>world</strong></p>"
        text = html_to_text(html)
        assert "Hello" in text
        assert "world" in text

    def test_list_html(self) -> None:
        html = "<ul><li>Item 1</li><li>Item 2</li></ul>"
        text = html_to_text(html)
        assert "Item 1" in text
        assert "Item 2" in text
