from __future__ import annotations

import email
import email.policy
import json
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import parseaddr, parsedate_to_datetime

from bs4 import BeautifulSoup
import html2text

from email_manager.models import Email


def parse_raw_email(raw_bytes: bytes, folder: str | None = None) -> Email:
    msg = email.message_from_bytes(raw_bytes, policy=email.policy.default)
    return _parse_message(msg, folder=folder, size_bytes=len(raw_bytes))


def _parse_message(
    msg: EmailMessage, folder: str | None = None, size_bytes: int | None = None
) -> Email:
    message_id = msg.get("Message-ID", "").strip("<>")
    subject = msg.get("Subject", "")

    from_name, from_address = parseaddr(msg.get("From", ""))
    to_addresses = _parse_address_list(msg.get_all("To"))
    cc_addresses = _parse_address_list(msg.get_all("Cc"))

    date = _parse_date(msg.get("Date"))

    body_text, body_html = _extract_body(msg)

    raw_headers = {
        "references": msg.get("References", ""),
        "in_reply_to": msg.get("In-Reply-To", ""),
        "list_id": msg.get("List-Id", ""),
    }

    has_attachments = _has_attachments(msg)

    return Email(
        message_id=message_id,
        subject=subject,
        from_address=from_address.lower(),
        from_name=from_name or None,
        to_addresses=to_addresses,
        cc_addresses=cc_addresses,
        date=date,
        body_text=body_text,
        body_html=body_html,
        raw_headers=raw_headers,
        folder=folder,
        size_bytes=size_bytes,
        has_attachments=has_attachments,
    )


def _parse_address_list(headers: list | None) -> list[str]:
    if not headers:
        return []
    addresses = []
    for header in headers:
        if header is None:
            continue
        for part in str(header).split(","):
            _, addr = parseaddr(part.strip())
            if addr:
                addresses.append(addr.lower())
    return addresses


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(timezone.utc)
    try:
        return parsedate_to_datetime(date_str)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _extract_body(msg: EmailMessage) -> tuple[str | None, str | None]:
    body_text = None
    body_html = None

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                continue
            if content_type == "text/plain" and body_text is None:
                body_text = _decode_payload(part)
            elif content_type == "text/html" and body_html is None:
                body_html = _decode_payload(part)
    else:
        content_type = msg.get_content_type()
        if content_type == "text/plain":
            body_text = _decode_payload(msg)
        elif content_type == "text/html":
            body_html = _decode_payload(msg)

    # If we only have HTML, convert to text
    if body_text is None and body_html is not None:
        body_text = html_to_text(body_html)

    return body_text, body_html


def _decode_payload(part: EmailMessage) -> str | None:
    try:
        content = part.get_content()
        if isinstance(content, str):
            return content
        if isinstance(content, bytes):
            return content.decode("utf-8", errors="replace")
    except (KeyError, LookupError, UnicodeDecodeError):
        pass
    return None


def _has_attachments(msg: EmailMessage) -> bool:
    if not msg.is_multipart():
        return False
    for part in msg.walk():
        disposition = str(part.get("Content-Disposition", ""))
        if "attachment" in disposition:
            return True
    return False


def html_to_text(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = True
    h.body_width = 0
    return h.handle(html).strip()


def email_to_db_row(em: Email) -> dict:
    return {
        "message_id": em.message_id,
        "thread_id": em.thread_id,
        "subject": em.subject,
        "from_address": em.from_address,
        "from_name": em.from_name,
        "to_addresses": json.dumps(em.to_addresses),
        "cc_addresses": json.dumps(em.cc_addresses),
        "date": em.date.isoformat(),
        "body_text": em.body_text,
        "body_html": em.body_html,
        "raw_headers": json.dumps(em.raw_headers),
        "folder": em.folder,
        "size_bytes": em.size_bytes,
        "has_attachments": int(em.has_attachments),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
