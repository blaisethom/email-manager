from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class Email(BaseModel):
    message_id: str
    thread_id: str | None = None
    subject: str | None = None
    from_address: str
    from_name: str | None = None
    to_addresses: list[str] = []
    cc_addresses: list[str] = []
    date: datetime
    body_text: str | None = None
    body_html: str | None = None
    raw_headers: dict = {}
    folder: str | None = None
    size_bytes: int | None = None
    has_attachments: bool = False


class Contact(BaseModel):
    email: str
    name: str | None = None
    company: str | None = None
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    email_count: int = 0
    sent_count: int = 0
    received_count: int = 0


class Thread(BaseModel):
    thread_id: str
    subject: str | None = None
    email_count: int = 0
    first_date: datetime | None = None
    last_date: datetime | None = None
    participants: list[str] = []
    summary: str | None = None


class Project(BaseModel):
    id: int | None = None
    name: str
    description: str | None = None
    department: str | None = None
    workstream: str | None = None
    is_auto: bool = True
