from __future__ import annotations

import json
import re
from pathlib import Path

from email_manager.memory.base import ContactMemory


class MarkdownMemoryBackend:
    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, email: str) -> Path:
        # Sanitize email for filename
        safe = email.replace("@", "_at_").replace("/", "_")
        return self._base_dir / f"{safe}.md"

    def store(self, memory: ContactMemory) -> None:
        path = self._path_for(memory.email)
        lines = [
            "---",
            f"email: {memory.email}",
            f"name: {memory.name or ''}",
            f"relationship: {memory.relationship}",
            f"model_used: {memory.model_used}",
            f"strategy_used: {memory.strategy_used}",
            f"version: {memory.version}",
            f"generated_at: {memory.generated_at}",
            f"emails_hash: {memory.emails_hash}",
            "---",
            "",
            f"# {memory.name or memory.email}",
            "",
            memory.summary,
            "",
        ]

        if memory.discussions:
            lines.append("## Discussions")
            lines.append("")
            for d in memory.discussions:
                status = d.get("status", "unknown")
                status_icon = {"active": "🟢", "waiting": "🟡", "resolved": "⚪"}.get(status, "❓")
                lines.append(f"### {status_icon} {d.get('topic', 'Untitled')} [{status}]")
                lines.append("")
                lines.append(d.get("summary", ""))
                lines.append("")

        if memory.key_facts:
            lines.append("## Key Facts")
            lines.append("")
            for fact in memory.key_facts:
                lines.append(f"- {fact}")
            lines.append("")

        path.write_text("\n".join(lines))

    def load(self, email: str) -> ContactMemory | None:
        path = self._path_for(email)
        if not path.exists():
            return None
        return _parse_markdown(path.read_text(), email)

    def load_all(self) -> list[ContactMemory]:
        memories = []
        for path in sorted(self._base_dir.glob("*.md")):
            text = path.read_text()
            # Extract email from frontmatter
            match = re.search(r"^email:\s*(.+)$", text, re.MULTILINE)
            if match:
                mem = _parse_markdown(text, match.group(1).strip())
                if mem:
                    memories.append(mem)
        return memories

    def delete(self, email: str) -> None:
        path = self._path_for(email)
        if path.exists():
            path.unlink()


def _parse_markdown(text: str, email: str) -> ContactMemory | None:
    """Parse a markdown memory file back into a ContactMemory."""
    # Extract frontmatter
    fm_match = re.match(r"^---\n(.+?)\n---", text, re.DOTALL)
    if not fm_match:
        return None

    fm = {}
    for line in fm_match.group(1).split("\n"):
        if ":" in line:
            key, _, value = line.partition(":")
            fm[key.strip()] = value.strip()

    return ContactMemory(
        email=fm.get("email", email),
        name=fm.get("name") or None,
        relationship=fm.get("relationship", "unknown"),
        summary="",  # Not round-tripped perfectly, but the structured data is in SQLite
        discussions=[],
        key_facts=[],
        generated_at=fm.get("generated_at", ""),
        model_used=fm.get("model_used", ""),
        strategy_used=fm.get("strategy_used", ""),
        version=int(fm.get("version", "1")),
        emails_hash=fm.get("emails_hash", ""),
    )
