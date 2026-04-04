from __future__ import annotations


class ConversationContext:
    """Manages conversation history with truncation to stay within context limits."""

    def __init__(self, max_messages: int = 40) -> None:
        self._messages: list[dict] = []
        self._max_messages = max_messages

    @property
    def messages(self) -> list[dict]:
        return self._messages

    def add_user(self, content: str) -> None:
        self._messages.append({"role": "user", "content": content})
        self._truncate()

    def add_assistant(self, content) -> None:
        """Add assistant message. Content can be a string or list of content blocks."""
        self._messages.append({"role": "assistant", "content": content})
        self._truncate()

    def add_tool_result(self, tool_use_id: str, result: str) -> None:
        self._messages.append({
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": result,
                }
            ],
        })
        self._truncate()

    def _truncate(self) -> None:
        if len(self._messages) > self._max_messages:
            # Keep the first message (for context) and the most recent ones
            keep = self._max_messages - 2
            self._messages = self._messages[:1] + self._messages[-keep:]
