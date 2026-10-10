from __future__ import annotations

from ..core.model import Message


class MemoryDeadLetter:
    def __init__(self):
        self.items: list[tuple[Message, str]] = []

    def send(self, message: Message, reason: str) -> None:
        self.items.append((message, reason))
