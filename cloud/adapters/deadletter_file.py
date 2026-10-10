"""Mensajes muertos a un archivo JSON-lines. En producción sería un tema aparte del log (telemetry.dlq)."""
from __future__ import annotations

import json

from ..core.model import Message


class FileDeadLetter:
    def __init__(self, path: str):
        self._f = open(path, "a", encoding="utf-8")

    def send(self, message: Message, reason: str) -> None:
        p = message.position
        self._f.write(json.dumps({"topic": p.topic, "partition": p.partition, "offset": p.offset,
                                  "reason": reason}, ensure_ascii=False) + "\n")
        self._f.flush()
