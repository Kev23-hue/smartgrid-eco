"""Consumidor del log en archivo (sustituto de Kafka para ensayos sin Docker). Una sola partición (0).
El offset de un mensaje es id-1; el siguiente a leer sale de la BD, igual que con Kafka."""
from __future__ import annotations

import sqlite3
import time
from typing import Callable, Sequence

from edge.adapters.codec import reading_from_json
from ..core.model import Message, Position


class FileLogConsumer:
    def __init__(self, path: str, topic: str, offset_lookup: Callable[[str, int], int | None]):
        self._db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)")
        self._topic = topic
        sig = offset_lookup(topic, 0)
        self._next = sig if sig is not None else 0

    def poll(self, max_messages: int, timeout_s: float) -> list[Message]:
        fin = time.time() + timeout_s
        while True:
            rows = self._db.execute("SELECT id, body FROM log WHERE id > ? ORDER BY id LIMIT ?",
                                    (self._next, max_messages)).fetchall()
            if rows or time.time() >= fin:
                break
            time.sleep(0.02)
        out: list[Message] = []
        for id_, body in rows:
            pos = Position(self._topic, 0, id_ - 1)
            try:
                out.append(Message(pos, reading_from_json(body)))
            except Exception as e:  # noqa: BLE001
                out.append(Message(pos, None, str(e)))
            self._next = id_
        return out

    def commit(self, positions: Sequence[Position]) -> None:
        pass

    def close(self) -> None:
        self._db.close()
