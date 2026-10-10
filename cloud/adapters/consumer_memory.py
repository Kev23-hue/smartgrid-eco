"""Log en memoria con particiones y offsets, que implementa BrokerPort (escritura) y ConsumerPort (lectura).

Imita lo esencial de Kafka: cada mensaje tiene (tema, partición, offset); la partición se elige por
hash de la clave, así que el orden solo se garantiza dentro de una partición; se puede releer desde un offset.
"""
from __future__ import annotations

import zlib
from typing import Sequence

from ..core.model import Message, Position, Reading
from edge.adapters.codec import reading_from_json, reading_to_json
from edge.core.ports import BrokerUnavailable as _BU


class MemoryLog:
    def __init__(self, topic: str = "telemetry.raw", partitions: int = 4):
        self.topic = topic
        self._parts: list[list[str]] = [[] for _ in range(partitions)]
        self.online = True
        self.lose_next_acks = 0

    # --- lado productor (BrokerPort del borde) ---
    def publish(self, batch: Sequence[Reading]) -> None:
        if not self.online:
            raise _BU("sin conexión (simulado)")
        for r in batch:
            p = zlib.crc32(r.meter_id.encode()) % len(self._parts)
            self._parts[p].append(reading_to_json(r))
        if self.lose_next_acks > 0:
            self.lose_next_acks -= 1
            raise _BU("ack perdido (simulado)")

    def inject_raw(self, partition: int, text: str) -> None:
        self._parts[partition].append(text)

    def partition_sizes(self) -> list[int]:
        return [len(p) for p in self._parts]

    def total(self) -> int:
        return sum(len(p) for p in self._parts)


class MemoryConsumer:
    def __init__(self, log: MemoryLog, start_offsets: dict[int, int] | None = None):
        self._log = log
        self._next = {p: (start_offsets or {}).get(p, 0) for p in range(len(log._parts))}

    def poll(self, max_messages: int, timeout_s: float) -> list[Message]:
        out: list[Message] = []
        for p, part in enumerate(self._log._parts):
            while self._next[p] < len(part) and len(out) < max_messages:
                off = self._next[p]
                pos = Position(self._log.topic, p, off)
                try:
                    out.append(Message(pos, reading_from_json(part[off])))
                except Exception as e:  # noqa: BLE001
                    out.append(Message(pos, None, str(e)))
                self._next[p] += 1
        return out

    def commit(self, positions: Sequence[Position]) -> None:
        pass  # el estado verdadero está en la BD
