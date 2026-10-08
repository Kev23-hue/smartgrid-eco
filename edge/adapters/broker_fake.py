"""Log de eventos simulado en memoria, con fallos controlables. Solo para pruebas y demostraciones."""
from __future__ import annotations

from typing import Sequence

from ..core.model import Reading
from ..core.ports import BrokerUnavailable


class FakeBroker:
    def __init__(self):
        self.online = True
        self.lose_next_acks = 0     # guarda el lote pero responde error: simula un ack perdido
        self.received: list[Reading] = []
        self.publish_calls = 0

    def publish(self, batch: Sequence[Reading]) -> None:
        self.publish_calls += 1
        if not self.online:
            raise BrokerUnavailable("sin conexión (simulado)")
        self.received.extend(batch)
        if self.lose_next_acks > 0:
            self.lose_next_acks -= 1
            raise BrokerUnavailable("ack perdido (simulado)")

    def unique_keys(self) -> set[str]:
        return {r.key for r in self.received}
