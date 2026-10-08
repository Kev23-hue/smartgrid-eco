"""Puertos: las interfaces que define el núcleo. Los adaptadores las implementan."""
from __future__ import annotations

from typing import Protocol, Sequence

from .model import BatteryState, Decision, Policy, Reading


class GenerationSourcePort(Protocol):
    """Una fuente de generación (solar, eólica, biomasa...). ES-04: una fuente nueva = un adaptador nuevo."""
    name: str

    def read_kw(self) -> float: ...


class DemandPort(Protocol):
    def read_kw(self) -> float: ...


class ActuatorPort(Protocol):
    """Inversores y baterías."""

    def read_battery(self) -> BatteryState: ...

    def apply(self, decision: Decision) -> None: ...


class AuditPort(Protocol):
    def record(self, event: str, payload: dict) -> None: ...


class PolicyPort(Protocol):
    def load(self) -> Policy: ...


class ClockPort(Protocol):
    def now_ns(self) -> int: ...


class BrokerUnavailable(Exception):
    """El log de eventos no confirmó el lote (red caída, broker caído o tiempo agotado)."""


class QueueFull(Exception):
    """La cola en disco alcanzó su límite. Es pérdida explícita y contada, no silenciosa."""


class QueuePort(Protocol):
    """Cola duradera en el borde. Orden de llegada; sobrevive a reinicios."""

    def put_many(self, readings: Sequence[Reading]) -> int:
        """Guarda las lecturas en una sola transacción. Devuelve cuántas eran nuevas
        (ignora las repetidas por clave). Lanza QueueFull si no hay espacio."""

    def peek(self, n: int) -> list[tuple[int, Reading]]:
        """Las n más antiguas, sin quitarlas, con su número de cola."""

    def ack(self, seqs: Sequence[int]) -> None:
        """Quita de la cola las ya confirmadas."""

    def size(self) -> int: ...


class BrokerPort(Protocol):
    """Log de eventos de la nube (Redpanda/Kafka)."""

    def publish(self, batch: Sequence[Reading]) -> None:
        """Entrega el lote completo o lanza BrokerUnavailable. Nunca confirma a medias:
        si hay duda, lanza, y el lote se reenvía (al menos una vez)."""
