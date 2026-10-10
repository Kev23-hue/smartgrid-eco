"""Puertos del procesador de flujos."""
from __future__ import annotations

from typing import Protocol, Sequence

from .model import IntervalAggregate, Message, Position, Reading


class ConsumerPort(Protocol):
    def poll(self, max_messages: int, timeout_s: float) -> list[Message]: ...

    def commit(self, positions: Sequence[Position]) -> None:
        """Informa al log hasta dónde se procesó. Es informativo: manda el offset guardado en la BD."""


class StorePort(Protocol):
    def commit_batch(self, readings: Sequence[Reading], positions: Sequence[Position]) -> int:
        """UNA transacción: inserta las lecturas ignorando claves existentes, recalcula los
        agregados de los intervalos tocados y guarda los offsets. Devuelve las lecturas nuevas.
        O todo o nada: así una caída a medias no deja agregados ni offsets desfasados."""

    def next_offset(self, topic: str, partition: int) -> int | None:
        """Siguiente offset por leer según la BD, o None si es la primera vez."""

    def closed_aggregates(self, closed_before_ms: int) -> list[IntervalAggregate]:
        """Agregados de intervalos que terminaron antes de closed_before_ms."""


class DeadLetterPort(Protocol):
    def send(self, message: Message, reason: str) -> None: ...


class WallClockPort(Protocol):
    def now_ms(self) -> int:
        """Hora de pared en ms Unix (no el reloj monotónico del borde)."""
