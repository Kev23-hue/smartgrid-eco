"""Modelo del lado nube (procesador de flujos y facturación). Sin dependencias externas.

Reading es el contrato de mensaje entre el borde y la nube; en un despliegue real viviría
en una biblioteca compartida. Aquí se reutiliza el mismo tipo para que no pueda divergir.
"""
from __future__ import annotations

from dataclasses import dataclass

from edge.core.model import Reading  # noqa: F401  (contrato compartido borde-nube)

INTERVAL_MS = 900_000          # intervalo de facturación: 15 minutos
EXPECTED_PER_INTERVAL = 180    # 900 s / 5 s por lectura


def interval_start_ms(measured_at_ms: int) -> int:
    return measured_at_ms // INTERVAL_MS * INTERVAL_MS


@dataclass(frozen=True)
class Position:
    """Dónde está un mensaje en el log: tema, partición y desplazamiento (offset)."""
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class Message:
    position: Position
    reading: Reading | None   # None si no se pudo decodificar
    error: str | None = None  # motivo de la falla de decodificación


@dataclass(frozen=True)
class IntervalAggregate:
    meter_id: str
    interval_start: int       # ms Unix, múltiplo de 15 minutos
    n: int                    # lecturas distintas en el intervalo
    sum_kwh: float

    @property
    def complete(self) -> bool:
        return self.n == EXPECTED_PER_INTERVAL

    @property
    def missing(self) -> int:
        return max(0, EXPECTED_PER_INTERVAL - self.n)


@dataclass
class ProcessorStats:
    consumed: int = 0
    rejected: int = 0      # a la cola de mensajes muertos
    inserted: int = 0      # lecturas nuevas
    duplicates: int = 0    # lecturas que ya existían (descartadas por la clave)
    batches: int = 0
