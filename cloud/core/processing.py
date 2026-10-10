"""Procesador de flujos: valida, deduplica por clave y escribe de forma idempotente (ADR-004, ES-06).

Orden que garantiza "efectivamente una vez" sobre un log de "al menos una vez":
  1. leer un lote del log;  2. validar (lo inválido va a la cola de mensajes muertos);
  3. UNA transacción en la BD: lecturas + agregados + offsets;  4. avisar al log (informativo).
Si el proceso muere entre 3 y 4, al reiniciar se retoma desde el offset guardado en la BD y
nada se aplica dos veces. Si muere antes de 3, no se aplicó nada y se relee el lote.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from .model import Message, ProcessorStats
from .ports import ConsumerPort, DeadLetterPort, StorePort, WallClockPort


@dataclass(frozen=True)
class ProcessorConfig:
    max_batch: int = 1000
    max_future_skew_ms: int = 300_000   # una lectura "del futuro" más allá de 5 min es sospechosa
    max_kwh_per_reading: float = 1000.0


class StreamProcessor:
    def __init__(self, consumer: ConsumerPort, store: StorePort, dead_letter: DeadLetterPort,
                 wall_clock: WallClockPort, config: ProcessorConfig = ProcessorConfig()):
        self._consumer, self._store, self._dlq = consumer, store, dead_letter
        self._clock, self._cfg = wall_clock, config
        self.stats = ProcessorStats()

    def reject_reason(self, m: Message) -> str | None:
        if m.reading is None:
            return f"no decodificable: {m.error}"
        r = m.reading
        if not r.meter_id:
            return "meter_id vacío"
        if r.measured_at <= 0:
            return "measured_at inválido"
        if r.measured_at > self._clock.now_ms() + self._cfg.max_future_skew_ms:
            return "measured_at en el futuro"
        if not math.isfinite(r.kwh) or r.kwh < 0 or r.kwh > self._cfg.max_kwh_per_reading:
            return "kwh fuera de rango"
        return None

    def run_once(self, timeout_s: float = 1.0) -> int:
        msgs = self._consumer.poll(self._cfg.max_batch, timeout_s)
        if not msgs:
            return 0
        valid = []
        for m in msgs:
            reason = self.reject_reason(m)
            if reason:
                self._dlq.send(m, reason)
                self.stats.rejected += 1
            else:
                valid.append(m.reading)
        positions = [m.position for m in msgs]
        inserted = self._store.commit_batch(valid, positions)
        self._consumer.commit(positions)
        s = self.stats
        s.consumed += len(msgs)
        s.inserted += inserted
        s.duplicates += len(valid) - inserted
        s.batches += 1
        return len(msgs)

    def run_until_idle(self, idle_polls: int = 3, timeout_s: float = 1.0) -> int:
        """Procesa hasta que el log no entrega nada durante idle_polls consultas seguidas."""
        total, vacias = 0, 0
        while vacias < idle_polls:
            n = self.run_once(timeout_s)
            total += n
            vacias = vacias + 1 if n == 0 else 0
        return total
