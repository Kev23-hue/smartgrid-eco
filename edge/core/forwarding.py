"""Reenviador (store-and-forward): ADR-002, ADR-004; escenarios ES-01 y ES-03.

Regla central: ingerir NUNCA depende de la nube. Primero se guarda en la cola en disco;
después se intenta publicar. Una lectura sale de la cola solo cuando el log la confirmó.
Entrega al menos una vez: si el ack se pierde, el lote se reenvía y puede haber duplicados.
Los duplicados son inofensivos porque el procesador escribe con clave (meter_id, measured_at).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

from .model import Reading
from .ports import BrokerPort, BrokerUnavailable, ClockPort, QueueFull, QueuePort


@dataclass(frozen=True)
class ForwarderConfig:
    batch_size: int = 500
    backoff_base_ms: int = 200
    backoff_max_ms: int = 30_000


@dataclass
class ForwarderStats:
    ingested: int = 0              # lecturas nuevas guardadas en la cola
    duplicates_ignored: int = 0    # repetidas que llegaron al borde (ya estaban en cola)
    rejected_full: int = 0         # perdidas por cola llena (se cuentan y se alarman)
    published: int = 0             # lecturas confirmadas por el log y quitadas de la cola
    batches_failed: int = 0
    consecutive_failures: int = 0


class Forwarder:
    def __init__(self, queue: QueuePort, broker: BrokerPort, clock: ClockPort,
                 config: ForwarderConfig = ForwarderConfig(),
                 jitter: Callable[[], float] = lambda: 1.0):
        self._queue = queue
        self._broker = broker
        self._clock = clock
        self._cfg = config
        self._jitter = jitter  # factor en (0, 1]: desincroniza los reintentos de las 12 microredes
        self._next_attempt_ns = 0
        self.stats = ForwarderStats()

    # --- entrada: no toca la red ---
    def ingest_many(self, readings: Sequence[Reading]) -> int:
        """Guarda un grupo de lecturas en una sola transacción. Devuelve las nuevas guardadas."""
        if not readings:
            return 0
        try:
            new = self._queue.put_many(readings)
        except QueueFull:
            self.stats.rejected_full += len(readings)
            return 0
        self.stats.ingested += new
        self.stats.duplicates_ignored += len(readings) - new
        return new

    def ingest(self, reading: Reading) -> bool:
        return self.ingest_many([reading]) == 1

    # --- salida ---
    @property
    def backlog(self) -> int:
        return self._queue.size()

    @property
    def next_attempt_ns(self) -> int:
        return self._next_attempt_ns

    def tick(self) -> int:
        """Un intento de envío si ya pasó la espera de reintento. Devuelve lecturas confirmadas."""
        if self._clock.now_ns() < self._next_attempt_ns:
            return 0
        return self.flush_once()

    def flush_once(self) -> int:
        batch = self._queue.peek(self._cfg.batch_size)
        if not batch:
            return 0
        try:
            self._broker.publish([r for _, r in batch])
        except BrokerUnavailable:
            self._register_failure()
            return 0
        # Si el proceso muere justo aquí, el lote queda en cola y se reenvía: duplicado, no pérdida.
        self._queue.ack([seq for seq, _ in batch])
        self.stats.published += len(batch)
        self.stats.consecutive_failures = 0
        self._next_attempt_ns = 0
        return len(batch)

    def drain(self, max_batches: int = 1_000_000) -> int:
        """Envía hasta vaciar la cola o hasta el primer fallo. Útil tras reconectar."""
        total = 0
        for _ in range(max_batches):
            sent = self.flush_once()
            if sent == 0:
                break
            total += sent
        return total

    def _register_failure(self) -> None:
        s = self.stats
        s.batches_failed += 1
        s.consecutive_failures += 1
        delay_ms = min(self._cfg.backoff_max_ms,
                       self._cfg.backoff_base_ms * 2 ** (s.consecutive_failures - 1))
        delay_ms *= self._jitter()
        self._next_attempt_ns = self._clock.now_ns() + int(delay_ms * 1_000_000)
