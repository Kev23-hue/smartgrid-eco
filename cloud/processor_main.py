"""Procesador de flujos real: Redpanda -> TimescaleDB. Termina cuando el log queda en reposo.

Uso: python -m cloud.processor_main --dsn postgresql://sge:sge@localhost:5433/sge
"""
from __future__ import annotations

import argparse
import time

from .adapters.clock import SystemWallClock
from .adapters.consumer_kafka import KafkaConsumer
from .adapters.deadletter_file import FileDeadLetter
from .adapters.store_timescale import TimescaleStore
from .core.processing import ProcessorConfig, StreamProcessor


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="telemetry.raw")
    ap.add_argument("--group", default="procesador-1")
    ap.add_argument("--dsn", default="postgresql://sge:sge@localhost:5433/sge")
    ap.add_argument("--idle-polls", type=int, default=5, help="consultas vacías seguidas para terminar")
    ap.add_argument("--batch", type=int, default=1000)
    a = ap.parse_args()

    store = TimescaleStore(a.dsn)
    consumer = KafkaConsumer(a.bootstrap, a.topic, a.group, offset_lookup=store.next_offset)
    proc = StreamProcessor(consumer, store, FileDeadLetter("mensajes_muertos.jsonl"), SystemWallClock(),
                           ProcessorConfig(max_batch=a.batch))
    t0, ultimo, vacias = time.time(), time.time(), 0
    while vacias < a.idle_polls:
        n = proc.run_once(timeout_s=1.0)
        vacias = vacias + 1 if n == 0 else 0
        if time.time() - ultimo >= 1.0:
            s = proc.stats
            print(f"t={time.time() - t0:5.1f}s consumidos={s.consumed} nuevos={s.inserted} "
                  f"duplicados={s.duplicates} rechazados={s.rejected}")
            ultimo = time.time()
    s, dt = proc.stats, max(time.time() - t0, 1e-9)
    print(f"\nTerminado: consumidos={s.consumed} nuevos={s.inserted} duplicados={s.duplicates} "
          f"rechazados={s.rejected} lotes={s.batches} en {dt:.1f}s")
    consumer.close()
    store.close()


if __name__ == "__main__":
    main()
