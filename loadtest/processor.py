"""Procesador de flujos para la prueba de carga (uno por proceso). Se reconecta solo si la BD o el log fallan,
como lo haría un contenedor con reinicio automático. Mide la latencia de extremo a extremo:
hora de pared al confirmarse la transacción menos measured_at (la hora en que se generó la lectura).
"""
from __future__ import annotations

import argparse
import os
import time

from cloud.adapters.clock import SystemWallClock
from cloud.adapters.deadletter_memory import MemoryDeadLetter
from cloud.core.processing import ProcessorConfig, StreamProcessor

from .backends import add_backend_args, ctl_dir, make_consumer, make_store
from .common import Reservoir, now_ms, percentiles, write_json_atomic


class TimedStore:
    """Decora la BD: mide la latencia tras confirmar y permite inyectar una caída de la BD (archivo db.down)."""

    def __init__(self, inner, a, lat: Reservoir):
        self._s, self._a, self.lat = inner, a, lat

    def commit_batch(self, readings, positions):
        if self._a.backend == "file" and os.path.exists(os.path.join(ctl_dir(self._a), "db.down")):
            raise ConnectionError("BD caída (inyectada)")
        n = self._s.commit_batch(readings, positions)
        t = now_ms()
        if n == len(readings):   # solo lotes sin repetidas: una repetida mediría la edad de la original
            self.lat.add_many(t - r.measured_at for r in readings if r.seq > 0)
        return n

    def __getattr__(self, name):
        return getattr(self._s, name)


def main() -> None:
    ap = argparse.ArgumentParser()
    add_backend_args(ap)
    ap.add_argument("--name", default="p0")
    ap.add_argument("--group", default="procesador")
    ap.add_argument("--batch", type=int, default=1000)
    a = ap.parse_args()

    lat = Reservoir(200_000)
    dlq = MemoryDeadLetter()
    totales = dict(consumed=0, inserted=0, duplicates=0, rejected=0, batches=0)
    reinicios, ultimo_error = 0, ""
    t_ini, ultimo_prog = time.time(), 0.0
    ctl = ctl_dir(a)
    inactivas = 0

    def progreso(final=False):
        write_json_atomic(os.path.join(a.dir, f"proc_{a.name}.json"), {
            "name": a.name, "final": final, "t": round(time.time() - t_ini, 2), **totales,
            "reconnects": reinicios, "last_error": ultimo_error,
            "latency_ms": percentiles(lat.items) if final else None})

    while True:
        store = consumer = None
        try:
            store = TimedStore(make_store(a), a, lat)
            consumer = make_consumer(a, a.group, store)
            proc = StreamProcessor(consumer, store, dlq, SystemWallClock(), ProcessorConfig(max_batch=a.batch))
            base = dict(totales)
            while True:
                n = proc.run_once(timeout_s=0.5)
                s = proc.stats
                totales.update(consumed=base["consumed"] + s.consumed, inserted=base["inserted"] + s.inserted,
                               duplicates=base["duplicates"] + s.duplicates, rejected=base["rejected"] + s.rejected,
                               batches=base["batches"] + s.batches)
                if time.time() - ultimo_prog >= 0.25:
                    progreso()
                    ultimo_prog = time.time()
                inactivas = inactivas + 1 if n == 0 else 0
                if os.path.exists(os.path.join(ctl, "stop")) and inactivas >= 6:
                    progreso(final=True)
                    return
        except Exception as e:  # noqa: BLE001  (caída de BD/log: reconectar, como un reinicio automático)
            reinicios += 1
            ultimo_error = f"{type(e).__name__}: {e}"[:200]
            progreso()
            time.sleep(1.0)
        finally:
            for x in (consumer, store):
                try:
                    if x is not None:
                        (x.close if hasattr(x, "close") else (lambda: None))()
                except Exception:  # noqa: BLE001
                    pass


if __name__ == "__main__":
    main()
