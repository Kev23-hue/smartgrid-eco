"""Generador de carga + reenviador del borde (un proceso = una microred simulada).

Hace lo que haría el equipo del borde: ingiere lecturas en la cola en disco (sin esperar a la red)
y un hilo aparte las reenvía al log. Mide:
  - latencia de ingesta (guardar un lote en la cola durable), también DURANTE las caídas del log;
  - latencia de publicación de un lote al log.
Inyecta: duplicados (reenvío de la misma lectura) y llegada tardía/desordenada (la lectura se retiene unos segundos).

Uso (lo lanza el orquestador): python -m loadtest.generator --id 0 --phases nominal:500:20 ...
"""
from __future__ import annotations

import argparse
import heapq
import os
import random
import threading
import time

from edge.adapters.clock import MonotonicClock
from edge.adapters.queue_sqlite import SqliteQueue
from edge.core.forwarding import Forwarder, ForwarderConfig
from edge.core.model import Reading
from edge.core.ports import BrokerUnavailable

from .backends import add_backend_args, make_broker
from .common import Reservoir, now_ms, parse_phases, percentiles, write_json_atomic


class TimedBroker:
    def __init__(self, inner):
        self._inner = inner
        self.ok_ms, self.fail = Reservoir(50_000), 0

    def publish(self, batch):
        t = time.perf_counter()
        try:
            self._inner.publish(batch)
        except BrokerUnavailable:
            self.fail += 1
            raise
        self.ok_ms.add((time.perf_counter() - t) * 1000)


def main() -> None:
    ap = argparse.ArgumentParser()
    add_backend_args(ap)
    ap.add_argument("--id", type=int, default=0)
    ap.add_argument("--phases", default="nominal:500:20")
    ap.add_argument("--meters", type=int, default=100)
    ap.add_argument("--batch", type=int, default=1000, help="tamaño de lote del reenviador")
    ap.add_argument("--dup-ratio", type=float, default=0.0)
    ap.add_argument("--late-ratio", type=float, default=0.0)
    ap.add_argument("--sync", default="NORMAL", choices=["FULL", "NORMAL"], help="durabilidad de la cola en disco")
    ap.add_argument("--drain-timeout", type=float, default=300.0)
    a = ap.parse_args()

    rnd = random.Random(1000 + a.id)
    tag = f"{a.run}-g{a.id}"
    qpath = os.path.join(a.dir, f"queue_{a.id}.db")
    queue = SqliteQueue(qpath, max_rows=5_000_000, synchronous=a.sync)
    broker = TimedBroker(make_broker(a, f"edge-{tag}"))
    fwd = Forwarder(queue, broker, MonotonicClock(), ForwarderConfig(batch_size=a.batch),
                    jitter=lambda: rnd.uniform(0.5, 1.0))

    parar = threading.Event()
    errores_envio: list[str] = []

    def envio():
        while not parar.is_set():
            try:
                if fwd.tick() == 0:
                    time.sleep(0.01)
            except Exception as e:  # noqa: BLE001  (un error inesperado no debe matar el envío en silencio)
                errores_envio.append(f"{type(e).__name__}: {e}"[:200])
                print("ERROR inesperado en el hilo de envío:", errores_envio[-1], flush=True)
                time.sleep(0.5)
    hilo = threading.Thread(target=envio, daemon=True)
    hilo.start()

    seqs = [0] * a.meters
    last_ts = [0] * a.meters   # la clave es (contador, hora): la hora solo debe ser única por contador
    unique = 0
    retenidas: list[tuple[float, int, Reading]] = []   # (momento de liberación, desempate, lectura) tardías
    reenvios: list[tuple[float, int, Reading]] = []    # duplicados programados
    tiebreak = 0
    ingest_ok, ingest_down = Reservoir(100_000), Reservoir(100_000)
    t_ini = time.time()
    ultimo_prog = 0.0
    fase_actual = ""

    def progreso(final=False):
        s = fwd.stats
        write_json_atomic(os.path.join(a.dir, f"gen_{a.id}.json"), {
            "id": a.id, "final": final, "t": round(time.time() - t_ini, 2), "phase": fase_actual,
            "unique_generated": unique, "ingested": s.ingested, "published": s.published,
            "backlog": fwd.backlog, "rejected_full": s.rejected_full, "batches_failed": s.batches_failed,
            "duplicates_ignored_in_queue": s.duplicates_ignored,
            "publish_ms": percentiles(broker.ok_ms.items), "publish_failures": broker.fail, "sender_errors": errores_envio[-3:],
            "ingest_batch_ms": percentiles(ingest_ok.items),
            "ingest_batch_ms_during_outage": percentiles(ingest_down.items),
        })

    def ingerir(lote):
        if not lote:
            return
        t = time.perf_counter()
        fwd.ingest_many(lote)
        ms = (time.perf_counter() - t) * 1000
        (ingest_down if fwd.stats.consecutive_failures > 0 else ingest_ok).add(ms)

    for nombre, rate, secs in parse_phases(a.phases):
        fase_actual = nombre
        t0, producidas = time.time(), 0
        while time.time() - t0 < secs:
            ahora = time.time()
            objetivo = int(rate * (ahora - t0)) if rate > 0 else producidas + 2000
            lote = []
            for _ in range(min(max(0, objetivo - producidas), 2000)):
                m = producidas % a.meters
                seqs[m] += 1
                last_ts[m] = max(now_ms(), last_ts[m] + 1)
                r = Reading(f"{tag}-m{m}", last_ts[m], seqs[m], 0.5)
                producidas += 1
                unique += 1
                if rnd.random() < a.late_ratio:
                    tiebreak += 1   # llega tarde: seq negativo marca "no medir latencia con esta"
                    heapq.heappush(retenidas, (ahora + rnd.uniform(2, 15), tiebreak,
                                               Reading(r.meter_id, r.measured_at, -r.seq, r.kwh)))
                    continue
                lote.append(r)
                if rnd.random() < a.dup_ratio:
                    tiebreak += 1
                    heapq.heappush(reenvios, (ahora + rnd.uniform(1, 5), tiebreak, r))
            for cola in (retenidas, reenvios):
                while cola and cola[0][0] <= ahora:
                    lote.append(heapq.heappop(cola)[2])
            ingerir(lote)
            if time.time() - ultimo_prog >= 0.25:
                progreso()
                ultimo_prog = time.time()
            time.sleep(0.02 if rate > 0 else 0)

    fase_actual = "vaciado"
    # Lo retenido y lo programado se entrega antes de cerrar (nada queda sin ingerir).
    ingerir([x[2] for x in sorted(retenidas + reenvios)])
    limite = time.time() + a.drain_timeout
    while fwd.backlog > 0 and time.time() < limite:
        time.sleep(0.05)
        if time.time() - ultimo_prog >= 0.25:
            progreso()
            ultimo_prog = time.time()
    parar.set()
    progreso(final=True)


if __name__ == "__main__":
    main()
