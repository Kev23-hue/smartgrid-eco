"""Demostración de ES-03 contra Redpanda real (requiere Docker y confluent-kafka).

Genera lecturas a un ritmo, las ingiere en la cola en disco y las reenvía a Redpanda.
Durante la ejecución se puede cortar el log (docker compose stop redpanda) y volver a levantarlo
(docker compose start redpanda): se ve cómo crece la cola, cómo se reintenta con espera creciente
y cómo se vacía al volver. Al final un consumidor verifica que no se perdió ninguna lectura.

Uso: python -m edge.forwarder_demo --readings 6000 --rate 100
"""
from __future__ import annotations

import argparse
import random
import threading
import time
import uuid

from .adapters.broker_kafka import KafkaBroker
from .adapters.clock import MonotonicClock
from .adapters.codec import reading_from_json
from .adapters.queue_sqlite import SqliteQueue
from .core.forwarding import Forwarder, ForwarderConfig
from .core.model import Reading


def verificar(bootstrap: str, topic: str, run: str, esperadas: set[str], espera_s: float = 8.0):
    from confluent_kafka import Consumer  # type: ignore
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"verif-{run}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    crudas, claves, ultimo = 0, set(), time.time()
    while time.time() - ultimo < espera_s:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        r = reading_from_json(m.value().decode())
        if r.meter_id.startswith(f"{run}-"):
            crudas += 1
            claves.add(r.key)
            ultimo = time.time()
    c.close()
    return crudas, claves


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="telemetry.raw")
    ap.add_argument("--readings", type=int, default=6000)
    ap.add_argument("--rate", type=int, default=100, help="lecturas por segundo")
    ap.add_argument("--meters", type=int, default=50)
    ap.add_argument("--queue", default="demo_queue.db")
    a = ap.parse_args()

    run = uuid.uuid4().hex[:6]
    q = SqliteQueue(a.queue)
    broker = KafkaBroker(a.bootstrap, a.topic, client_id=f"edge-demo-{run}")
    fwd = Forwarder(q, broker, MonotonicClock(), ForwarderConfig(batch_size=500),
                    jitter=lambda: random.uniform(0.5, 1.0))
    print(f"Corrida {run}: {a.readings} lecturas a {a.rate}/s. Corta Redpanda cuando quieras.\n")

    # Dos hilos, como en el equipo real: uno ingiere (nunca espera a la red) y otro envía.
    # Si el envío está bloqueado esperando al log caído, la ingesta sigue (ES-03).
    parar = threading.Event()

    def envio():
        while not parar.is_set():
            fwd.tick()
            time.sleep(0.1)

    hilo = threading.Thread(target=envio, daemon=True)
    hilo.start()

    esperadas: set[str] = set()
    seqs = {i: 0 for i in range(a.meters)}
    base_ms = int(time.time() * 1000)
    generadas, t_inicio, ultimo_print = 0, time.time(), 0.0
    while generadas < a.readings or fwd.backlog > 0:
        if generadas < a.readings:
            por_tick = max(1, a.rate // 10)
            lote = []
            for _ in range(min(por_tick, a.readings - generadas)):
                m = generadas % a.meters
                seqs[m] += 1
                lote.append(Reading(f"{run}-m{m}", base_ms + generadas, seqs[m], 0.5))
                generadas += 1
            esperadas.update(r.key for r in lote)
            fwd.ingest_many(lote)
        if time.time() - ultimo_print >= 1.0:
            s = fwd.stats
            estado = "ENVIANDO" if s.consecutive_failures == 0 else f"SIN LOG (fallos seguidos: {s.consecutive_failures})"
            print(f"t={time.time() - t_inicio:5.1f}s  generadas={generadas:6d}  en_cola={fwd.backlog:6d}  "
                  f"publicadas={s.published:6d}  {estado}")
            ultimo_print = time.time()
        time.sleep(0.1)
        if time.time() - t_inicio > 600:
            print("Tiempo máximo alcanzado; ¿Redpanda sigue apagado?")
            break

    parar.set()
    print("\nVerificando con un consumidor independiente...")
    crudas, claves = verificar(a.bootstrap, a.topic, run, esperadas)
    perdidas = len(esperadas - claves)
    print(f"Esperadas: {len(esperadas)} | mensajes crudos en el log: {crudas} | claves únicas: {len(claves)}")
    print(f"Duplicados crudos (se descartan por clave): {crudas - len(claves)}")
    print(f"PERDIDAS: {perdidas}   ->   {'PASA' if perdidas == 0 else 'FALLA'}")
    print(f"Rechazadas por cola llena: {fwd.stats.rejected_full}")


if __name__ == "__main__":
    main()
