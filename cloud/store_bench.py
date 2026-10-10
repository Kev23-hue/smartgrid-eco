"""Valida S16: ¿TimescaleDB sostiene >= 30.000 filas/s con inserción idempotente?

Con 5% de duplicados (ADR-004), mide una matriz de configuraciones:
  - escritores en paralelo (el diseño tiene 24 particiones, o sea hasta 24 consumidores);
  - tamaño de lote;
  - synchronous_commit activado o apagado. Apagarlo es SEGURO aquí porque los offsets van en la misma
    transacción que las lecturas: si una caída pierde la última transacción, pierde las dos cosas
    juntas, se relee del log y la escritura idempotente lo absorbe.
Cada prueba se hace de dos formas: solo la inserción, y el lote completo del procesador
(inserción + recálculo de agregados + offsets, en una transacción).
Uso: python -m cloud.store_bench --rows 200000
"""
from __future__ import annotations

import argparse
import json
import os
import random
import threading
import time
import zlib

from .adapters.store_timescale import SQL_INSERT, TimescaleStore
from .core.model import INTERVAL_MS, Position, Reading

OBJETIVO = 30_000


def generar(rows: int, meters: int, dup_ratio: float, seed: int = 1):
    """Lecturas en orden de tiempo (cada contador cada 5 s) con una fracción de repetidas."""
    rnd = random.Random(seed)
    t0 = (1_700_000_000_000 // INTERVAL_MS + 1) * INTERVAL_MS
    out: list[Reading] = []
    i = 0
    while len(out) < rows:
        step, m = divmod(i, meters)
        r = Reading(f"m{m}", t0 + step * 5000, step, 0.5)
        out.append(r)
        if rnd.random() < dup_ratio:
            out.append(out[rnd.randrange(max(0, len(out) - 5000), len(out))])
        i += 1
    return out[:rows]


def repartir(lecturas, workers: int):
    """Como Kafka: la partición depende del contador, así un contador siempre va al mismo escritor."""
    partes = [[] for _ in range(workers)]
    for r in lecturas:
        partes[zlib.crc32(r.meter_id.encode()) % workers].append(r)
    return partes


def lotes(lecturas, n):
    for i in range(0, len(lecturas), n):
        yield lecturas[i:i + n]


def escritor(dsn, lecturas, batch, modo, sync_off, worker, resultado, listo):
    store = TimescaleStore(dsn)
    if sync_off:
        store._conn.execute("SET synchronous_commit = off")
    nuevas = 0
    listo.wait()                                   # todos arrancan a la vez
    for k, lote in enumerate(lotes(lecturas, batch)):
        if modo == "solo_insercion":
            with store._conn.transaction():
                nuevas += store._conn.execute(SQL_INSERT, (
                    [r.meter_id for r in lote], [r.measured_at for r in lote],
                    [r.seq for r in lote], [r.kwh for r in lote])).fetchone()[0]
        else:
            nuevas += store.commit_batch(lote, [Position("telemetry.raw", worker, k)])
    resultado[worker] = nuevas
    store.close()


def correr(dsn, lecturas, unicas, batch, workers, modo, sync_off):
    prep = TimescaleStore(dsn)
    prep.truncate_all()
    prep.close()
    partes = repartir(lecturas, workers)
    resultado: dict[int, int] = {}
    listo = threading.Event()
    hilos = [threading.Thread(target=escritor, args=(dsn, partes[w], batch, modo, sync_off, w, resultado, listo))
             for w in range(workers)]
    for h in hilos:
        h.start()
    t0 = time.perf_counter()
    listo.set()
    for h in hilos:
        h.join()
    dt = time.perf_counter() - t0
    nuevas = sum(resultado.values())
    chk = TimescaleStore(dsn)
    n, _ = chk.raw_totals()
    chk.close()
    return {"modo": modo, "escritores": workers, "lote": batch, "synchronous_commit": "off" if sync_off else "on",
            "filas_por_s": round(len(lecturas) / dt), "nuevas": nuevas, "correcto": nuevas == unicas == n}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://sge:sge@localhost:5433/sge")
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--batches", type=int, nargs="+", default=[1000, 5000])
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 4, 8])
    ap.add_argument("--meters", type=int, default=4167, help="contadores por microred")
    ap.add_argument("--dup-ratio", type=float, default=0.05)
    ap.add_argument("--out", default="results/s16_timescale.json")
    a = ap.parse_args()

    lecturas = generar(a.rows, a.meters, a.dup_ratio)
    unicas = len({r.key for r in lecturas})
    runs = []
    for modo in ("solo_insercion", "lote_completo"):
        for sync_off in (False, True):
            for w in a.workers:
                for b in a.batches:
                    r = correr(a.dsn, lecturas, unicas, b, w, modo, sync_off)
                    runs.append(r)
                    print(f"{r['modo']:15s} escritores={r['escritores']:2d} lote={r['lote']:5d} "
                          f"sync={r['synchronous_commit']:3s} -> {r['filas_por_s']:7d} filas/s "
                          f"{'OK' if r['correcto'] else 'INCORRECTO'}", flush=True)
    completos = [r for r in runs if r["modo"] == "lote_completo" and r["synchronous_commit"] == "on"]
    mejor_seguro = max(completos, key=lambda r: r["filas_por_s"])
    mejor = max((r for r in runs if r["modo"] == "lote_completo"), key=lambda r: r["filas_por_s"])
    res = {"filas": a.rows, "unicas": unicas, "duplicados_inyectados": a.rows - unicas,
           "cpus_visibles": os.cpu_count(), "objetivo_filas_por_s": OBJETIVO,
           "corridas": runs, "mejor_lote_completo_con_sync_on": mejor_seguro,
           "mejor_lote_completo": mejor,
           "todas_correctas": all(r["correcto"] for r in runs),
           "aviso": ("Medido en un contenedor local, no en los 2 nodos de 8 vCPU / 64 GiB de S16. "
                     "Es una referencia, no la validación final.")}
    print("\nMejor (sync on, lote completo):", mejor_seguro["filas_por_s"], "filas/s")
    print("Mejor (cualquiera, lote completo):", mejor["filas_por_s"], "filas/s")
    print("Todas las corridas correctas:", res["todas_correctas"])
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    print("guardado en", a.out)


if __name__ == "__main__":
    main()
