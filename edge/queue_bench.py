"""Mide cuántas lecturas por segundo aguanta la cola en disco (ES-01: pico por microred).

Pico de ES-01 = 30.000 mensajes/s en total / 12 microredes = 2.500 mensajes/s por microred.
Se mide ingesta (put_many) y envío (peek + ack) contra disco real, por tamaño de grupo.
Uso: python -m edge.queue_bench --rows 20000
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
import time

from .adapters.queue_sqlite import SqliteQueue
from .core.model import Reading

PICO_POR_MICRORED = 30_000 / 12


def medir(rows: int, grupo: int, sync: str) -> dict:
    with tempfile.TemporaryDirectory() as d:
        q = SqliteQueue(os.path.join(d, "cola.db"), max_rows=rows * 2, synchronous=sync)
        lecturas = [Reading(f"m{i % 4167}", 1_700_000_000_000 + i, i, 0.5) for i in range(rows)]
        t0 = time.perf_counter()
        for i in range(0, rows, grupo):
            q.put_many(lecturas[i:i + grupo])
        t_in = time.perf_counter() - t0
        t0 = time.perf_counter()
        while q.size():
            lote = q.peek(500)
            q.ack([s for s, _ in lote])
        t_out = time.perf_counter() - t0
        q.close()
    return {"synchronous": sync, "grupo": grupo, "ingesta_por_s": round(rows / t_in),
            "salida_por_s": round(rows / t_out), "cumple_pico": rows / t_in >= PICO_POR_MICRORED}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20_000)
    ap.add_argument("--out", default="results/cola_en_disco.json")
    a = ap.parse_args()
    res = []
    for sync in ("FULL", "NORMAL"):
        for grupo in (1, 50, 500):
            n = min(a.rows, 2000) if grupo == 1 else a.rows   # grupo=1 es muy lento; se acorta
            r = medir(n, grupo, sync)
            res.append(r)
            print(r)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump({"pico_por_microred": PICO_POR_MICRORED, "resultados": res}, f, indent=2)
    print("guardado en", a.out)


if __name__ == "__main__":
    main()
