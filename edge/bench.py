"""Benchmark de ES-02: latencia del lazo de control ante una caída de generación.

Uso:  python -m edge.bench [--events 1000] [--io-delay-ms 0] [--out results/es02.json]

Mide dos cosas:
  * reaccion: desde que el estado está leído hasta que el actuador confirma
    (es la definición del enunciado: detección -> comando al actuador).
  * peor_caso_con_sondeo: reaccion + un periodo completo de sondeo (cycle_ms),
    que es lo peor que puede tardar en *enterarse* de una caída.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics

from .adapters.audit_sqlite import SqliteAudit
from .adapters.clock import MonotonicClock
from .adapters.policy_file import StaticPolicyRepository
from .adapters.simulated import SimulatedActuator, SimulatedDemand, SimulatedSolar
from .core.control_loop import ControlLoop
from .core.model import Policy


def percentile(sorted_values: list[float], p: float) -> float:
    k = max(0, min(len(sorted_values) - 1, int(round(p / 100 * len(sorted_values) + 0.5)) - 1))
    return sorted_values[k]


def run(events: int, io_delay_ms: float, cycle_ms: float) -> dict:
    solar = SimulatedSolar(100.0)
    actuator = SimulatedActuator(soc=0.6, io_delay_ms=io_delay_ms)
    policy = Policy(1, 60, 60, 0.2, 0.95, 1.0)
    loop = ControlLoop([solar], SimulatedDemand(120.0), actuator, SqliteAudit(":memory:"),
                       StaticPolicyRepository(policy), MonotonicClock())
    lat_ms: list[float] = []
    for i in range(events):
        solar.kw = 100.0 if i % 2 == 0 else 70.0  # alterna: normal / caída de generación
        r = loop.run_cycle()
        if r.acted:
            lat_ms.append(r.latency_ns / 1e6)
    lat_ms.sort()
    out = {"eventos": events, "eventos_con_accion": len(lat_ms), "io_delay_ms": io_delay_ms,
           "cycle_ms": cycle_ms, "unidad": "ms"}
    for name, offset in (("reaccion", 0.0), ("peor_caso_con_sondeo", cycle_ms)):
        out[name] = {"p50": round(percentile(lat_ms, 50) + offset, 4),
                     "p95": round(percentile(lat_ms, 95) + offset, 4),
                     "p99": round(percentile(lat_ms, 99) + offset, 4),
                     "max": round(lat_ms[-1] + offset, 4),
                     "media": round(statistics.mean(lat_ms) + offset, 4)}
    out["cumple_es02_p99_menor_2000ms"] = out["peor_caso_con_sondeo"]["p99"] <= 2000.0
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=1000)
    ap.add_argument("--io-delay-ms", type=float, default=0.0,
                    help="retardo simulado del bus de campo al escribir la consigna")
    ap.add_argument("--cycle-ms", type=float, default=250.0)
    ap.add_argument("--out", default="results/es02.json")
    a = ap.parse_args()
    res = run(a.events, a.io_delay_ms, a.cycle_ms)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
