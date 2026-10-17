"""Utilidades compartidas por el generador, el procesador y el orquestador de la prueba de carga."""
from __future__ import annotations

import json
import math
import os
import random
import time
from typing import Sequence


def percentiles(samples: Sequence[float]) -> dict:
    """Percentiles por rango más cercano (nearest-rank). Devuelve n=0 si no hay muestras."""
    if not samples:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None, "mean": None}
    s = sorted(samples)

    def pct(p: float) -> float:
        return s[max(0, math.ceil(p / 100 * len(s)) - 1)]
    return {"n": len(s), "p50": round(pct(50), 3), "p95": round(pct(95), 3), "p99": round(pct(99), 3),
            "max": round(s[-1], 3), "mean": round(sum(s) / len(s), 3)}


def write_json_atomic(path: str, obj) -> None:
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    for intento in range(50):   # en Windows, replace falla si otro proceso tiene el archivo abierto un instante
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.01)
    os.replace(tmp, path)


def read_json(path: str):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


class Reservoir:
    """Muestra acotada y uniforme (reservoir sampling): memoria fija aunque haya millones de lecturas."""

    def __init__(self, cap: int = 200_000, seed: int = 1):
        self.cap, self.seen, self.items = cap, 0, []
        self._rnd = random.Random(seed)

    def add(self, x: float) -> None:
        self.seen += 1
        if len(self.items) < self.cap:
            self.items.append(x)
        else:
            j = self._rnd.randrange(self.seen)
            if j < self.cap:
                self.items[j] = x

    def add_many(self, xs) -> None:
        for x in xs:
            self.add(x)


def parse_phases(text: str) -> list[tuple[str, float, float]]:
    """'nominal:1000:20,burst:3000:20' -> [('nominal', 1000.0, 20.0), ...] (nombre:lecturas_por_s:segundos)."""
    out = []
    for part in text.split(","):
        nombre, rate, secs = part.split(":")
        out.append((nombre, float(rate), float(secs)))
    return out


def now_ms() -> int:
    return time.time_ns() // 1_000_000
