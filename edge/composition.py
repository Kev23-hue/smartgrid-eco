"""Raíz de composición: único lugar que conoce núcleo y adaptadores a la vez.

Agregar una fuente nueva (ES-04) = registrar su clase en SOURCE_ADAPTERS.
No se toca nada de edge/core.
"""
from __future__ import annotations

import json

from .adapters.audit_sqlite import SqliteAudit
from .adapters.clock import MonotonicClock
from .adapters.policy_file import FilePolicyRepository
from .adapters.simulated import (SimulatedActuator, SimulatedDemand,
                                 SimulatedSolar, SimulatedWind)
from .core.control_loop import ControlLoop

SOURCE_ADAPTERS = {
    "solar": SimulatedSolar,
    "eolica": SimulatedWind,
    
}


def build_from_config(path: str, audit_path: str | None = None) -> ControlLoop:
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f)
    sources = []
    for s in cfg["sources"]:
        cls = SOURCE_ADAPTERS[s["adapter"]]
        sources.append(cls(s["kw"]))
    return ControlLoop(
        sources=sources,
        demand=SimulatedDemand(cfg["demand_kw"]),
        actuator=SimulatedActuator(**cfg["actuator"]),
        audit=SqliteAudit(audit_path or cfg.get("audit_db", ":memory:")),
        policy_repo=FilePolicyRepository(cfg["policy_file"]),
        clock=MonotonicClock(),
    )
