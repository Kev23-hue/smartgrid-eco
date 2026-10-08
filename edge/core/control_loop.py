"""Ciclo de control: lee estado, decide, ordena al actuador y audita.

Nunca consulta la nube (ADR-001). Solo usa puertos.
"""
from __future__ import annotations

from typing import Sequence

from .balancing import decide
from .model import CycleResult, GridState
from .ports import (ActuatorPort, AuditPort, ClockPort, DemandPort,
                    GenerationSourcePort, PolicyPort)


class ControlLoop:
    def __init__(self, sources: Sequence[GenerationSourcePort], demand: DemandPort,
                 actuator: ActuatorPort, audit: AuditPort, policy_repo: PolicyPort,
                 clock: ClockPort):
        if not sources:
            raise ValueError("se requiere al menos una fuente de generación")
        self._sources = list(sources)
        self._demand = demand
        self._actuator = actuator
        self._audit = audit
        self._policy_repo = policy_repo
        self._clock = clock
        self._policy = policy_repo.load()  # copia local: la decisión no consulta nada remoto

    def reload_policy(self) -> int:
        self._policy = self._policy_repo.load()
        return self._policy.version

    def run_cycle(self) -> CycleResult:
        generation = sum(s.read_kw() for s in self._sources)
        state = GridState(generation, self._demand.read_kw(), self._actuator.read_battery())

        t0 = self._clock.now_ns()  # estado leído: la "detección" empieza aquí
        decision = decide(state, self._policy)
        acted = not decision.is_noop
        if acted:
            self._actuator.apply(decision)
        latency = self._clock.now_ns() - t0

        # La auditoría va después de medir: no pertenece al camino crítico.
        if acted:
            self._audit.record("rebalance", {
                "policy": self._policy.version,
                "generation_kw": state.generation_kw,
                "demand_kw": state.demand_kw,
                "battery_kw": decision.battery_kw,
                "shed_kw": decision.shed_kw,
                "curtail_kw": decision.curtail_kw,
                "reason": decision.reason,
                "latency_us": latency // 1000,
            })
        return CycleResult(decision, state, latency, acted)
