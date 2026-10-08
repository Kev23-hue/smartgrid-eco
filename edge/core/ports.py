"""Puertos: las interfaces que define el núcleo. Los adaptadores las implementan."""
from __future__ import annotations

from typing import Protocol

from .model import BatteryState, Decision, Policy


class GenerationSourcePort(Protocol):
    """Una fuente de generación (solar, eólica, biomasa...). ES-04: una fuente nueva = un adaptador nuevo."""
    name: str

    def read_kw(self) -> float: ...


class DemandPort(Protocol):
    def read_kw(self) -> float: ...


class ActuatorPort(Protocol):
    """Inversores y baterías."""

    def read_battery(self) -> BatteryState: ...

    def apply(self, decision: Decision) -> None: ...


class AuditPort(Protocol):
    def record(self, event: str, payload: dict) -> None: ...


class PolicyPort(Protocol):
    def load(self) -> Policy: ...


class ClockPort(Protocol):
    def now_ns(self) -> int: ...
