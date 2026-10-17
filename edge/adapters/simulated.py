"""Adaptadores simulados (sin hardware). En producción serían Modbus TCP / IEC 61850."""
from __future__ import annotations

import time

from ..core.model import BatteryState, Decision


class SimulatedSolar:
    name = "solar"

    def __init__(self, kw: float = 100.0):
        self.kw = kw

    def read_kw(self) -> float:
        return self.kw


class SimulatedWind:
    name = "eolica"

    def __init__(self, kw: float = 40.0):
        self.kw = kw

    def read_kw(self) -> float:
        return self.kw


class SimulatedBiomass:
    """ES-04: fuente nueva. Es solo un adaptador más del mismo puerto; el núcleo no cambia."""
    name = "biomasa"

    def __init__(self, kw: float = 30.0):
        self.kw = kw

    def read_kw(self) -> float:
        return self.kw


class SimulatedDemand:
    def __init__(self, kw: float = 120.0):
        self.kw = kw

    def read_kw(self) -> float:
        return self.kw


class SimulatedActuator:
    """Inversor y batería simulados. io_delay_ms imita la latencia del bus de campo."""

    def __init__(self, soc: float = 0.6, capacity_kwh: float = 200.0, io_delay_ms: float = 0.0):
        self.soc = soc
        self.capacity_kwh = capacity_kwh
        self.io_delay_ms = io_delay_ms
        self.applied: list[Decision] = []

    def read_battery(self) -> BatteryState:
        return BatteryState(self.soc, self.capacity_kwh)

    def apply(self, decision: Decision) -> None:
        if self.io_delay_ms > 0:
            time.sleep(self.io_delay_ms / 1000.0)
        self.applied.append(decision)
