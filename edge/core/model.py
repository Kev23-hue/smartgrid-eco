"""Modelo de dominio del controlador de borde. Sin dependencias externas."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatteryState:
    soc: float  # estado de carga, 0.0 a 1.0
    capacity_kwh: float


@dataclass(frozen=True)
class GridState:
    """Foto de la microred en un instante."""
    generation_kw: float
    demand_kw: float
    battery: BatteryState

    @property
    def balance_kw(self) -> float:
        """Positivo = excedente; negativo = déficit."""
        return self.generation_kw - self.demand_kw


@dataclass(frozen=True)
class Policy:
    """Parámetros de la política de balanceo (versionados y distribuidos desde la nube)."""
    version: int
    max_discharge_kw: float
    max_charge_kw: float
    soc_min: float
    soc_max: float
    tolerance_kw: float  # desbalance menor que esto se ignora (evita oscilaciones)


@dataclass(frozen=True)
class Decision:
    """Comando de rebalanceo. battery_kw > 0 descarga, < 0 carga."""
    battery_kw: float
    shed_kw: float      # carga que se desconecta por déficit no cubierto
    curtail_kw: float   # generación que se recorta por excedente no almacenable
    reason: str

    @property
    def is_noop(self) -> bool:
        return self.battery_kw == 0 and self.shed_kw == 0 and self.curtail_kw == 0


@dataclass(frozen=True)
class CycleResult:
    decision: Decision
    state: GridState
    latency_ns: int  # desde que el estado se lee hasta que el actuador confirma
    acted: bool


@dataclass(frozen=True)
class Reading:
    """Lectura de un contador. La clave de deduplicación es (meter_id, measured_at) (ADR-004).

    measured_at: instante de la medición en milisegundos Unix, puesto por el contador.
    seq: número de secuencia por contador; permite detectar huecos (S10).
    """
    meter_id: str
    measured_at: int
    seq: int
    kwh: float

    @property
    def key(self) -> str:
        return f"{self.meter_id}|{self.measured_at}"
