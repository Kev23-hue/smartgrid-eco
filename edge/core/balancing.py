"""Política de balanceo: función pura (sin entrada/salida), fácil de probar."""
from __future__ import annotations

from .model import Decision, GridState, Policy


def decide(state: GridState, policy: Policy) -> Decision:
    balance = state.balance_kw
    if abs(balance) <= policy.tolerance_kw:
        return Decision(0.0, 0.0, 0.0, "balanceado")

    soc = state.battery.soc

    if balance < 0:  # déficit: descargar batería y, si no alcanza, desconectar carga
        deficit = -balance
        available = policy.max_discharge_kw if soc > policy.soc_min else 0.0
        battery = min(deficit, available)
        shed = deficit - battery
        reason = "deficit_cubierto" if shed == 0 else "deficit_con_desconexion"
        return Decision(battery, shed, 0.0, reason)

    surplus = balance  # excedente: cargar batería y, si no cabe, recortar generación
    room = policy.max_charge_kw if soc < policy.soc_max else 0.0
    charge = min(surplus, room)
    curtail = surplus - charge
    reason = "excedente_almacenado" if curtail == 0 else "excedente_con_recorte"
    return Decision(-charge, 0.0, curtail, reason)
