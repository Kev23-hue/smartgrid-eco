"""Verifica una corrida de forwarder_demo contra la base: ¿0 lecturas perdidas o duplicadas?

Uso: python -m cloud.check_run --run a1b2c3 --expected 6000
(--run es el identificador que imprime forwarder_demo al empezar)
"""
from __future__ import annotations

import argparse

from .adapters.store_timescale import TimescaleStore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://sge:sge@localhost:5433/sge")
    ap.add_argument("--run", required=True)
    ap.add_argument("--expected", type=int, required=True)
    ap.add_argument("--kwh", type=float, default=0.5, help="kWh de cada lectura de la demo")
    a = ap.parse_args()
    store = TimescaleStore(a.dsn)
    n, suma = store.prefix_totals(f"{a.run}-")
    esperado_kwh = a.expected * a.kwh
    print(f"Corrida {a.run}: filas en la base = {n} (esperadas {a.expected}); suma kWh = {suma} (esperada {esperado_kwh})")
    ok = n == a.expected and abs(suma - esperado_kwh) < 1e-9
    print(f"PERDIDAS/DUPLICADAS: {abs(a.expected - n)}   ->   {'PASA' if ok else 'FALLA'}")
    store.close()


if __name__ == "__main__":
    main()
