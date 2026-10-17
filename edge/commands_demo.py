"""Demostración de ES-05 y ES-07 y medición del costo de verificar firmas (T4 / ADR-007).

Uso: python -m edge.commands_demo            (necesita: pip install cryptography)
"""
from __future__ import annotations

import json
import os
import random
import tempfile
import time

from loadtest.common import percentiles

from .adapters.audit_sqlite import SqliteAudit
from .adapters.clock import SystemWallClock
from .adapters.nonce_sqlite import SqliteNonceStore
from .adapters.sign_ed25519 import Ed25519Verifier, Operator
from .core.commands import ACCEPTED, PENDING, REJECTED, Command, CommandGateway


class Inversor:
    def __init__(self):
        self.recibidos = []

    def execute(self, action, params):
        self.recibidos.append(action)


def main() -> None:
    ana, beto = Operator("ana"), Operator("beto")
    verif = Ed25519Verifier({"ana": ana.public_hex, "beto": beto.public_hex})
    audit, inv = SqliteAudit(":memory:"), Inversor()
    gw = CommandGateway(verif, SqliteNonceStore(":memory:"), SystemWallClock(), inv, audit)
    rnd = random.Random(3)

    print("== ES-05: 1000 comandos NO autenticados (firma basura, sin firma, llave ajena, suplantación, alterados) ==")
    intrusa = Operator("ana")
    rechazados = 0
    for i in range(1000):
        t = i % 5
        if t == 0:
            c = ana.issue(f"x{i}", "set_battery_kw", {"kw": "50"})
            c = Command(**{**c.__dict__, "signature": bytes(rnd.getrandbits(8) for _ in range(64))})
        elif t == 1:
            c = Command(f"x{i}", "set_battery_kw", (("kw", "50"),), "ana", 0, 1, f"n{i}", b"")
        elif t == 2:
            c = Operator("mallory").issue(f"x{i}", "set_battery_kw", {"kw": "50"})
        elif t == 3:
            c = intrusa.issue(f"x{i}", "set_battery_kw", {"kw": "50"})
        else:
            c = ana.issue(f"x{i}", "set_battery_kw", {"kw": "50"})
            c = Command(**{**c.__dict__, "params": (("kw", "9999"),)})
        rechazados += gw.submit(c).status == REJECTED
    registrados = sum(1 for e, in audit._db.execute("SELECT event FROM audit") if e == "command_rejected")
    print(f"Rechazados: {rechazados}/1000 | registrados en la auditoría: {registrados}/1000 | llegaron al inversor: {len(inv.recibidos)}")
    print(f"ES-05 -> {'PASA' if rechazados == 1000 and registrados == 1000 and not inv.recibidos else 'FALLA'}")

    print("\n== ES-07: comando crítico (open_breaker) ==")
    c = ana.issue("crit-1", "open_breaker", {"feeder": "F3"})
    r1 = gw.submit(c)
    print(f"1) Ana lo emite sola: {r1.status} ({r1.reason}); ejecutado: {'open_breaker' in inv.recibidos}")
    r2 = gw.approve("crit-1", "ana", ana.approve(c))
    print(f"2) Ana intenta aprobar su propio comando: {r2.status} ({r2.reason})")
    r3 = gw.approve("crit-1", "beto", beto.approve(c))
    print(f"3) Beto (otro operador) aprueba: {r3.status}; ejecutado: {'open_breaker' in inv.recibidos}")
    ok = r1.status == PENDING and r2.status == REJECTED and r3.status == ACCEPTED
    print(f"ES-07 -> {'PASA' if ok else 'FALLA'}")

    print("\n== Anti-repetición ==")
    c2 = ana.issue("n-1", "set_battery_kw", {"kw": "10"})
    a, b = gw.submit(c2).status, gw.submit(c2).status
    print(f"Primera vez: {a} | repetido: {b}")

    print("\n== Auditoría ==")
    print(f"Eventos: {audit.count()} | cadena de hashes íntegra: {audit.verify_chain()}")
    try:
        audit._db.execute("DELETE FROM audit")
    except Exception as e:  # noqa: BLE001
        print(f"Intento de borrar la auditoría: bloqueado ({e})")

    print("\n== Costo de verificar (T4): 3000 comandos válidos, con nonces y auditoría EN DISCO ==")
    d = tempfile.mkdtemp()
    gw2 = CommandGateway(verif, SqliteNonceStore(os.path.join(d, "n.db")), SystemWallClock(), Inversor(),
                         SqliteAudit(os.path.join(d, "a.db")))
    cmds = [ana.issue(f"v{i}", "set_battery_kw", {"kw": "1"}) for i in range(3000)]
    tiempos = []
    for c in cmds:
        t = time.perf_counter()
        gw2.submit(c)
        tiempos.append((time.perf_counter() - t) * 1000)
    p = percentiles(tiempos)
    print(json.dumps(p))
    print(f"Verificar+nonce+auditar+ejecutar: p50={p['p50']} ms, p99={p['p99']} ms (presupuesto del lazo de control: 2000 ms)")


if __name__ == "__main__":
    main()
