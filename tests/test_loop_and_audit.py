import sqlite3
import unittest

from edge.adapters.audit_sqlite import SqliteAudit
from edge.adapters.clock import MonotonicClock
from edge.adapters.policy_file import StaticPolicyRepository
from edge.adapters.simulated import SimulatedActuator, SimulatedDemand, SimulatedSolar
from edge.core.control_loop import ControlLoop
from edge.core.model import Policy

P = Policy(1, 60, 60, 0.2, 0.95, 1.0)


def make(gen=100.0, dem=120.0, io_delay_ms=0.0):
    solar = SimulatedSolar(gen)
    act = SimulatedActuator(io_delay_ms=io_delay_ms)
    audit = SqliteAudit(":memory:")
    loop = ControlLoop([solar], SimulatedDemand(dem), act, audit, StaticPolicyRepository(P), MonotonicClock())
    return solar, act, audit, loop


class LoopTests(unittest.TestCase):
    def test_caida_de_generacion_envia_comando_y_audita(self):
        solar, act, audit, loop = make(gen=120.0)
        self.assertFalse(loop.run_cycle().acted)  # balanceado
        solar.kw = 70.0
        r = loop.run_cycle()
        self.assertTrue(r.acted)
        self.assertEqual(act.applied[-1].battery_kw, 50)
        self.assertEqual(audit.count(), 1)
        self.assertGreater(r.latency_ns, 0)

    def test_sin_fuentes_falla(self):
        with self.assertRaises(ValueError):
            ControlLoop([], SimulatedDemand(1), SimulatedActuator(), SqliteAudit(), StaticPolicyRepository(P),
                        MonotonicClock())

    def test_la_latencia_incluye_el_retardo_del_actuador(self):
        _, _, _, loop = make(gen=70.0, io_delay_ms=20.0)
        r = loop.run_cycle()
        self.assertGreaterEqual(r.latency_ns / 1e6, 19.0)


class AuditTests(unittest.TestCase):
    def test_cadena_valida(self):
        a = SqliteAudit()
        for i in range(5):
            a.record("evento", {"i": i})
        self.assertTrue(a.verify_chain())

    def test_no_permite_borrar_ni_modificar(self):
        a = SqliteAudit()
        a.record("e", {"x": 1})
        with self.assertRaises(sqlite3.DatabaseError):
            a._db.execute("DELETE FROM audit")
        with self.assertRaises(sqlite3.DatabaseError):
            a._db.execute("UPDATE audit SET payload='{}'")

    def test_detecta_manipulacion(self):
        a = SqliteAudit()
        for i in range(3):
            a.record("e", {"i": i})
        # un atacante con acceso al archivo elimina el trigger y altera un registro
        a._db.execute("DROP TRIGGER no_update")
        a._db.execute("UPDATE audit SET payload='{\"i\": 99}' WHERE id=2")
        self.assertFalse(a.verify_chain())


if __name__ == "__main__":
    unittest.main()
