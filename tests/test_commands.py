"""ES-05 (100% de comandos no autenticados rechazados y registrados) y ES-07 (doble autorización)."""
import os
import random
import tempfile
import unittest

try:
    from edge.adapters.sign_ed25519 import Ed25519Verifier, Operator, command_from_json, command_to_json
    HAY_CRYPTO = True
except ImportError:  # pragma: no cover
    HAY_CRYPTO = False

from edge.adapters.audit_sqlite import SqliteAudit
from edge.adapters.nonce_sqlite import SqliteNonceStore
from edge.core.commands import ACCEPTED, PENDING, REJECTED, Command, CommandGateway


class Reloj:
    def __init__(self, t=1_000_000):
        self.t = t

    def now_ms(self):
        return self.t


class Ejecutor:
    def __init__(self):
        self.llamadas = []

    def execute(self, action, params):
        self.llamadas.append((action, params))


@unittest.skipUnless(HAY_CRYPTO, "falta la librería cryptography (pip install cryptography)")
class ComandosTests(unittest.TestCase):
    def setUp(self):
        self.ana, self.beto = Operator("ana"), Operator("beto")
        self.verif = Ed25519Verifier({"ana": self.ana.public_hex, "beto": self.beto.public_hex})
        self.reloj, self.ej, self.audit = Reloj(), Ejecutor(), SqliteAudit(":memory:")
        self.nonces = SqliteNonceStore(":memory:")
        self.gw = CommandGateway(self.verif, self.nonces, self.reloj, self.ej, self.audit)

    def cmd(self, op=None, cid="c1", action="set_battery_kw", **kw):
        return (op or self.ana).issue(cid, action, {"kw": "20"}, now_ms=self.reloj.t, **kw)

    def rechazos_auditados(self):
        return sum(1 for e, in self.audit._db.execute("SELECT event FROM audit") if e == "command_rejected")

    # ---- ES-05 ----
    def test_comando_valido_se_ejecuta_y_se_audita(self):
        r = self.gw.submit(self.cmd())
        self.assertEqual(r.status, ACCEPTED)
        self.assertEqual(self.ej.llamadas, [("set_battery_kw", {"kw": "20"})])

    def test_es05_mil_comandos_no_autenticados_100_por_ciento_rechazados_y_registrados(self):
        rnd = random.Random(7)
        intrusa = Operator("ana")                       # dice ser 'ana' pero con otra llave
        n = 0
        for i in range(1000):
            tipo = i % 5
            if tipo == 0:      # firma basura
                c = self.cmd(cid=f"x{i}")
                c = Command(**{**c.__dict__, "signature": bytes(rnd.getrandbits(8) for _ in range(64))})
            elif tipo == 1:    # sin firma
                c = Command(f"x{i}", "set_battery_kw", (("kw", "99"),), "ana", self.reloj.t, self.reloj.t + 1000, f"n{i}", b"")
            elif tipo == 2:    # llave desconocida
                c = Operator("mallory").issue(f"x{i}", "set_battery_kw", {"kw": "99"}, now_ms=self.reloj.t)
            elif tipo == 3:    # suplantación: otra llave diciendo ser 'ana'
                c = intrusa.issue(f"x{i}", "set_battery_kw", {"kw": "99"}, now_ms=self.reloj.t)
            else:              # comando legítimo alterado después de firmar
                c = self.cmd(cid=f"x{i}")
                c = Command(**{**c.__dict__, "params": (("kw", "9999"),)})
            self.assertEqual(self.gw.submit(c).status, REJECTED)
            n += 1
        self.assertEqual(self.ej.llamadas, [])           # nada llegó al inversor
        self.assertEqual(self.rechazos_auditados(), n)   # y cada intento quedó registrado
        self.assertTrue(self.audit.verify_chain())

    def test_rechazo_no_gasta_nonces(self):
        malo = Operator("mallory").issue("m", "set_battery_kw", {"kw": "1"}, now_ms=self.reloj.t, nonce="N1")
        self.gw.submit(malo)
        self.assertTrue(self.nonces.remember_if_new("N1", 1))   # el nonce sigue libre

    def test_repeticion_se_rechaza(self):
        c = self.cmd()
        self.assertEqual(self.gw.submit(c).status, ACCEPTED)
        r = self.gw.submit(c)
        self.assertEqual(r.status, REJECTED)
        self.assertIn("repetición", r.reason)
        self.assertEqual(len(self.ej.llamadas), 1)

    def test_repeticion_tras_reinicio_se_rechaza(self):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "nonces.db")
            n1 = SqliteNonceStore(ruta)
            gw1 = CommandGateway(self.verif, n1, self.reloj, self.ej, self.audit)
            c = self.cmd()
            self.assertEqual(gw1.submit(c).status, ACCEPTED)
            n1.close()
            n2 = SqliteNonceStore(ruta)   # "reinicio": otro objeto sobre el mismo archivo
            gw2 = CommandGateway(self.verif, n2, self.reloj, self.ej, self.audit)
            self.assertEqual(gw2.submit(c).status, REJECTED)
            n2.close()   # Windows no deja borrar la carpeta con el archivo abierto

    def test_comando_vencido_y_del_futuro(self):
        c = self.cmd(ttl_ms=10_000)
        self.reloj.t += 10_001
        self.assertIn("vencido", self.gw.submit(c).reason)
        futuro = self.ana.issue("f", "set_battery_kw", {}, now_ms=self.reloj.t + 60_000)
        self.assertIn("futuro", self.gw.submit(futuro).reason)

    def test_vigencia_excesiva_y_accion_no_permitida(self):
        self.assertIn("vigencia", self.gw.submit(self.cmd(ttl_ms=3_600_000)).reason)
        self.assertIn("no permitida", self.gw.submit(self.cmd(cid="z", action="formatear_disco")).reason)

    def test_serializacion_ida_y_vuelta_conserva_la_firma(self):
        c = self.cmd()
        self.assertEqual(self.gw.submit(command_from_json(command_to_json(c))).status, ACCEPTED)

    # ---- ES-07 ----
    def test_critico_con_una_sola_autorizacion_no_se_ejecuta(self):
        r = self.gw.submit(self.cmd(action="open_breaker"))
        self.assertEqual(r.status, PENDING)
        self.assertEqual(self.ej.llamadas, [])

    def test_critico_con_dos_operadores_distintos_se_ejecuta_y_ambos_quedan_registrados(self):
        c = self.cmd(action="open_breaker")
        self.gw.submit(c)
        r = self.gw.approve(c.command_id, "beto", self.beto.approve(c))
        self.assertEqual(r.status, ACCEPTED)
        self.assertEqual(len(self.ej.llamadas), 1)
        fila = [p for e, p in self.audit._db.execute("SELECT event, payload FROM audit") if e == "command_executed"][0]
        self.assertIn('"ana"', fila)
        self.assertIn('"beto"', fila)

    def test_el_mismo_operador_no_puede_aprobar_su_propio_comando(self):
        c = self.cmd(action="open_breaker")
        self.gw.submit(c)
        r = self.gw.approve(c.command_id, "ana", self.ana.approve(c))
        self.assertEqual(r.status, REJECTED)
        self.assertEqual(self.ej.llamadas, [])

    def test_aprobacion_falsa_o_de_llave_desconocida_se_rechaza(self):
        c = self.cmd(action="open_breaker")
        self.gw.submit(c)
        self.assertEqual(self.gw.approve(c.command_id, "beto", b"\x00" * 64).status, REJECTED)
        self.assertEqual(self.gw.approve(c.command_id, "mallory", Operator("mallory").approve(c)).status, REJECTED)
        self.assertEqual(self.ej.llamadas, [])

    def test_aprobacion_de_otro_comando_no_sirve(self):
        c1, c2 = self.cmd(cid="a", action="open_breaker"), self.cmd(cid="b", action="open_breaker")
        self.gw.submit(c1)
        self.gw.submit(c2)
        self.assertEqual(self.gw.approve("b", "beto", self.beto.approve(c1)).status, REJECTED)

    def test_aprobacion_tardia_tras_vencer_se_rechaza(self):
        c = self.cmd(action="open_breaker", ttl_ms=10_000)
        self.gw.submit(c)
        self.reloj.t += 20_000
        self.assertEqual(self.gw.approve(c.command_id, "beto", self.beto.approve(c)).status, REJECTED)
        self.assertEqual(self.ej.llamadas, [])

    def test_no_se_puede_aprobar_dos_veces(self):
        c = self.cmd(action="open_breaker")
        self.gw.submit(c)
        self.gw.approve(c.command_id, "beto", self.beto.approve(c))
        self.assertEqual(self.gw.approve(c.command_id, "beto", self.beto.approve(c)).status, REJECTED)
        self.assertEqual(len(self.ej.llamadas), 1)


if __name__ == "__main__":
    unittest.main()
