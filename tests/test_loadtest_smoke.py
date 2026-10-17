"""Prueba de humo de la prueba de carga: corre el orquestador completo con el log en archivo
(sin Docker) y una falla de cada tipo. Tarda ~20 s. Si falla, el orquestador está roto."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class CargaSmokeTests(unittest.TestCase):
    def test_chaos_en_archivo_no_pierde_ni_duplica(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "r.json")
            p = subprocess.run(
                [sys.executable, "-m", "loadtest.run", "--backend", "file", "--scenario", "chaos", "--nominal", "300",
                 "--phase-secs", "2", "--generators", "1", "--chaos", "1:broker_down:2,2:db_down:2,3:lose_acks:2,5:kill_processor:1",
                 "--drain-timeout", "60", "--out", out],
                cwd=RAIZ, capture_output=True, text=True, timeout=180)
            self.assertEqual(p.returncode, 0, p.stdout[-800:] + p.stderr[-800:])
            r = json.load(open(out))
            v = r["verificacion"]
            self.assertGreater(v["lecturas_unicas_generadas"], 1000)
            self.assertEqual(v["perdidas"], 0)
            self.assertEqual(v["kwh_diferencia"], 0)
            self.assertTrue(v["PASA_sin_perdidas_ni_duplicados"])
            self.assertEqual(len(r["fallas_inyectadas"]), 4)


if __name__ == "__main__":
    unittest.main()
