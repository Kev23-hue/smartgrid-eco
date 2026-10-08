import unittest

from edge.core.balancing import decide
from edge.core.model import BatteryState, GridState, Policy

P = Policy(version=1, max_discharge_kw=60, max_charge_kw=60, soc_min=0.2, soc_max=0.95, tolerance_kw=1.0)


def st(gen, dem, soc=0.6):
    return GridState(gen, dem, BatteryState(soc, 200.0))


class BalancingTests(unittest.TestCase):
    def test_balanceado_no_hace_nada(self):
        self.assertTrue(decide(st(100, 100.5), P).is_noop)

    def test_deficit_se_cubre_con_bateria(self):
        d = decide(st(70, 120), P)  # deficit 50 <= 60
        self.assertEqual(d.battery_kw, 50)
        self.assertEqual(d.shed_kw, 0)
        self.assertEqual(d.reason, "deficit_cubierto")

    def test_deficit_grande_desconecta_carga(self):
        d = decide(st(20, 120), P)  # deficit 100: bateria 60 + desconexion 40
        self.assertEqual((d.battery_kw, d.shed_kw), (60, 40))

    def test_bateria_en_minimo_no_descarga(self):
        d = decide(st(70, 120, soc=0.2), P)
        self.assertEqual((d.battery_kw, d.shed_kw), (0, 50))

    def test_excedente_se_almacena(self):
        d = decide(st(140, 120), P)
        self.assertEqual((d.battery_kw, d.curtail_kw), (-20, 0))

    def test_bateria_llena_recorta(self):
        d = decide(st(140, 120, soc=0.95), P)
        self.assertEqual((d.battery_kw, d.curtail_kw), (0, 20))

    def test_la_energia_siempre_cuadra(self):
        # generacion + descarga - carga - recorte == demanda - desconexion
        for gen in range(0, 300, 7):
            for soc in (0.1, 0.2, 0.6, 0.95, 1.0):
                s = st(gen, 120, soc)
                d = decide(s, P)
                if d.is_noop:
                    continue
                lhs = gen + d.battery_kw - d.curtail_kw
                rhs = 120 - d.shed_kw
                self.assertAlmostEqual(lhs, rhs, places=6, msg=f"gen={gen} soc={soc}")


if __name__ == "__main__":
    unittest.main()
