"""Contrato común de los almacenes: SqliteStore siempre; TimescaleStore si se define SGE_PG_DSN.

Con Docker (ver GUIA_ETAPA3.md):  set SGE_PG_DSN=postgresql://sge:sge@localhost:5433/sge
Sin esa variable las pruebas de PostgreSQL se omiten (skip), no fallan.
"""
import os
import unittest

from cloud.adapters.store_sqlite import SqliteStore
from cloud.core.model import EXPECTED_PER_INTERVAL, INTERVAL_MS, Position, Reading

T0 = (1_700_000_000_000 // INTERVAL_MS + 1) * INTERVAL_MS
CIERRE = T0 + 10 * INTERVAL_MS
DSN = os.environ.get("SGE_PG_DSN")


def lec(i, meter="m1", kwh=0.5):
    return Reading(meter, T0 + i * 5000, i, kwh)


def pos(partition, offset):
    return Position("telemetry.raw", partition, offset)


class ContratoAlmacen:
    def hacer(self):
        raise NotImplementedError

    def setUp(self):
        self.s = self.hacer()

    def test_insertar_dos_veces_es_idempotente(self):
        lote = [lec(i) for i in range(20)]
        self.assertEqual(self.s.commit_batch(lote, [pos(0, 19)]), 20)
        self.assertEqual(self.s.commit_batch(lote, [pos(0, 19)]), 0)
        self.assertEqual(self.s.raw_totals(), (20, 10.0))

    def test_clave_repetida_dentro_del_mismo_lote(self):
        self.assertEqual(self.s.commit_batch([lec(1), lec(1), lec(2)], [pos(0, 2)]), 2)
        self.assertEqual(self.s.raw_totals()[0], 2)

    def test_intervalo_completo_y_cerrado(self):
        self.s.commit_batch([lec(i) for i in range(EXPECTED_PER_INTERVAL)], [pos(0, 179)])
        (a,) = self.s.closed_aggregates(CIERRE)
        self.assertEqual((a.meter_id, a.interval_start, a.n, a.sum_kwh), ("m1", T0, 180, 90.0))
        self.assertTrue(a.complete)

    def test_intervalo_abierto_no_aparece(self):
        self.s.commit_batch([lec(0)], [pos(0, 0)])
        self.assertEqual(self.s.closed_aggregates(T0 + 1000), [])

    def test_llegada_tardia_actualiza_el_agregado(self):
        self.s.commit_batch([lec(i) for i in range(100)], [pos(0, 99)])
        self.assertEqual(self.s.closed_aggregates(CIERRE)[0].n, 100)
        self.s.commit_batch([lec(i) for i in range(100, 180)], [pos(0, 179)])   # llegan tarde
        self.assertEqual(self.s.closed_aggregates(CIERRE)[0].n, 180)

    def test_agregado_reconcilia_con_lo_crudo(self):
        lote = [lec(i, f"m{i % 4}") for i in range(400)]
        self.s.commit_batch(lote, [pos(0, 399)])
        self.s.commit_batch(lote[:50], [pos(0, 49)])                      # reenvío parcial
        n, suma = self.s.raw_totals()
        aggs = self.s.closed_aggregates(CIERRE)
        self.assertEqual(sum(a.n for a in aggs), n)
        self.assertEqual(sum(a.sum_kwh for a in aggs), suma)

    def test_offsets_solo_avanzan(self):
        self.s.commit_batch([lec(0)], [pos(0, 9), pos(1, 4)])
        self.assertEqual((self.s.next_offset("telemetry.raw", 0), self.s.next_offset("telemetry.raw", 1)), (10, 5))
        self.s.commit_batch([], [pos(0, 3)])                                # un lote viejo no retrocede
        self.assertEqual(self.s.next_offset("telemetry.raw", 0), 10)
        self.assertIsNone(self.s.next_offset("telemetry.raw", 7))

    def test_lote_sin_lecturas_solo_guarda_offsets(self):
        self.assertEqual(self.s.commit_batch([], [pos(2, 5)]), 0)
        self.assertEqual(self.s.next_offset("telemetry.raw", 2), 6)


class ContratoSqlite(ContratoAlmacen, unittest.TestCase):
    def hacer(self):
        return SqliteStore()


@unittest.skipUnless(DSN, "defina SGE_PG_DSN para probar contra PostgreSQL/TimescaleDB")
class ContratoTimescale(ContratoAlmacen, unittest.TestCase):
    def hacer(self):
        from cloud.adapters.store_timescale import TimescaleStore
        s = TimescaleStore(DSN)
        s.truncate_all()
        return s


if __name__ == "__main__":
    unittest.main()
