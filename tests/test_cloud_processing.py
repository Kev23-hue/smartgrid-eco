"""Procesador de flujos: validación, mensajes muertos, transaccionalidad y reanudación."""
import unittest

from cloud.adapters.consumer_memory import MemoryConsumer, MemoryLog
from cloud.adapters.deadletter_memory import MemoryDeadLetter
from cloud.adapters.store_sqlite import SqliteStore
from cloud.core.model import INTERVAL_MS, Position, Reading
from cloud.core.processing import StreamProcessor


class RelojFijo:
    def __init__(self, ms):
        self.ms = ms

    def now_ms(self):
        return self.ms


T0 = (1_700_000_000_000 // INTERVAL_MS + 1) * INTERVAL_MS   # inicio de un intervalo de 15 min
AHORA = T0 + 86_400_000


def lectura(i, meter="m1", kwh=0.5):
    return Reading(meter, T0 + i * 5000, i, kwh)


def armar(log=None):
    log = log or MemoryLog(partitions=2)
    store, dlq = SqliteStore(), MemoryDeadLetter()
    proc = StreamProcessor(MemoryConsumer(log), store, dlq, RelojFijo(AHORA))
    return log, store, dlq, proc


class ProcesadorTests(unittest.TestCase):
    def test_lecturas_validas_se_escriben(self):
        log, store, dlq, proc = armar()
        log.publish([lectura(i) for i in range(10)])
        proc.run_until_idle(idle_polls=1)
        self.assertEqual(store.raw_totals(), (10, 5.0))
        self.assertEqual(proc.stats.inserted, 10)
        self.assertEqual(dlq.items, [])

    def test_duplicados_se_descartan_por_clave(self):
        log, store, _, proc = armar()
        log.publish([lectura(i) for i in range(10)])
        log.publish([lectura(i) for i in range(5)])     # reenvío
        proc.run_until_idle(idle_polls=1)
        self.assertEqual(store.raw_totals()[0], 10)
        self.assertEqual(proc.stats.duplicates, 5)

    def test_invalidos_van_a_mensajes_muertos(self):
        log, store, dlq, proc = armar()
        log.publish([lectura(0)])
        log.inject_raw(0, "esto no es json")
        log.publish([Reading("m1", T0 + 99, 1, -3.0),              # kwh negativo
                     Reading("m1", AHORA + 3_600_000, 2, 1.0),     # del futuro
                     Reading("", T0 + 5, 3, 1.0)])                 # sin contador
        proc.run_until_idle(idle_polls=1)
        self.assertEqual(store.raw_totals()[0], 1)
        self.assertEqual(proc.stats.rejected, 4)
        razones = " | ".join(r for _, r in dlq.items)
        for esperado in ("no decodificable", "kwh fuera de rango", "futuro", "meter_id vacío"):
            self.assertIn(esperado, razones)

    def test_offsets_se_guardan_con_las_lecturas(self):
        log, store, _, proc = armar(MemoryLog(partitions=1))
        log.publish([lectura(i) for i in range(7)])
        proc.run_until_idle(idle_polls=1)
        self.assertEqual(store.next_offset(log.topic, 0), 7)
        self.assertIsNone(store.next_offset(log.topic, 5))

    def test_reanudar_desde_la_bd_no_reprocesa(self):
        log, store, dlq, proc = armar(MemoryLog(partitions=1))
        log.publish([lectura(i) for i in range(10)])
        proc.run_until_idle(idle_polls=1)
        # "el proceso muere y arranca de nuevo": retoma desde el offset de la BD
        inicio = {0: store.next_offset(log.topic, 0)}
        proc2 = StreamProcessor(MemoryConsumer(log, inicio), store, dlq, RelojFijo(AHORA))
        log.publish([lectura(i) for i in range(10, 15)])
        proc2.run_until_idle(idle_polls=1)
        self.assertEqual(proc2.stats.consumed, 5)
        self.assertEqual(store.raw_totals()[0], 15)

    def test_releer_todo_el_log_da_el_mismo_resultado(self):
        """Kappa: reprocesar desde el inicio es seguro porque la escritura es idempotente."""
        log, store, dlq, proc = armar()
        log.publish([lectura(i, f"m{i % 3}") for i in range(60)])
        proc.run_until_idle(idle_polls=1)
        antes = (store.raw_totals(), store.closed_aggregates(AHORA))
        proc2 = StreamProcessor(MemoryConsumer(log), store, dlq, RelojFijo(AHORA))   # desde el offset 0
        proc2.run_until_idle(idle_polls=1)
        self.assertEqual(proc2.stats.inserted, 0)
        self.assertEqual((store.raw_totals(), store.closed_aggregates(AHORA)), antes)

    def test_lote_que_falla_no_deja_nada_a_medias(self):
        log, store, dlq, proc = armar(MemoryLog(partitions=1))
        log.publish([lectura(i) for i in range(5)])

        class Roto:
            def __init__(self, db): self._db = db
            def __getattr__(self, n): return getattr(self._db, n)
            def executemany(self, sql, rows):
                if "consumer_offsets" in sql:
                    raise RuntimeError("falla simulada al guardar offsets")
                return self._db.executemany(sql, rows)

        store._db = Roto(store._db)
        with self.assertRaises(RuntimeError):
            proc.run_once()
        store._db = store._db._db
        self.assertEqual(store.raw_totals()[0], 0)                  # las lecturas se revirtieron
        self.assertIsNone(store.next_offset(log.topic, 0))
        # al reintentar con un consumidor nuevo, todo se aplica una vez
        proc2 = StreamProcessor(MemoryConsumer(log), store, dlq, RelojFijo(AHORA))
        proc2.run_until_idle(idle_polls=1)
        self.assertEqual(store.raw_totals()[0], 5)


if __name__ == "__main__":
    unittest.main()
