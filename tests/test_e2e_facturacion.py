"""Punta a punta: contador -> reenviador (cola en disco) -> log -> procesador -> informe de facturación.

Valida ES-03 + ES-06 + ADR-004 juntos: corte de red, acks perdidos, 5% de duplicados y desorden,
y al final la factura de prueba tiene 0 lecturas duplicadas o perdidas.
"""
import random
import unittest

from cloud.adapters.consumer_memory import MemoryConsumer, MemoryLog
from cloud.adapters.deadletter_memory import MemoryDeadLetter
from cloud.adapters.store_sqlite import SqliteStore
from cloud.core.billing import gap_report
from cloud.core.model import EXPECTED_PER_INTERVAL, INTERVAL_MS, Reading
from cloud.core.processing import StreamProcessor
from edge.adapters.queue_sqlite import SqliteQueue
from edge.core.forwarding import Forwarder, ForwarderConfig

T0 = (1_700_000_000_000 // INTERVAL_MS + 1) * INTERVAL_MS
AHORA = T0 + 10 * INTERVAL_MS
MS = 1_000_000


class Reloj:
    def __init__(self): self.t = 0
    def now_ns(self): return self.t
    def now_ms(self): return AHORA


def generar(meters=3, intervalos=2, faltantes=()):
    out = []
    for m in range(meters):
        for i in range(intervalos * EXPECTED_PER_INTERVAL):
            if (m, i) in faltantes:
                continue
            out.append(Reading(f"m{m}", T0 + i * 5000, i, 0.5))
    return out


def correr(lecturas, semilla=7):
    rnd = random.Random(semilla)
    log, reloj = MemoryLog(partitions=4), Reloj()
    fwd = Forwarder(SqliteQueue(":memory:"), log, reloj, ForwarderConfig(batch_size=100, backoff_base_ms=100))
    desordenadas = lecturas[:]
    rnd.shuffle(desordenadas)                                   # desorden de llegada
    duplicadas = rnd.sample(lecturas, k=len(lecturas) // 20)    # 5% de duplicados
    mitad = len(desordenadas) // 2
    fwd.ingest_many(desordenadas[:mitad])
    log.online = False                                          # corte de red a mitad de camino
    for _ in range(3):
        fwd.tick(); reloj.t += 60_000 * MS
    fwd.ingest_many(desordenadas[mitad:])                       # la microred sigue midiendo sin nube
    log.online = True
    log.lose_next_acks = 2                                      # y al volver se pierden dos acks
    for _ in range(30):
        reloj.t += 60_000 * MS
        fwd.tick()
    fwd.ingest_many(duplicadas)                                 # el contador reintenta lecturas ya enviadas
    for _ in range(10):
        reloj.t += 60_000 * MS
        fwd.tick()
    assert fwd.backlog == 0
    store, dlq = SqliteStore(), MemoryDeadLetter()
    proc = StreamProcessor(MemoryConsumer(log), store, dlq, reloj)
    proc.run_until_idle(idle_polls=1)
    return log, store, dlq, proc


class FacturacionTests(unittest.TestCase):
    def test_factura_de_prueba_sin_duplicadas_ni_perdidas(self):
        lecturas = generar()
        log, store, dlq, proc = correr(lecturas)
        self.assertGreater(log.total(), len(lecturas))            # el log SÍ tiene duplicados crudos
        self.assertGreater(proc.stats.duplicates, 0)              # y el procesador los descartó
        self.assertEqual(dlq.items, [])
        # Ruta 1: lecturas crudas. Ruta 2: agregados. Deben reconciliar con tolerancia 0.
        n, suma = store.raw_totals()
        self.assertEqual(n, len(lecturas))
        rep = gap_report(store.closed_aggregates(AHORA))
        self.assertEqual(rep.gaps, [])
        self.assertEqual(rep.complete_intervals, 6)
        self.assertEqual(rep.billable_kwh, suma)
        self.assertEqual(rep.billable_kwh, 0.5 * len(lecturas))

    def test_huecos_se_reportan_y_no_se_facturan(self):
        faltantes = {(1, i) for i in range(10)}                   # el contador m1 nunca envió 10 lecturas
        lecturas = generar(faltantes=faltantes)
        _, store, _, _ = correr(lecturas)
        rep = gap_report(store.closed_aggregates(AHORA))
        self.assertEqual(rep.missing_readings, 10)
        self.assertEqual(len(rep.gaps), 1)
        self.assertEqual((rep.gaps[0].meter_id, rep.gaps[0].n), ("m1", 170))
        self.assertEqual(rep.complete_intervals, 5)
        self.assertEqual(rep.billable_kwh, 5 * EXPECTED_PER_INTERVAL * 0.5)

    def test_intervalo_abierto_no_se_factura(self):
        lecturas = generar(meters=1, intervalos=1)
        _, store, _, _ = correr(lecturas)
        self.assertEqual(store.closed_aggregates(T0 + 1000), [])   # el intervalo aún no termina


if __name__ == "__main__":
    unittest.main()
