"""ES-03 (autonomía y reconciliación) y ES-01 (sin pérdida) a nivel del reenviador, con log simulado."""
import unittest

from edge.adapters.broker_fake import FakeBroker
from edge.adapters.queue_sqlite import SqliteQueue
from edge.core.forwarding import Forwarder, ForwarderConfig
from edge.core.model import Reading

MS = 1_000_000


class FakeClock:
    def __init__(self):
        self.t = 0

    def now_ns(self) -> int:
        return self.t

    def advance_ms(self, ms: float) -> None:
        self.t += int(ms * MS)


def lecturas(n, meter="m1", start=0, seq0=0):
    return [Reading(meter, 1_700_000_000_000 + (start + i) * 1000, seq0 + i, 0.5) for i in range(n)]


def armar(max_rows=1_000_000, **cfg):
    q, b, c = SqliteQueue(":memory:", max_rows=max_rows), FakeBroker(), FakeClock()
    return Forwarder(q, b, c, ForwarderConfig(**cfg)), q, b, c


class ReenviadorTests(unittest.TestCase):
    def test_envio_normal_vacia_la_cola(self):
        f, q, b, _ = armar(batch_size=100)
        f.ingest_many(lecturas(1000))
        f.drain()
        self.assertEqual(q.size(), 0)
        self.assertEqual(len(b.received), 1000)
        self.assertEqual(f.stats.published, 1000)

    def test_ingerir_no_depende_de_la_nube(self):
        """ES-03: con el log caído, ingerir sigue funcionando y ni siquiera intenta la red."""
        f, q, b, _ = armar()
        b.online = False
        self.assertEqual(f.ingest_many(lecturas(500)), 500)
        self.assertEqual(b.publish_calls, 0)
        self.assertEqual(q.size(), 500)

    def test_corte_de_red_no_pierde_nada_y_reconcilia(self):
        f, q, b, c = armar(batch_size=100)
        b.online = False
        f.ingest_many(lecturas(300, "m1"))
        f.ingest_many(lecturas(300, "m2", seq0=0))
        for _ in range(5):                       # el corte dura un rato
            f.tick()
            c.advance_ms(60_000)
        self.assertEqual(q.size(), 600)          # todo sigue en disco
        self.assertEqual(len(b.received), 0)
        b.online = True                          # vuelve la red
        c.advance_ms(60_000)
        f.drain()
        self.assertEqual(q.size(), 0)
        esperadas = {r.key for r in lecturas(300, "m1")} | {r.key for r in lecturas(300, "m2")}
        self.assertEqual(b.unique_keys(), esperadas)   # reconciliación completa
        self.assertEqual(len(b.received), 600)         # y sin duplicados en este caso

    def test_orden_por_contador_se_conserva(self):
        f, _, b, _ = armar(batch_size=7)
        f.ingest_many(lecturas(50, "m1"))
        f.drain()
        self.assertEqual([r.seq for r in b.received], list(range(50)))

    def test_ack_perdido_produce_duplicados_pero_no_perdidas(self):
        """ADR-004: al menos una vez. El lote se reenvía; la clave permite descartar duplicados."""
        f, q, b, c = armar(batch_size=100)
        f.ingest_many(lecturas(100))
        b.lose_next_acks = 1
        f.tick()                                  # el log lo guardó pero el ack se perdió
        self.assertEqual(q.size(), 100)           # sigue en cola
        c.advance_ms(10_000)
        f.tick()                                  # reintento: llega otra vez
        self.assertEqual(q.size(), 0)
        self.assertEqual(len(b.received), 200)    # duplicado crudo
        self.assertEqual(len(b.unique_keys()), 100)  # 0 perdidas, 0 duplicadas tras deduplicar

    def test_repetidas_en_cola_se_ignoran(self):
        f, q, _, _ = armar()
        f.ingest_many(lecturas(10))
        self.assertEqual(f.ingest_many(lecturas(10)), 0)
        self.assertEqual(q.size(), 10)
        self.assertEqual(f.stats.duplicates_ignored, 10)

    def test_cola_llena_se_cuenta_como_perdida_explicita(self):
        f, q, _, _ = armar(max_rows=100)
        self.assertEqual(f.ingest_many(lecturas(100)), 100)
        self.assertEqual(f.ingest_many(lecturas(10, start=1000)), 0)
        self.assertEqual(f.stats.rejected_full, 10)
        self.assertEqual(q.size(), 100)           # la transacción fallida no dejó restos

    def test_espera_exponencial_con_tope(self):
        f, _, b, c = armar(backoff_base_ms=200, backoff_max_ms=1000)
        b.online = False
        f.ingest_many(lecturas(1))
        esperas = []
        for _ in range(6):
            c.t = max(c.t, f.next_attempt_ns)
            antes = c.t
            f.tick()
            esperas.append((f.next_attempt_ns - antes) // MS)
        self.assertEqual(esperas, [200, 400, 800, 1000, 1000, 1000])

    def test_no_reintenta_antes_de_tiempo_y_se_recupera(self):
        f, _, b, c = armar(backoff_base_ms=1000)
        b.online = False
        f.ingest_many(lecturas(5))
        f.tick()
        llamadas = b.publish_calls
        c.advance_ms(500)
        f.tick()                                  # aún en espera
        self.assertEqual(b.publish_calls, llamadas)
        b.online = True
        c.advance_ms(600)
        self.assertEqual(f.tick(), 5)
        self.assertEqual(f.stats.consecutive_failures, 0)


if __name__ == "__main__":
    unittest.main()
