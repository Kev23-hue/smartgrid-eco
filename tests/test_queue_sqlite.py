"""La cola debe sobrevivir a un reinicio: es lo que hace creíble el almacenar-y-reenviar."""
import os
import tempfile
import unittest

from edge.adapters.queue_sqlite import SqliteQueue
from edge.core.model import Reading
from edge.core.ports import QueueFull


def r(i, meter="m1"):
    return Reading(meter, 1_700_000_000_000 + i, i, 1.0)


class ColaTests(unittest.TestCase):
    def test_sobrevive_a_reinicio(self):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "cola.db")
            q = SqliteQueue(ruta)
            q.put_many([r(i) for i in range(100)])
            q.close()                              # "el proceso muere"
            q2 = SqliteQueue(ruta)                 # "arranca de nuevo"
            self.assertEqual(q2.size(), 100)
            self.assertEqual([x.seq for _, x in q2.peek(3)], [0, 1, 2])
            q2.close()

    def test_ack_quita_solo_lo_confirmado(self):
        q = SqliteQueue()
        q.put_many([r(i) for i in range(10)])
        lote = q.peek(4)
        q.ack([s for s, _ in lote])
        self.assertEqual(q.size(), 6)
        self.assertEqual(q.peek(1)[0][1].seq, 4)

    def test_clave_repetida_dentro_del_mismo_lote(self):
        q = SqliteQueue()
        self.assertEqual(q.put_many([r(1), r(1), r(2)]), 2)
        self.assertEqual(q.size(), 2)

    def test_limite_y_reversion(self):
        q = SqliteQueue(max_rows=5)
        q.put_many([r(i) for i in range(5)])
        with self.assertRaises(QueueFull):
            q.put_many([r(100), r(101)])
        self.assertEqual(q.size(), 5)
        self.assertEqual(len(q.peek(100)), 5)

    def test_tras_ack_el_espacio_se_libera(self):
        q = SqliteQueue(max_rows=5)
        q.put_many([r(i) for i in range(5)])
        q.ack([s for s, _ in q.peek(5)])
        self.assertEqual(q.put_many([r(i) for i in range(10, 15)]), 5)

    def test_un_hilo_ingiere_mientras_otro_envia(self):
        import threading
        q = SqliteQueue()
        total, enviados = 3000, []

        def productor():
            for i in range(0, total, 50):
                q.put_many([r(j) for j in range(i, i + 50)])

        def consumidor():
            while len(enviados) < total:
                lote = q.peek(100)
                q.ack([s for s, _ in lote])
                enviados.extend(x.seq for _, x in lote)

        hs = [threading.Thread(target=productor), threading.Thread(target=consumidor)]
        [h.start() for h in hs]
        [h.join(timeout=30) for h in hs]
        self.assertEqual(sorted(enviados), list(range(total)))
        self.assertEqual(q.size(), 0)


if __name__ == "__main__":
    unittest.main()
