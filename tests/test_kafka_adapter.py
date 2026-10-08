"""El adaptador Kafka con un productor falso: valida el manejo de errores sin necesitar Redpanda.
La prueba real contra Redpanda es edge/forwarder_demo.py (requiere Docker)."""
import unittest

from edge.adapters.broker_kafka import KafkaBroker
from edge.core.model import Reading
from edge.core.ports import BrokerUnavailable

LOTE = [Reading("m1", 1000 + i, i, 1.0) for i in range(5)]


class ProductorFalso:
    def __init__(self, modo="ok"):
        self.modo, self.enviados, self._cbs = modo, [], []
        self.buffer_error_veces = 0

    def produce(self, topic, key, value, on_delivery):
        if self.buffer_error_veces > 0:
            self.buffer_error_veces -= 1
            raise BufferError("cola local llena")
        self.enviados.append((topic, key, value))
        self._cbs.append(on_delivery)

    def poll(self, t):
        return 0

    def flush(self, timeout):
        if self.modo == "timeout":
            return len(self._cbs)
        for cb in self._cbs:
            cb("ERROR_DE_ENTREGA" if self.modo == "error" else None, None)
        return 0


def armar(modo="ok"):
    p = ProductorFalso(modo)
    return KafkaBroker("x:9092", "telemetry.raw", producer=p), p


class KafkaAdapterTests(unittest.TestCase):
    def test_exito(self):
        b, p = armar()
        b.publish(LOTE)
        self.assertEqual(len(p.enviados), 5)
        self.assertEqual(p.enviados[0][1], b"m1")            # clave = meter_id
        self.assertEqual(p.enviados[0][0], "telemetry.raw")

    def test_flush_con_pendientes_lanza(self):
        b, _ = armar("timeout")
        with self.assertRaises(BrokerUnavailable):
            b.publish(LOTE)

    def test_error_de_entrega_lanza(self):
        b, _ = armar("error")
        with self.assertRaises(BrokerUnavailable):
            b.publish(LOTE)

    def test_cola_local_llena_reintenta_y_pasa(self):
        b, p = armar()
        p.buffer_error_veces = 3
        b.publish(LOTE)
        self.assertEqual(len(p.enviados), 5)

    def test_cola_local_siempre_llena_lanza(self):
        b, p = armar()
        p.buffer_error_veces = 10_000
        with self.assertRaises(BrokerUnavailable):
            b.publish(LOTE[:1])


if __name__ == "__main__":
    unittest.main()
