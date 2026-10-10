"""Consumidor Kafka con un cliente falso. La prueba real es con Redpanda (GUIA_ETAPA3.md)."""
import unittest
from collections import namedtuple

from cloud.adapters.consumer_kafka import KafkaConsumer
from cloud.core.model import Position
from edge.adapters.codec import reading_to_json
from edge.core.model import Reading

TP = namedtuple("TP", "topic partition offset")


class Msg:
    def __init__(self, t, p, o, v, err=None):
        self._a = (t, p, o, v, err)
    def topic(self): return self._a[0]
    def partition(self): return self._a[1]
    def offset(self): return self._a[2]
    def value(self): return self._a[3]
    def error(self): return self._a[4]


class ClienteFalso:
    def __init__(self, msgs):
        self.msgs, self.asignadas, self.confirmadas = msgs, None, None
    def subscribe(self, topics, on_assign): self.on_assign = on_assign
    def assign(self, partitions): self.asignadas = partitions
    def consume(self, num_messages, timeout): return self.msgs
    def commit(self, offsets, asynchronous): self.confirmadas = offsets
    def close(self): pass


class _P:   # partición asignada, mutable como TopicPartition real
    def __init__(self, t, p): self.topic, self.partition, self.offset = t, p, -1001


class ConsumidorKafkaTests(unittest.TestCase):
    def test_al_asignar_salta_al_offset_de_la_bd(self):
        c = ClienteFalso([])
        KafkaConsumer("x", "telemetry.raw", "g", lambda t, p: {0: 42}.get(p), consumer=c, tp_factory=TP)
        ps = [_P("telemetry.raw", 0), _P("telemetry.raw", 1)]
        c.on_assign(c, ps)
        self.assertEqual((ps[0].offset, ps[1].offset), (42, -1001))   # la 1 no tiene dato en la BD: no se toca
        self.assertIs(c.asignadas, ps)

    def test_decodifica_y_marca_basura(self):
        buena = reading_to_json(Reading("m1", 1000, 1, 0.5)).encode()
        c = ClienteFalso([Msg("t", 0, 5, buena), Msg("t", 0, 6, b"{no es json"), Msg("t", 0, 7, b"", err="x")])
        k = KafkaConsumer("x", "t", "g", lambda t, p: None, consumer=c, tp_factory=TP)
        out = k.poll(10, 0.1)
        self.assertEqual(len(out), 2)                                  # el mensaje con error del cliente se omite
        self.assertEqual(out[0].reading.meter_id, "m1")
        self.assertIsNone(out[1].reading)
        self.assertEqual(out[1].position, Position("t", 0, 6))

    def test_commit_informativo_usa_offset_mas_uno(self):
        c = ClienteFalso([])
        k = KafkaConsumer("x", "t", "g", lambda t, p: None, consumer=c, tp_factory=TP)
        k.commit([Position("t", 0, 3), Position("t", 0, 9), Position("t", 1, 2)])
        self.assertEqual({(o.partition, o.offset) for o in c.confirmadas}, {(0, 10), (1, 3)})


if __name__ == "__main__":
    unittest.main()
