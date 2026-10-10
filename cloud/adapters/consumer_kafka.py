"""Consumidor de Redpanda/Kafka (confluent-kafka). Implementa ConsumerPort.

Punto clave de ADR-004: el offset verdadero vive en la BD, guardado junto con las lecturas.
Al asignarse particiones, el consumidor salta a ese offset (on_assign), sin importar lo que
Kafka crea que ya se confirmó. commit() hacia Kafka es solo informativo (para ver el atraso).
"""
from __future__ import annotations

from typing import Callable, Sequence

from edge.adapters.codec import reading_from_json
from ..core.model import Message, Position


class KafkaConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str,
                 offset_lookup: Callable[[str, int], int | None],
                 consumer=None, tp_factory=None, extra_conf: dict | None = None):
        if consumer is None:
            from confluent_kafka import Consumer, TopicPartition  # type: ignore
            conf = {"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                    "enable.auto.commit": False, "auto.offset.reset": "earliest"}
            conf.update(extra_conf or {})
            consumer = Consumer(conf)
            tp_factory = TopicPartition
        self._c, self._tp, self._lookup = consumer, tp_factory, offset_lookup
        self._c.subscribe([topic], on_assign=self._on_assign)

    def _on_assign(self, consumer, partitions):
        for p in partitions:
            siguiente = self._lookup(p.topic, p.partition)
            if siguiente is not None:
                p.offset = siguiente
        consumer.assign(partitions)

    def poll(self, max_messages: int, timeout_s: float) -> list[Message]:
        out: list[Message] = []
        for m in self._c.consume(num_messages=max_messages, timeout=timeout_s):
            if m.error():
                continue   # errores transitorios del cliente; el siguiente poll reintenta
            pos = Position(m.topic(), m.partition(), m.offset())
            try:
                out.append(Message(pos, reading_from_json(m.value().decode("utf-8"))))
            except Exception as e:  # noqa: BLE001  (basura en el log: va a mensajes muertos)
                out.append(Message(pos, None, str(e)))
        return out

    def commit(self, positions: Sequence[Position]) -> None:
        maximos: dict[tuple[str, int], int] = {}
        for p in positions:
            k = (p.topic, p.partition)
            maximos[k] = max(maximos.get(k, -1), p.offset)
        try:
            self._c.commit(offsets=[self._tp(t, part, off + 1) for (t, part), off in maximos.items()],
                           asynchronous=False)
        except Exception:  # noqa: BLE001  (informativo: la BD manda)
            pass

    def close(self) -> None:
        self._c.close()
