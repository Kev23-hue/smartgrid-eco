"""Productor hacia Redpanda/Kafka (confluent-kafka). Implementa BrokerPort.

- acks=all + enable.idempotence: el log confirma tras replicar y no duplica por reintentos internos.
- La clave del mensaje es meter_id: todas las lecturas de un contador van a la misma partición,
  así se conserva su orden (el log solo ordena dentro de una partición).
- publish() bloquea hasta conocer el resultado de TODO el lote. Si algo falla, lanza
  BrokerUnavailable y el reenviador reenvía el lote completo (al menos una vez).
- message.timeout.ms < flush_timeout: todo mensaje termina en éxito o error antes de que
  flush() se rinda, así no quedan mensajes "fantasma" que se envíen después sin confirmar.

confluent_kafka se importa al construir el objeto, para que las pruebas corran sin instalarlo.
"""
from __future__ import annotations

from typing import Sequence

from ..core.model import Reading
from ..core.ports import BrokerUnavailable
from .codec import reading_to_json


class KafkaBroker:
    def __init__(self, bootstrap_servers: str, topic: str, client_id: str = "edge-1",
                 flush_timeout_s: float = 15.0, message_timeout_ms: int = 8000,
                 compression: str = "gzip", extra_conf: dict | None = None, producer=None):
        if producer is None:
            from confluent_kafka import Producer  # type: ignore
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": compression,
                "linger.ms": 20,
                "message.timeout.ms": message_timeout_ms,
                "socket.timeout.ms": 5000,
            }
            conf.update(extra_conf or {})
            producer = Producer(conf)
        self._p = producer
        self._topic = topic
        self._flush_timeout_s = flush_timeout_s

    def publish(self, batch: Sequence[Reading]) -> None:
        errors: list = []

        def on_delivery(err, _msg):
            if err is not None:
                errors.append(err)

        for r in batch:
            for _ in range(200):  # cola local llena: dar tiempo a que se libere
                try:
                    self._p.produce(self._topic, key=r.meter_id.encode(),
                                    value=reading_to_json(r).encode(), on_delivery=on_delivery)
                    break
                except BufferError:
                    self._p.poll(0.05)
            else:
                raise BrokerUnavailable("cola local del productor llena")
        remaining = self._p.flush(self._flush_timeout_s)
        if remaining:
            raise BrokerUnavailable(f"{remaining} mensajes sin confirmar tras {self._flush_timeout_s}s")
        if errors:
            raise BrokerUnavailable(f"{len(errors)} mensajes rechazados; primero: {errors[0]}")
