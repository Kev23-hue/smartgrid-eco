"""Fábricas de adaptadores según el backend: 'file' (sin Docker, ensayo) o 'kafka' (Redpanda + TimescaleDB reales)."""
from __future__ import annotations

import os


def add_backend_args(ap) -> None:
    ap.add_argument("--backend", choices=["file", "kafka"], default="kafka")
    ap.add_argument("--dir", required=True, help="carpeta de la corrida")
    ap.add_argument("--run", required=True)
    ap.add_argument("--topic", default="telemetry.raw")
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--dsn", default="postgresql://sge:sge@localhost:5433/sge")


def ctl_dir(a) -> str:
    return os.path.join(a.dir, "control")


def make_broker(a, client_id: str):
    if a.backend == "file":
        from edge.adapters.log_file import FileLogBroker
        return FileLogBroker(os.path.join(a.dir, "log.db"), ctl_dir(a))
    from edge.adapters.broker_kafka import KafkaBroker
    return KafkaBroker(a.bootstrap, a.topic, client_id=client_id)


def make_store(a):
    if a.backend == "file":
        from cloud.adapters.store_sqlite import SqliteStore
        return SqliteStore(os.path.join(a.dir, "store.db"))
    from cloud.adapters.store_timescale import TimescaleStore
    return TimescaleStore(a.dsn)


def make_consumer(a, group: str, store):
    if a.backend == "file":
        from cloud.adapters.consumer_filelog import FileLogConsumer
        return FileLogConsumer(os.path.join(a.dir, "log.db"), a.topic, store.next_offset)
    from cloud.adapters.consumer_kafka import KafkaConsumer
    return KafkaConsumer(a.bootstrap, a.topic, group, offset_lookup=store.next_offset)
