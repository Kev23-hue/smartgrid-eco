"""Log de eventos en un archivo SQLite compartido entre procesos. SUSTITUTO de Redpanda para ensayar
la prueba de carga sin Docker (por ejemplo en CI o en un equipo sin contenedores).

No mide nada de Redpanda: solo permite correr el mismo orquestador, las mismas fallas y la misma
verificación. Las cifras de rendimiento de la entrega se toman con Redpanda real.

Fallas controlables desde fuera por archivos en la carpeta de control:
  broker.down        -> publish() lanza BrokerUnavailable
  lose_acks          -> contiene un entero N: los próximos N lotes se guardan pero responden error
"""
from __future__ import annotations

import os
import sqlite3
from typing import Sequence

from ..core.model import Reading
from ..core.ports import BrokerUnavailable
from .codec import reading_to_json


def _connect(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)")
    return db


class FileLogBroker:
    def __init__(self, path: str, control_dir: str):
        self._db = _connect(path)
        self._ctl = control_dir

    def publish(self, batch: Sequence[Reading]) -> None:
        if os.path.exists(os.path.join(self._ctl, "broker.down")):
            raise BrokerUnavailable("broker caído (inyectado)")
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.executemany("INSERT INTO log (body) VALUES (?)", [(reading_to_json(r),) for r in batch])
            self._db.execute("COMMIT")
        except Exception as e:  # noqa: BLE001
            self._db.execute("ROLLBACK")
            raise BrokerUnavailable(str(e)) from e
        ruta = os.path.join(self._ctl, "lose_acks")
        if os.path.exists(ruta):
            try:
                n = int(open(ruta).read().strip() or "0")
            except (ValueError, OSError):
                n = 0
            if n > 0:
                open(ruta, "w").write(str(n - 1))
                raise BrokerUnavailable("ack perdido (inyectado)")

    def size(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM log").fetchone()[0]
