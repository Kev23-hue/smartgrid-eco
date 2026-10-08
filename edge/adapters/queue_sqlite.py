"""Cola duradera en disco sobre SQLite (modo WAL). Implementa QueuePort.

Decisiones:
- synchronous=FULL por defecto: una lectura confirmada al llamador sobrevive a un corte de energía.
  NORMAL es más rápido pero puede perder las últimas transacciones si cae el sistema operativo.
- Se escribe por grupos (put_many en una transacción): un commit por lectura no alcanza el pico.
- Seguro entre hilos (un candado): un hilo ingiere y otro envía; el envío bloqueado por la red
  no puede frenar la ingesta (ES-03).
- La clave (meter_id|measured_at) es UNIQUE: una lectura repetida que aún está en cola se ignora.
  Una repetida que llega DESPUÉS de haber sido enviada sí se reenvía; la descarta el procesador.
"""
from __future__ import annotations

import sqlite3
import threading
from typing import Sequence

from ..core.model import Reading
from ..core.ports import QueueFull
from .codec import reading_from_json, reading_to_json


class SqliteQueue:
    def __init__(self, path: str = ":memory:", max_rows: int = 1_000_000, synchronous: str = "FULL"):
        if synchronous not in ("FULL", "NORMAL"):
            raise ValueError("synchronous debe ser FULL o NORMAL")
        self._max_rows = max_rows
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)  # transacciones manuales
        if path != ":memory:":
            self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(f"PRAGMA synchronous={synchronous}")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS q (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
            "key TEXT NOT NULL UNIQUE, body TEXT NOT NULL)")
        self._count = self._db.execute("SELECT COUNT(*) FROM q").fetchone()[0]

    def put_many(self, readings: Sequence[Reading]) -> int:
        with self._lock:
            return self._put_many(readings)

    def _put_many(self, readings: Sequence[Reading]) -> int:
        rows = [(r.key, reading_to_json(r)) for r in readings]
        self._db.execute("BEGIN IMMEDIATE")
        try:
            before = self._db.total_changes
            self._db.executemany("INSERT OR IGNORE INTO q (key, body) VALUES (?, ?)", rows)
            new = self._db.total_changes - before
            if self._count + new > self._max_rows:
                self._db.execute("ROLLBACK")
                raise QueueFull(f"cola llena: {self._count} de {self._max_rows}")
            self._db.execute("COMMIT")
        except QueueFull:
            raise
        except Exception:
            self._db.execute("ROLLBACK")
            raise
        self._count += new
        return new

    def peek(self, n: int) -> list[tuple[int, Reading]]:
        with self._lock:
            rows = self._db.execute("SELECT seq, body FROM q ORDER BY seq LIMIT ?", (n,)).fetchall()
        return [(seq, reading_from_json(body)) for seq, body in rows]

    def ack(self, seqs: Sequence[int]) -> None:
        if not seqs:
            return
        with self._lock:
            self._ack(seqs)

    def _ack(self, seqs: Sequence[int]) -> None:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.executemany("DELETE FROM q WHERE seq = ?", [(s,) for s in seqs])
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            raise
        self._count -= len(seqs)

    def size(self) -> int:
        with self._lock:
            return self._count

    def close(self) -> None:
        with self._lock:
            self._db.close()
