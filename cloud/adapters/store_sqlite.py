"""Almacén SQLite con la misma semántica que TimescaleStore. Sirve para pruebas y para correr sin Docker.

Clave primaria (meter_id, measured_at): una lectura repetida se ignora. Todo en una transacción.
"""
from __future__ import annotations

import sqlite3
from typing import Sequence

from ..core.model import INTERVAL_MS, IntervalAggregate, Position, Reading, interval_start_ms


class SqliteStore:
    def __init__(self, path: str = ":memory:"):
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS readings (
                meter_id TEXT NOT NULL, measured_at INTEGER NOT NULL, seq INTEGER NOT NULL, kwh REAL NOT NULL,
                PRIMARY KEY (meter_id, measured_at)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS agg_15min (
                meter_id TEXT NOT NULL, interval_start INTEGER NOT NULL, n INTEGER NOT NULL, sum_kwh REAL NOT NULL,
                PRIMARY KEY (meter_id, interval_start)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS consumer_offsets (
                topic TEXT NOT NULL, partition INTEGER NOT NULL, next_offset INTEGER NOT NULL,
                PRIMARY KEY (topic, partition)) WITHOUT ROWID;
        """)

    def commit_batch(self, readings: Sequence[Reading], positions: Sequence[Position]) -> int:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            before = self._db.total_changes
            self._db.executemany("INSERT OR IGNORE INTO readings VALUES (?,?,?,?)",
                                 [(r.meter_id, r.measured_at, r.seq, r.kwh) for r in readings])
            inserted = self._db.total_changes - before
            if inserted:
                tocados = {(r.meter_id, interval_start_ms(r.measured_at)) for r in readings}
                # Recálculo determinista desde las lecturas crudas, solo de los intervalos tocados.
                self._db.executemany(
                    "INSERT OR REPLACE INTO agg_15min SELECT meter_id, ?2, COUNT(*), SUM(kwh) FROM readings "
                    "WHERE meter_id = ?1 AND measured_at >= ?2 AND measured_at < ?2 + ?3 GROUP BY meter_id",
                    [(m, i, INTERVAL_MS) for m, i in tocados])
            maximos: dict[tuple[str, int], int] = {}
            for p in positions:
                k = (p.topic, p.partition)
                maximos[k] = max(maximos.get(k, -1), p.offset)
            self._db.executemany(
                "INSERT INTO consumer_offsets VALUES (?,?,?) ON CONFLICT (topic, partition) "
                "DO UPDATE SET next_offset = MAX(next_offset, excluded.next_offset)",
                [(t, part, off + 1) for (t, part), off in maximos.items()])
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            raise
        return inserted

    def next_offset(self, topic: str, partition: int) -> int | None:
        row = self._db.execute("SELECT next_offset FROM consumer_offsets WHERE topic=? AND partition=?",
                               (topic, partition)).fetchone()
        return row[0] if row else None

    def closed_aggregates(self, closed_before_ms: int) -> list[IntervalAggregate]:
        cur = self._db.execute(
            "SELECT meter_id, interval_start, n, sum_kwh FROM agg_15min WHERE interval_start + ? <= ? "
            "ORDER BY meter_id, interval_start", (INTERVAL_MS, closed_before_ms))
        return [IntervalAggregate(*row) for row in cur]

    def raw_totals(self) -> tuple[int, float]:
        """Conteo y suma directamente de las lecturas crudas: la segunda ruta para reconciliar (ES-06)."""
        return self._db.execute("SELECT COUNT(*), COALESCE(SUM(kwh), 0) FROM readings").fetchone()

    def close(self) -> None:
        self._db.close()
