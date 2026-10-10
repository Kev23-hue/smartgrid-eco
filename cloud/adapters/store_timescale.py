"""Almacén PostgreSQL/TimescaleDB (psycopg 3). Misma semántica que SqliteStore.

Una sola transacción por lote: inserta con ON CONFLICT DO NOTHING (la clave primaria deduplica),
recalcula desde las lecturas crudas los agregados de los intervalos tocados y guarda los offsets.
El lote viaja como arreglos (unnest): una sola instrucción por lote en vez de una por lectura.
"""
from __future__ import annotations

from typing import Sequence

from ..core.model import IntervalAggregate, Position, Reading, interval_start_ms

SQL_INSERT = """
WITH ins AS (
    INSERT INTO readings (meter_id, measured_at, seq, kwh)
    SELECT meter_id, to_timestamp(ms / 1000.0), seq, kwh
    FROM unnest(%s::text[], %s::bigint[], %s::bigint[], %s::float8[]) AS t(meter_id, ms, seq, kwh)
    ON CONFLICT (meter_id, measured_at) DO NOTHING
    RETURNING 1)
SELECT count(*) FROM ins
"""

SQL_AGGREGATE = """
INSERT INTO agg_15min (meter_id, interval_start, n, sum_kwh)
SELECT a.meter_id, a.istart, count(*), sum(r.kwh)
FROM (SELECT DISTINCT meter_id, to_timestamp(s / 1000.0) AS istart
      FROM unnest(%s::text[], %s::bigint[]) AS u(meter_id, s)) a
JOIN readings r ON r.meter_id = a.meter_id
               AND r.measured_at >= a.istart
               AND r.measured_at <  a.istart + interval '15 minutes'
GROUP BY a.meter_id, a.istart
ON CONFLICT (meter_id, interval_start)
DO UPDATE SET n = EXCLUDED.n, sum_kwh = EXCLUDED.sum_kwh, updated_at = now()
"""

SQL_OFFSETS = """
INSERT INTO consumer_offsets (topic, partition, next_offset)
SELECT topic, partition, next_offset
FROM unnest(%s::text[], %s::int[], %s::bigint[]) AS t(topic, partition, next_offset)
ON CONFLICT (topic, partition)
DO UPDATE SET next_offset = GREATEST(consumer_offsets.next_offset, EXCLUDED.next_offset)
"""

SQL_NEXT_OFFSET = "SELECT next_offset FROM consumer_offsets WHERE topic = %s AND partition = %s"

SQL_CLOSED = """
SELECT meter_id, round(extract(epoch FROM interval_start) * 1000)::bigint, n, sum_kwh
FROM agg_15min
WHERE interval_start + interval '15 minutes' <= to_timestamp(%s / 1000.0)
ORDER BY meter_id, interval_start
"""

SQL_RAW_TOTALS = "SELECT count(*), coalesce(sum(kwh), 0) FROM readings"
SQL_PREFIX_TOTALS = "SELECT count(*), coalesce(sum(kwh), 0) FROM readings WHERE meter_id LIKE %s"


class TimescaleStore:
    def __init__(self, dsn: str, connection=None):
        if connection is None:
            import psycopg  # type: ignore
            connection = psycopg.connect(dsn, autocommit=True)
        self._conn = connection

    def commit_batch(self, readings: Sequence[Reading], positions: Sequence[Position]) -> int:
        maximos: dict[tuple[str, int], int] = {}
        for p in positions:
            k = (p.topic, p.partition)
            maximos[k] = max(maximos.get(k, -1), p.offset)
        tocados = sorted({(r.meter_id, interval_start_ms(r.measured_at)) for r in readings})
        with self._conn.transaction():
            inserted = 0
            if readings:
                inserted = self._conn.execute(SQL_INSERT, (
                    [r.meter_id for r in readings], [r.measured_at for r in readings],
                    [r.seq for r in readings], [r.kwh for r in readings])).fetchone()[0]
                self._conn.execute(SQL_AGGREGATE, ([m for m, _ in tocados], [i for _, i in tocados]))
            if maximos:
                self._conn.execute(SQL_OFFSETS, (
                    [t for t, _ in maximos], [p for _, p in maximos], [o + 1 for o in maximos.values()]))
        return inserted

    def next_offset(self, topic: str, partition: int) -> int | None:
        row = self._conn.execute(SQL_NEXT_OFFSET, (topic, partition)).fetchone()
        return row[0] if row else None

    def closed_aggregates(self, closed_before_ms: int) -> list[IntervalAggregate]:
        return [IntervalAggregate(m, int(i), int(n), float(s))
                for m, i, n, s in self._conn.execute(SQL_CLOSED, (closed_before_ms,)).fetchall()]

    def raw_totals(self) -> tuple[int, float]:
        n, s = self._conn.execute(SQL_RAW_TOTALS).fetchone()
        return int(n), float(s)

    def prefix_totals(self, prefix: str) -> tuple[int, float]:
        n, s = self._conn.execute(SQL_PREFIX_TOTALS, (prefix + "%",)).fetchone()
        return int(n), float(s)

    def truncate_all(self) -> None:
        self._conn.execute("TRUNCATE readings, agg_15min, consumer_offsets")

    def close(self) -> None:
        self._conn.close()
