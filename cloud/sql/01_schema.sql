-- Esquema de la ingesta (PostgreSQL estándar; funciona con o sin TimescaleDB).
-- La clave primaria (meter_id, measured_at) ES la deduplicación (ADR-004): es el "índice único"
-- cuyo costo de escritura debe medirse (S16). Incluye measured_at porque TimescaleDB exige que
-- todo índice único de una hipertabla contenga la columna de tiempo.

CREATE TABLE IF NOT EXISTS readings (
    meter_id    text             NOT NULL,
    measured_at timestamptz      NOT NULL,
    seq         bigint           NOT NULL,
    kwh         double precision NOT NULL,   -- ver nota en el README: facturar exige decimal exacto
    PRIMARY KEY (meter_id, measured_at)
);

-- Agregados de 15 minutos: siempre recalculados desde las lecturas crudas (determinista).
CREATE TABLE IF NOT EXISTS agg_15min (
    meter_id       text             NOT NULL,
    interval_start timestamptz      NOT NULL,
    n              integer          NOT NULL,
    sum_kwh        double precision NOT NULL,
    updated_at     timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (meter_id, interval_start)
);

-- Offsets del log, guardados en la MISMA transacción que las lecturas (ADR-004).
CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic       text   NOT NULL,
    partition   int    NOT NULL,
    next_offset bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);
