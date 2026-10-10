-- Solo con TimescaleDB. Se ejecuta después de 01_schema.sql, con la tabla aún vacía.
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('readings', 'measured_at', chunk_time_interval => interval '1 day', if_not_exists => TRUE);
