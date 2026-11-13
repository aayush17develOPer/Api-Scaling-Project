-- Run once against the real Postgres (port 5432), not through PgBouncer:
--   psql -h 127.0.0.1 -p 5432 -U bench -d test -f schema.sql
CREATE TABLE IF NOT EXISTS events (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload     JSONB       NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Benchmark-only options (you give up durability, so say so if you quote numbers):
--   ALTER TABLE events SET UNLOGGED;                 -- skips WAL
--   ALTER SYSTEM SET synchronous_commit = off;       -- then: SELECT pg_reload_conf();