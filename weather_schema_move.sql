-- ============================================================================
-- weather2kafka: move the weather tables from `laddms` to `geo_feeds`.
--
--   psql -h $DB_HOST -U $DB_USER -d $DB_DBNAME -f weather_schema_move.sql
--
-- Run this while the service is stopped. It is safe to run twice.
--
-- `geo_feeds` is where the rest of the 2kafka fleet already writes (grid,
-- water, gauge, fires, lightning, aqi, satellites, roadclosures, rowpermits,
-- hubnashville, anything); weather was the last one still on `laddms`.
--
-- weather_conditions and weather_radar are live hypertables holding data.
-- SET SCHEMA moves them in place: no rewrite, no copy, no data movement. Rows,
-- indexes, constraints, column comments and table-level grants all follow the
-- table. TimescaleDB updates its own catalog as part of the statement; the
-- chunks themselves live in _timescaledb_internal and do not move or care.
--
-- weather_clouds and weather_cloud_layers may or may not exist yet depending on
-- whether the earlier cloud migration was applied. IF EXISTS makes both cases
-- work: if they are there they move, and if they are not, the updated
-- weather_tables.sql creates them directly in geo_feeds afterwards.
-- ============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS geo_feeds;

ALTER TABLE IF EXISTS laddms.weather_conditions   SET SCHEMA geo_feeds;
ALTER TABLE IF EXISTS laddms.weather_radar        SET SCHEMA geo_feeds;
ALTER TABLE IF EXISTS laddms.weather_clouds       SET SCHEMA geo_feeds;
ALTER TABLE IF EXISTS laddms.weather_cloud_layers SET SCHEMA geo_feeds;

COMMIT;


-- ---------------------------------------------------------------------------
-- Verify. Both queries should report geo_feeds for every weather table, and
-- the hypertable query should still list weather_conditions and weather_radar
-- as hypertables (plus the two cloud tables once they exist).
-- ---------------------------------------------------------------------------

SELECT schemaname, tablename
  FROM pg_tables
 WHERE tablename LIKE 'weather%'
 ORDER BY schemaname, tablename;

SELECT hypertable_schema, hypertable_name, num_chunks
  FROM timescaledb_information.hypertables
 WHERE hypertable_name LIKE 'weather%'
 ORDER BY hypertable_schema, hypertable_name;


-- ---------------------------------------------------------------------------
-- Grants. Table-level privileges move with the table, so an explicit GRANT on
-- laddms.weather_conditions is still in force. What does NOT follow is access
-- that came from the *schema* — a `GRANT ... ON ALL TABLES IN SCHEMA laddms`
-- or an ALTER DEFAULT PRIVILEGES scoped to laddms covers these tables no
-- longer. The service user also needs USAGE on geo_feeds.
--
-- Other 2kafka services already write to geo_feeds with the same db-write-user,
-- so this is very likely already in place. Running it anyway costs nothing.
-- Substitute the real role name for <write_user>.
-- ---------------------------------------------------------------------------

-- GRANT USAGE ON SCHEMA geo_feeds TO <write_user>;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON geo_feeds.weather_conditions   TO <write_user>;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON geo_feeds.weather_radar        TO <write_user>;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON geo_feeds.weather_clouds       TO <write_user>;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON geo_feeds.weather_cloud_layers TO <write_user>;


-- ---------------------------------------------------------------------------
-- Optional: compatibility views, if anything still reads laddms.weather_*.
--
-- These keep old queries working against the new location. They are simple
-- single-table views, so Postgres makes them auto-updatable and writes would
-- pass through too. Leave them out if you would rather have stale consumers
-- fail loudly and get fixed.
-- ---------------------------------------------------------------------------

-- CREATE OR REPLACE VIEW laddms.weather_conditions   AS SELECT * FROM geo_feeds.weather_conditions;
-- CREATE OR REPLACE VIEW laddms.weather_radar        AS SELECT * FROM geo_feeds.weather_radar;
-- CREATE OR REPLACE VIEW laddms.weather_clouds       AS SELECT * FROM geo_feeds.weather_clouds;
-- CREATE OR REPLACE VIEW laddms.weather_cloud_layers AS SELECT * FROM geo_feeds.weather_cloud_layers;
