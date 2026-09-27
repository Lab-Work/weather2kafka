-- Schema for weather2kafka. Apply once before running the service:
--
--   psql -h $DB_HOST -U $DB_USER -d $DB_DBNAME -f weather_tables.sql
--
-- Everything lives in geo_feeds, alongside the rest of the 2kafka fleet. These
-- tables were originally created in `laddms` and moved across on 2026-09-27;
-- see weather_schema_move.sql for that one-time migration.
--
-- The CREATE SCHEMA below is a no-op safety net -- geo_feeds is owned by the
-- fleet, not by this service, so this file must never redefine it.

CREATE SCHEMA IF NOT EXISTS geo_feeds;


CREATE TABLE IF NOT EXISTS geo_feeds.weather_conditions (
    write_time          TIMESTAMPTZ NOT NULL,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ,
    generate_time       TIMESTAMPTZ,
    is_daytime          BOOL,
    temperature         REAL,
    feels_like          REAL,
    humidity            REAL,
    short_forecast      TEXT,
    precip_chance       REAL,
    precip_last3hours   REAL
);

COMMENT ON COLUMN geo_feeds.weather_conditions.temperature IS 'Units: degrees Fahrenheit';
COMMENT ON COLUMN geo_feeds.weather_conditions.feels_like IS 'Units: degrees Fahrenheit';
COMMENT ON COLUMN geo_feeds.weather_conditions.humidity IS 'Units: percent (relative humidity)';
COMMENT ON COLUMN geo_feeds.weather_conditions.humidity IS 'Units: percent';
COMMENT ON COLUMN geo_feeds.weather_conditions.precip_chance IS 'Units: percent';
COMMENT ON COLUMN geo_feeds.weather_conditions.precip_last3hours IS 'Units: inches';

SELECT create_hypertable(
    'geo_feeds.weather_conditions',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);



CREATE TABLE IF NOT EXISTS geo_feeds.weather_radar (
    write_time          TIMESTAMPTZ NOT NULL,
    generate_time       TIMESTAMPTZ,
    x_easting           JSON,
    y_northing          JSON,
    radar_array         JSON,
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,
    range_miles         REAL,
    utm_zone_epsg       INTEGER
);

COMMENT ON COLUMN geo_feeds.weather_radar.generate_time IS 'Reported radar generation time from NOAA.';
COMMENT ON COLUMN geo_feeds.weather_radar.x_easting IS 'Easting (UTM) coordinates of x-dimension of radar data.';
COMMENT ON COLUMN geo_feeds.weather_radar.y_northing IS 'Northing (UTM) coordinates of y-dimension of radar data.';
COMMENT ON COLUMN geo_feeds.weather_radar.radar_array IS 'RGBA array with dimensions (M, N, 4), where M=rows, N=cols; ordered from image upper left.';

SELECT create_hypertable(
    'geo_feeds.weather_radar',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);




CREATE TABLE IF NOT EXISTS geo_feeds.weather_clouds (
    write_time          TIMESTAMPTZ NOT NULL,
    generate_time       TIMESTAMPTZ,
    satellite           TEXT,
    x_easting           JSON,
    y_northing          JSON,
    cloud_probability   JSON,
    cloud_mask          JSON,
    cloud_top_height    JSON,
    cloud_optical_depth JSON,
    cloud_top_phase     JSON,
    sky_coverage        REAL,
    cloud_type          TEXT,
    cloud_density       REAL,
    cloud_top_height_m  REAL,
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,
    range_miles         REAL,
    utm_zone_epsg       INTEGER
);

COMMENT ON TABLE  geo_feeds.weather_clouds IS 'GOES ABI L2 observed cloud fields, clipped to a lat/lon radius. Every grid column is a (M, N) array on the shared x_easting/y_northing grid, ordered from the image upper left — the same convention as geo_feeds.weather_radar.';
COMMENT ON COLUMN geo_feeds.weather_clouds.generate_time IS 'ABI scan start time. All fields in a row come from one scan.';
COMMENT ON COLUMN geo_feeds.weather_clouds.satellite IS 'Source satellite, e.g. G19 (GOES-19, GOES-East).';
COMMENT ON COLUMN geo_feeds.weather_clouds.x_easting IS 'Easting (UTM) coordinates of the cloud grid.';
COMMENT ON COLUMN geo_feeds.weather_clouds.y_northing IS 'Northing (UTM) coordinates of the cloud grid.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_probability IS 'Probability the pixel is cloudy, 0.0-1.0. Continuous — prefer this over cloud_mask for rendering.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_mask IS 'Four-level cloud mask: 0=clear, 1=probably clear, 2=probably cloudy, 3=cloudy.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_top_height IS 'Units: metres. NULL where the pixel is clear, so this doubles as a cloud extent mask.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_optical_depth IS 'Cloud optical depth at 640 nm, dimensionless. Higher is more opaque; use it to drive render opacity.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_top_phase IS 'Cloud top phase: 0=clear sky, 1=liquid water, 2=supercooled liquid water, 3=mixed phase, 4=ice, 5=unknown.';
COMMENT ON COLUMN geo_feeds.weather_clouds.sky_coverage IS 'Scalar summary: mean cloud probability over the box, as a percent of area covered.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_type IS 'Scalar summary: DERIVED cloud genus (ISCCP-style, from cloud top height and optical depth) -- not an observed product. One of cumulus, stratocumulus, stratus, altocumulus, altostratus, nimbostratus, cirrus, cirrostratus, deep_convection, or clear.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_density IS 'Scalar summary: mean cloud optical depth over cloudy pixels. Dimensionless; higher is more opaque.';
COMMENT ON COLUMN geo_feeds.weather_clouds.cloud_top_height_m IS 'Scalar summary: median cloud top height over cloudy pixels. Units: metres.';

SELECT create_hypertable(
    'geo_feeds.weather_clouds',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);



CREATE TABLE IF NOT EXISTS geo_feeds.weather_cloud_layers (
    write_time          TIMESTAMPTZ NOT NULL,
    generate_time       TIMESTAMPTZ,
    x_easting           JSON,
    y_northing          JSON,
    cloud_cover_low     JSON,
    cloud_cover_mid     JSON,
    cloud_cover_high    JSON,
    cloud_cover_total   JSON,
    cloud_base_height   JSON,
    cloud_top_height    JSON,
    cloud_base_height_m     REAL,
    cloud_drift_speed       REAL,
    cloud_drift_direction   REAL,
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,
    range_miles         REAL,
    utm_zone_epsg       INTEGER
);

COMMENT ON TABLE  geo_feeds.weather_cloud_layers IS 'HRRR layered cloud cover and steering wind, clipped to a lat/lon radius. Analysis only, no forecast. Grid columns follow the same (M, N) convention as geo_feeds.weather_clouds.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.generate_time IS 'HRRR run initialization time; the analysis this row describes.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_cover_low IS 'Units: percent. Low cloud layer fraction.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_cover_mid IS 'Units: percent. Middle cloud layer fraction.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_cover_high IS 'Units: percent. High cloud layer fraction.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_cover_total IS 'Units: percent. Whole-column cloud fraction; always >= each individual layer.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_base_height IS 'Units: geopotential metres, lowest cloud base in the column. NULL where there is no cloud (HRRR writes a 9999 sentinel, which the feed strips).';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_top_height IS 'Units: geopotential metres, highest cloud top in the column. NULL where there is no cloud.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_base_height_m IS 'Scalar summary: median cloud base over cells that have one. Units: metres.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_drift_speed IS 'Scalar summary: area-mean 700 mb wind speed, the rate a cloud field advects across the box. Units: mph.';
COMMENT ON COLUMN geo_feeds.weather_cloud_layers.cloud_drift_direction IS 'Scalar summary: area-mean 700 mb wind bearing the wind blows FROM. Units: degrees.';

SELECT create_hypertable(
    'geo_feeds.weather_cloud_layers',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);



-- ---------------------------------------------------------------------------
-- Cloud and wind summary columns on the existing conditions table.
--
-- geo_feeds.weather_conditions is already deployed and carrying data, so these go
-- on with ALTER rather than a recreate. Only the current-conditions row fills
-- them; forecast rows leave them NULL, exactly like feels_like and
-- precip_last3hours already do.
--
-- The cloud values are handed over by the cloud feed threads, which poll on
-- their own cadences, so they can lag the weather half of a row by up to one
-- cloud poll. cloud_observed_time and cloud_layer_observed_time are how a
-- consumer tells.
-- ---------------------------------------------------------------------------

ALTER TABLE geo_feeds.weather_conditions
    ADD COLUMN IF NOT EXISTS wind_speed                REAL,
    ADD COLUMN IF NOT EXISTS wind_direction            REAL,
    ADD COLUMN IF NOT EXISTS wind_gust                 REAL,
    ADD COLUMN IF NOT EXISTS sky_coverage              REAL,
    ADD COLUMN IF NOT EXISTS cloud_type                TEXT,
    ADD COLUMN IF NOT EXISTS cloud_density             REAL,
    ADD COLUMN IF NOT EXISTS cloud_base_height         REAL,
    ADD COLUMN IF NOT EXISTS cloud_top_height          REAL,
    ADD COLUMN IF NOT EXISTS cloud_drift_speed         REAL,
    ADD COLUMN IF NOT EXISTS cloud_drift_direction     REAL,
    ADD COLUMN IF NOT EXISTS cloud_observed_time       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cloud_layer_observed_time TIMESTAMPTZ;

COMMENT ON COLUMN geo_feeds.weather_conditions.wind_speed IS 'Units: mph. Surface wind from the NWS station observation (reported in km/h and converted).';
COMMENT ON COLUMN geo_feeds.weather_conditions.wind_direction IS 'Units: degrees, the bearing the wind blows FROM.';
COMMENT ON COLUMN geo_feeds.weather_conditions.wind_gust IS 'Units: mph. NULL when the station reports no gust.';
COMMENT ON COLUMN geo_feeds.weather_conditions.sky_coverage IS 'Units: percent of area under cloud, from the GOES cloud probability field.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_type IS 'DERIVED cloud genus (ISCCP-style, from cloud top height and optical depth) -- not an observed product. See geo_feeds.weather_clouds.cloud_type.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_density IS 'Mean cloud optical depth over cloudy pixels. Dimensionless; higher is more opaque.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_base_height IS 'Units: metres. Median cloud base across the area, from HRRR.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_top_height IS 'Units: metres. Median cloud top across the area, from GOES.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_drift_speed IS 'Units: mph. Area-mean 700 mb wind -- the speed a cloud field actually advects, which surface wind understates.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_drift_direction IS 'Units: degrees, the bearing the 700 mb wind blows FROM.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_observed_time IS 'GOES scan time behind sky_coverage / cloud_type / cloud_density / cloud_top_height.';
COMMENT ON COLUMN geo_feeds.weather_conditions.cloud_layer_observed_time IS 'HRRR analysis time behind cloud_base_height and the drift columns.';
