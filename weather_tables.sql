CREATE TABLE IF NOT EXISTS laddms.weather_conditions (
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

COMMENT ON COLUMN laddms.weather_conditions.temperature IS 'Units: degrees Fahrenheit';
COMMENT ON COLUMN laddms.weather_conditions.feels_like IS 'Units: degrees Fahrenheit';
COMMENT ON COLUMN laddms.weather_conditions.humidity IS 'Units: percent (relative humidity)';
COMMENT ON COLUMN laddms.weather_conditions.humidity IS 'Units: percent';
COMMENT ON COLUMN laddms.weather_conditions.precip_chance IS 'Units: percent';
COMMENT ON COLUMN laddms.weather_conditions.precip_last3hours IS 'Units: inches';

SELECT create_hypertable(
    'laddms.weather_conditions',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);



CREATE TABLE IF NOT EXISTS laddms.weather_radar (
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

COMMENT ON COLUMN laddms.weather_radar.generate_time IS 'Reported radar generation time from NOAA.';
COMMENT ON COLUMN laddms.weather_radar.x_easting IS 'Easting (UTM) coordinates of x-dimension of radar data.';
COMMENT ON COLUMN laddms.weather_radar.y_northing IS 'Northing (UTM) coordinates of y-dimension of radar data.';
COMMENT ON COLUMN laddms.weather_radar.radar_array IS 'RGBA array with dimensions (M, N, 4), where M=rows, N=cols; ordered from image upper left.';

SELECT create_hypertable(
    'laddms.weather_radar',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);




CREATE TABLE IF NOT EXISTS laddms.weather_clouds (
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
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,
    range_miles         REAL,
    utm_zone_epsg       INTEGER
);

COMMENT ON TABLE  laddms.weather_clouds IS 'GOES ABI L2 observed cloud fields, clipped to a lat/lon radius. Every grid column is a (M, N) array on the shared x_easting/y_northing grid, ordered from the image upper left — the same convention as laddms.weather_radar.';
COMMENT ON COLUMN laddms.weather_clouds.generate_time IS 'ABI scan start time. All fields in a row come from one scan.';
COMMENT ON COLUMN laddms.weather_clouds.satellite IS 'Source satellite, e.g. G19 (GOES-19, GOES-East).';
COMMENT ON COLUMN laddms.weather_clouds.x_easting IS 'Easting (UTM) coordinates of the cloud grid.';
COMMENT ON COLUMN laddms.weather_clouds.y_northing IS 'Northing (UTM) coordinates of the cloud grid.';
COMMENT ON COLUMN laddms.weather_clouds.cloud_probability IS 'Probability the pixel is cloudy, 0.0-1.0. Continuous — prefer this over cloud_mask for rendering.';
COMMENT ON COLUMN laddms.weather_clouds.cloud_mask IS 'Four-level cloud mask: 0=clear, 1=probably clear, 2=probably cloudy, 3=cloudy.';
COMMENT ON COLUMN laddms.weather_clouds.cloud_top_height IS 'Units: metres. NULL where the pixel is clear, so this doubles as a cloud extent mask.';
COMMENT ON COLUMN laddms.weather_clouds.cloud_optical_depth IS 'Cloud optical depth at 640 nm, dimensionless. Higher is more opaque; use it to drive render opacity.';
COMMENT ON COLUMN laddms.weather_clouds.cloud_top_phase IS 'Cloud top phase: 0=clear sky, 1=liquid water, 2=supercooled liquid water, 3=mixed phase, 4=ice, 5=unknown.';

SELECT create_hypertable(
    'laddms.weather_clouds',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);



CREATE TABLE IF NOT EXISTS laddms.weather_cloud_layers (
    write_time          TIMESTAMPTZ NOT NULL,
    generate_time       TIMESTAMPTZ,
    valid_time          TIMESTAMPTZ,
    forecast_hour       INTEGER,
    x_easting           JSON,
    y_northing          JSON,
    cloud_cover_low     JSON,
    cloud_cover_mid     JSON,
    cloud_cover_high    JSON,
    cloud_cover_total   JSON,
    cloud_base_height   JSON,
    cloud_top_height    JSON,
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,
    range_miles         REAL,
    utm_zone_epsg       INTEGER
);

COMMENT ON TABLE  laddms.weather_cloud_layers IS 'HRRR layered cloud cover, clipped to a lat/lon radius. One row per forecast hour; grid columns follow the same (M, N) convention as laddms.weather_clouds.';
COMMENT ON COLUMN laddms.weather_cloud_layers.generate_time IS 'HRRR run initialization time. Rows from one poll share it.';
COMMENT ON COLUMN laddms.weather_cloud_layers.valid_time IS 'Time the row describes: generate_time + forecast_hour.';
COMMENT ON COLUMN laddms.weather_cloud_layers.forecast_hour IS 'Hours past the run. 0 is the analysis (the model present); higher values are forecasts.';
COMMENT ON COLUMN laddms.weather_cloud_layers.cloud_cover_low IS 'Units: percent. Low cloud layer fraction.';
COMMENT ON COLUMN laddms.weather_cloud_layers.cloud_cover_mid IS 'Units: percent. Middle cloud layer fraction.';
COMMENT ON COLUMN laddms.weather_cloud_layers.cloud_cover_high IS 'Units: percent. High cloud layer fraction.';
COMMENT ON COLUMN laddms.weather_cloud_layers.cloud_cover_total IS 'Units: percent. Whole-column cloud fraction; always >= each individual layer.';
COMMENT ON COLUMN laddms.weather_cloud_layers.cloud_base_height IS 'Units: geopotential metres, lowest cloud base in the column. NULL where there is no cloud (HRRR writes a 9999 sentinel, which the feed strips).';
COMMENT ON COLUMN laddms.weather_cloud_layers.cloud_top_height IS 'Units: geopotential metres, highest cloud top in the column. NULL where there is no cloud.';

SELECT create_hypertable(
    'laddms.weather_cloud_layers',
    'write_time',
    chunk_time_interval => INTERVAL '1 week'
);
