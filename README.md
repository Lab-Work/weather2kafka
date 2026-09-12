# Weather to Kafka

Four feeds run on their own threads:

| Feed | Topic / table | Source | Cadence |
|---|---|---|---|
| Forecast | `weather_forecast` / `laddms.weather_conditions` | NWS current conditions + hourly forecast | 30 min |
| Radar | `weather_radar` / `laddms.weather_radar` | NOAA MRMS `BREF_QCD`, clipped to a radius | 5 min |
| Clouds | `weather_clouds` / `laddms.weather_clouds` | GOES-19 ABI L2, 2 km, observed | 10 min |
| Cloud layers | `weather_cloud_layers` / `laddms.weather_cloud_layers` | HRRR, 3 km, layered + forecast | 1 hour |

Logs are JSON on stdout and Prometheus metrics are served on `:9100/metrics`.
SIGTERM/SIGINT stop every loop cleanly.

## The cloud feeds

Cloud cover is a different field from precipitation, not a by-product of it. On
a typical overcast day GOES sees cloud over ~60% of the Nashville box while
MRMS paints echoes over ~2% of the same box, so the radar feed cannot stand in
for clouds and neither can the forecast's `short_forecast` text. The two cloud
feeds answer different questions and are meant to be used together:

**`weather_clouds` — what the satellite sees now.** GOES-19 ABI Level-2 CONUS
products, 2 km, a new scan roughly every 10 minutes landing about 3 minutes
behind real time. Each row carries `cloud_probability` (continuous 0-1, the
field to render), `cloud_mask` (four-level categorical), `cloud_top_height`
(metres, NULL where clear), `cloud_optical_depth` (drives opacity) and
`cloud_top_phase` (liquid/ice, drives appearance) on one shared coordinate grid.

The four source products are published a couple of minutes apart, so the feed
takes the newest scan timestamp present in **all** of them rather than each
product's own newest — otherwise a row would mix fields from different instants.

**`weather_cloud_layers` — vertical structure and the near future.** HRRR
`wrfsfc`, 3 km, hourly. Each row is one forecast hour and carries
`cloud_cover_low` / `_mid` / `_high` / `_total` (percent) plus
`cloud_base_height` and `cloud_top_height`. Forecast hour 0 is the analysis —
the model's estimate of the present — and the higher hours let a viewer animate
clouds forward instead of only rendering the current instant. Set the hours with
`WEATHER_CLOUD_LAYER_FORECAST_HOURS`; each one costs a ~10 MB ranged download
per poll, so keep the list short.

Both feeds emit the radar feed's payload shape — 2-D JSON arrays over a
per-pixel UTM `x_easting` / `y_northing` grid, ordered from the upper left — so
a consumer that already draws `weather_radar` can draw these with the same code.

Both NOAA buckets are public: anonymous HTTPS, no AWS credentials to configure.

### Notes for anyone modifying the cloud feeds

Three upstream quirks are load-bearing, and all three fail quietly rather than
loudly if they are undone:

* **GOES `Cloud_Probabilities` declares `valid_range = [0, 1]` in physical
  units while storing packed `uint16`.** netCDF4's auto-masking applies that
  range to the *raw* values, masks 100% of the array, and the field reads back
  as entirely NaN with no error. The feed switches auto-masking off and unpacks
  against `_FillValue` by hand. The `x`/`y` coordinate axes must still be read
  *before* auto-scaling is disabled — they are packed too.
* **HRRR GRIB message numbers shift between forecast hours.** The cloud block
  sits at messages 112-121 in `f00` but 115-124 in `f03`. The feed looks fields
  up in the `.idx` by `(parameter, level)` and then selects bands by their GDAL
  `GRIB_ELEMENT` / `GRIB_SHORT_NAME` tags. Hardcoding message numbers reads the
  wrong fields without erroring.
* **HRRR writes `9999` into cloud base/top height where there is no cloud** and
  sets no GRIB nodata value to declare it. The feed strips it to NULL. Left in,
  it renders as a 9999 m cloud deck over every clear pixel.

Cloud-top height is genuinely NULL wherever a pixel is clear, which is why
`grid_to_json_safe()` exists: `json.dumps()` will otherwise emit a bare `NaN`
token that is not valid JSON and that strict consumers reject.

## Configuration

Copy `example.env` to `.env` and fill in the blanks. Kafka credentials
(`KAFKA_BOOTSTRAP`, `KAFKA_USER`, `KAFKA_PASSWORD`, `KAFKA_CA_LOCATION` or an
inline `KAFKA_CA_CERT`), Postgres credentials (`DB_HOST`, `DB_PORT`, `DB_DBNAME`,
`DB_USER`, `DB_PASSWORD`), and the `WEATHER_*` feed settings all come from the
environment. Every `WEATHER_*` variable is read with no default and immediately
coerced, so omitting one crash-loops the pod rather than degrading a feed. `TELEMETRY_LOG_LEVEL=DEBUG` turns on debug logging.

The pre-1.0 `SQL_*` names and `LOG_PATH` are gone: the deployment env must
supply `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` and now also
`DB_DBNAME` (the database name used to be hardcoded to `NDOT`).

`KAFKA_CA_LOCATION` defaults to `strimzi-ca.crt`, resolved relative to the
container's `/app` working directory — hence the `-v` below mounting the cert
there read-only. Set `KAFKA_CA_LOCATION` to an absolute path if the cert lives
elsewhere (a deployment mounting `/etc/viewlive/certs` would do that), or skip
the mount entirely by supplying `KAFKA_CA_CERT` as an inline PEM string, which
takes precedence.

## Local run

The `lv_*` connectors aren't on PyPI — install them from your sibling checkouts
first, then the rest:

```
pip install -e ../lv_telemetry_connector -e ../lv_kafka_connector -e ../lv_db_connector
pip install -r requirements.txt
python weather2kafka.py
```

## Docker

The build needs a GitHub token to install the private `lv_*` connectors in its
builder stage:

```
docker build --build-arg GITHUB_TOKEN=$GITHUB_TOKEN -t weather2kafka:0.0 .
docker run \
  --env-file path/to/1.env \
  -v $(pwd)/strimzi-ca.crt:/app/strimzi-ca.crt:ro \
  -p 9100:9100 \
  weather2kafka:0.0
```

### CI/CD

Drone (`.drone.yml`) runs on every push to `main`: it builds the image, pushes
`docker.mogi.io/weather2kafka:<commit-sha>`, then bumps the image tag in
`2kafka/weather/deployment.yaml` of `k8s-manifests-backend` to deploy.
