# Weather to Kafka

Four feeds run on their own threads:

| Feed | Topic / table | Source | Cadence |
|---|---|---|---|
| Forecast | `weather_forecast` / `geo_feeds.weather_conditions` | NWS current conditions + hourly forecast | 30 min |
| Radar | `weather_radar` / `geo_feeds.weather_radar` | NOAA MRMS `BREF_QCD`, clipped to a radius | 5 min |
| Clouds | `weather_clouds` / `geo_feeds.weather_clouds` | GOES-19 ABI L2, 2 km, observed | 10 min |
| Cloud layers | `weather_cloud_layers` / `geo_feeds.weather_cloud_layers` | HRRR, 3 km, layered analysis + steering wind | 1 hour |

Logs are JSON on stdout and Prometheus metrics are served on `:9100/metrics`.
SIGTERM/SIGINT stop every loop cleanly.

All four tables live in the **`geo_feeds`** schema, alongside the rest of the
2kafka fleet. They were created in `laddms` originally and moved on 2026-09-27
— `weather_schema_move.sql` is that one-time migration, and it is safe to
re-run. Anything still querying `laddms.weather_*` needs repointing; the
migration file has commented-out compatibility views if you want a grace
period instead.

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

**`weather_cloud_layers` — vertical structure and cloud drift.** HRRR `wrfsfc`,
3 km, hourly, **analysis only**. Each row carries `cloud_cover_low` / `_mid` /
`_high` / `_total` (percent) plus `cloud_base_height` and `cloud_top_height`
grids. This feed deliberately holds no forecast: cloud cover here is a visual
layer, not something being predicted against, so there is nothing to animate
forward and no reason to pay for the extra downloads.

It also carries the **700 mb cloud-steering wind** as `cloud_drift_speed` and
`cloud_drift_direction`. That is the wind that actually advects a cloud field
across a map — surface wind is slowed and turned by friction and terrain, so it
understates cloud motion badly. This is fetched as a second byte range rather
than widening the first: the wind messages sit far from the cloud block in the
file, and one span covering both costs ~34 MB against ~8.5 MB + ~1.2 MB.

Poll cadence for both cloud feeds is `WEATHER_CLOUD_UPDATE_SECS` and
`WEATHER_CLOUD_LAYER_UPDATE_SECS`. Neither has a code-side default.

Both feeds emit the radar feed's payload shape — 2-D JSON arrays over a
per-pixel UTM `x_easting` / `y_northing` grid, ordered from the upper left — so
a consumer that already draws `weather_radar` can draw these with the same code.

Both NOAA buckets are public: anonymous HTTPS, no AWS credentials to configure.

## The summary row

Each cloud poll also reduces its grid to scalars, and the forecast feed folds
those into the **current-conditions row** it already writes, so one row in
`geo_feeds.weather_conditions` describes the whole sky:

| Column | Units | Source |
|---|---|---|
| `sky_coverage` | percent of area | GOES mean cloud probability |
| `cloud_type` | genus name | **derived**, see below |
| `cloud_density` | optical depth | GOES, mean over cloudy pixels |
| `cloud_top_height` | metres | GOES, median over cloudy pixels |
| `cloud_base_height` | metres | HRRR, median over cells with a base |
| `cloud_drift_speed` / `_direction` | mph / degrees FROM | HRRR 700 mb area mean |
| `wind_speed` / `_direction` / `_gust` | mph / degrees FROM | NWS station observation |

Wind comes free: the current-conditions observation the forecast feed already
fetches carries `windSpeed`, `windDirection` and `windGust`, so there is no
extra request. It is reported in km/h and converted, since the rest of the table
is imperial. `windDirection` is legitimately null when the station reports a
variable direction (METAR `VRB`), so the column is nullable by design.

Only the current row fills these; forecast rows leave them NULL, exactly as
`feels_like` and `precip_last3hours` already do.

**Staleness.** The feeds are separate threads on separate cadences, so the cloud
half of a summary row can lag the weather half by up to one cloud poll.
`cloud_observed_time` and `cloud_layer_observed_time` carry the GOES scan time
and HRRR analysis time so a consumer can tell. Right after a restart they are
NULL until the first cloud poll lands, which is normal and not an error.

### `cloud_type` is derived, not observed

There is no NOAA cloud-type product. GOES publishes cloud *phase* (liquid/ice)
but not genus, so `cloud_type` is computed here using the ISCCP scheme, which
sorts cloud on two axes — how high the top is, and how optically thick it is:

| | thin (τ<3.6) | medium (τ 3.6–23) | thick (τ>23) |
|---|---|---|---|
| **high** (top >6 km) | cirrus | cirrostratus | deep_convection |
| **middle** (2–6 km) | altocumulus | altostratus | nimbostratus |
| **low** (top <2 km) | cumulus | stratocumulus | stratus |

Below 10% coverage it reports `clear` and no genus. ISCCP defines the height
axis on cloud-top pressure; this uses height against the WMO mid-latitude etage
boundaries instead, because the feed already carries height in metres.

**Read it as a one-word summary of a 60-mile box, not as an observation of a
cloud.** It is computed from *median* top height and *mean* optical depth, so a
genuinely mixed sky — scattered cumulus under a high deck — collapses to a
single label that may match neither layer. The grids in `weather_clouds` and
`weather_cloud_layers` carry the real structure; this column is for when a
caller wants one word.

### Notes for anyone modifying the cloud feeds

Three upstream quirks are load-bearing, and all three fail quietly rather than
loudly if they are undone:

* **GOES `Cloud_Probabilities` declares `valid_range = [0, 1]` in physical
  units while storing packed `uint16`.** netCDF4's auto-masking applies that
  range to the *raw* values, masks 100% of the array, and the field reads back
  as entirely NaN with no error. The feed switches auto-masking off and unpacks
  against `_FillValue` by hand. The `x`/`y` coordinate axes must still be read
  *before* auto-scaling is disabled — they are packed too.
* **HRRR GRIB message numbers are not stable.** The cloud block sits at
  messages 112-121 in one file and 115-124 in another. The feed looks fields up
  in the `.idx` by `(parameter, level)` and then selects bands by their GDAL
  `GRIB_ELEMENT` / `GRIB_SHORT_NAME` tags. Hardcoding message numbers reads the
  wrong fields without erroring. Note the short name for a pressure level is in
  pascals, not millibars: 700 mb is `70000-ISBL`.
* **The scalar summaries must not reuse a grid column's name.** `cloud_top_height`
  and `cloud_base_height` are grids on the cloud rows, so the scalars go in as
  `cloud_top_height_m` and `cloud_base_height_m` there. On
  `weather_conditions`, where there are no grids, they keep the bare names.
* **Every row of a bulk insert must carry identical keys.** `lv_db_connector`
  takes its column set from the first row and raises on any later row that
  lacks a key. The current-conditions row is richer than the forecast rows
  behind it, and its cloud keys are absent entirely until a cloud poll lands, so
  `insert_weather_batch()` pads every row against `WEATHER_SUMMARY_COLUMNS`.
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
