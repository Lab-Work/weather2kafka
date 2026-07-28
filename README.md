# Weather to Kafka

Two feeds run on their own threads: NWS current conditions + hourly forecast
(`weather_forecast` topic, `laddms.weather_conditions` table) and NOAA MRMS
radar clipped to a lat/lon radius (`weather_radar` topic, `laddms.weather_radar`
table). Logs are JSON on stdout and Prometheus metrics are served on
`:9100/metrics`. SIGTERM/SIGINT stop both loops cleanly.

## Configuration

Copy `example.env` to `.env` and fill in the blanks. Kafka credentials
(`KAFKA_BOOTSTRAP`, `KAFKA_USER`, `KAFKA_PASSWORD`, `KAFKA_CA_LOCATION` or an
inline `KAFKA_CA_CERT`), Postgres credentials (`DB_HOST`, `DB_PORT`, `DB_DBNAME`,
`DB_USER`, `DB_PASSWORD`), and the `WEATHER_*` feed settings all come from the
environment. `TELEMETRY_LOG_LEVEL=DEBUG` turns on debug logging.

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
