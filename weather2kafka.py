"""weather2kafka — NWS forecast/current conditions and NOAA MRMS radar to Kafka + Postgres.

Two independent feeds run on their own threads with their own cadences:
    * weather_forecast — api.weather.gov points/stations/hourly forecast
    * weather_radar    — MRMS BREF_QCD GeoTIFF, clipped to a lat/lon radius

    Run:        python weather2kafka.py
    Logs:       JSON on stdout (Loki-friendly)
    Metrics:    /metrics endpoint on :9100 (Prometheus)
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import shutil
import signal
import sys
import threading
import time
import traceback
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import rasterio
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyproj import CRS, Transformer
from rasterio.transform import xy

from lv_db_connector import Connector, DbEnvCredentials
from lv_kafka_connector import KafkaEnvCredentials, KafkaProducer
from lv_telemetry_connector import configure_telemetry

load_dotenv()

SERVICE = os.getenv("SERVICE_NAME", "weather2kafka")

nashville_tz = ZoneInfo('US/Central')

# Telemetry handles. `main()` fills these in once via _bind_telemetry(); they are
# module-level so the thread targets below can log and record metrics without
# threading `tel` through every signature.
logger: Any = None
_fetched_total: Any = None
_emitted_total: Any = None
_fetch_seconds: Any = None


def _bind_telemetry(tel) -> None:
    """Bind the module-level logger and metric handles from a configured Telemetry."""
    global logger, _fetched_total, _emitted_total, _fetch_seconds
    logger = tel.get_logger("weather2kafka")
    _fetched_total = tel.counter(
        "events_fetched_total",
        "Forecast periods and radar payloads fetched from the upstream APIs.",
    )
    _emitted_total = tel.counter(
        "events_emitted_total",
        "Payloads produced to Kafka.",
    )
    _fetch_seconds = tel.histogram(
        "fetch_seconds",
        "Wall-clock time of an upstream fetch (forecast pull or radar download).",
    )


def now_dtz():
    return dt.datetime.now(tz=nashville_tz)


# Helper function to wrap thread targets for fatal error handling
def thread_wrapper(target_func, args=(), name=""):
    def wrapped():
        try:
            target_func(*args)
        except Exception:
            logger.critical(f"Unhandled exception in thread '{name}', exiting entire process.", exc_info=True)
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
    return wrapped


# =============================================================================
# Lifecycle — graceful shutdown.
# =============================================================================

_shutdown = False


def _on_signal(_signum, _frame) -> None:
    """SIGTERM / SIGINT handler. Flip the flag; both feed loops notice."""
    global _shutdown
    _shutdown = True


def _sleep_responsively(seconds: float) -> None:
    """Sleep in small chunks so SIGTERM is responsive.

    Never sleep a whole poll interval in one call — k8s SIGTERMs and waits
    `terminationGracePeriodSeconds` (default 30 s) before SIGKILL, and the radar
    cadence here is minutes.
    """
    deadline = time.monotonic() + seconds
    while not _shutdown:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(0.5, remaining))


# =============================================================================
# Database connector — all weather SQL lives here.
# =============================================================================

class WeatherDb(Connector):
    """Postgres connector with one insert method per weather table."""

    def insert_weather_conditions(self, rows: list[dict]) -> None:
        self.insert("laddms.weather_conditions", rows)

    def insert_weather_radar(self, rows: list[dict]) -> None:
        self.insert("laddms.weather_radar", rows)


class WeatherForecastProducer:
    def __init__(self, url, poll_interval_minutes, kafka: KafkaProducer, db: WeatherDb):
        self.url = url
        # NOTE: the caller passes WEATHER_FORECAST_UPDATE_SECS in here, so the
        # effective forecast cadence is that value in *minutes*. Preserved as-is
        # — the deployed interval depends on it.
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kafka = kafka
        self.db = db

        self.topic_name = "weather_forecast"
        self.partition_key = "0"


    def insert_weather_batch(self, current_dict: dict, forecast_dicts: list[dict], write_time: dt.datetime):
        """
        Insert the current observation and forecast periods into laddms.weather_conditions
        using a single write_time.
        """
        self.db.insert_weather_conditions(
            [{'write_time': write_time, **d} for d in [current_dict] + forecast_dicts]
        )
        logger.info(f"Inserted {len(forecast_dicts) + 1} rows into laddms.weather_conditions.")


    def wait(self):
        _sleep_responsively(self.poll_interval_seconds)


    def pull_weather_forecast(self, latitude, longitude, num_forecast_hours):
        # Step 1: Get metadata from /points
        point_resp = requests.get(f"{self.url}/points/{latitude},{longitude}").json()
        stations_url = point_resp['properties']['observationStations']
        forecast_hourly_url = point_resp['properties']['forecastHourly']

        # Step 2: Get observation station and latest observation
        stations = requests.get(stations_url).json()
        station_id = stations['observationStations'][0].split('/')[-1]
        obs = requests.get(f"{self.url}/stations/{station_id}/observations/latest").json()['properties']

        # Extract current weather data
        if obs.get('temperature', {}).get('unitCode', '').upper() == 'WMOUNIT:DEGC':
            # convert to degF
            temperature = (float(obs['temperature']['value'])  * 9 / 5) + 32
        else:
            temperature = None
        humidity = obs['relativeHumidity']['value']
        # If can't find the value, use None
        if obs.get('precipitationLast3Hours', {}).get('value', -1) == -1:
            precip_last = None
        # If value is present but None, assume 0.
        elif obs.get('precipitationLast3Hours', {}).get('value') is None:
            precip_last = 0
        elif obs.get('precipitationLast3Hours', {}).get('value', '').upper() == 'NONE':
            precip_last = 0
        elif len(obs.get('precipitationLast3Hours', {}).get('value', '')) > 0:
            if obs.get('precipitationLast3Hours', {}).get('unitCode', '').upper() == 'WMOUNIT:MM':
                # convert to inches
                precip_last = float(obs.get('precipitationLast3Hours', {}).get('value')) / 25.4
            else:
                precip_last = None
        else:
            precip_last = None
        if obs.get('heatIndex', {}).get('value', None) is not None:
            if obs.get('heatIndex', {}).get('unitCode', '').upper() == 'WMOUNIT:DEGC':
                # convert to degF
                feels_like = (float(obs.get('heatIndex').get('value')) * 9 / 5) + 32
            else:
                feels_like = None
        elif obs.get('windChill', {}).get('value', None) is not None:
            if obs.get('windChill', {}).get('unitCode', '').upper() == 'WMOUNIT:DEGC':
                # convert to degF
                feels_like = (float(obs.get('windChill', {}).get('value')) * 9 / 5) + 32
            else:
                feels_like = None
        else:
            feels_like = None

        # Output Current Conditions
        current_dict = {
            'start_time': obs['timestamp'],
            'end_time': None,
            'generate_time': obs['timestamp'],
            'is_daytime': None,
            'temperature': temperature,
            'feels_like': feels_like,
            'humidity': humidity,
            'short_forecast': obs.get('textDescription', None),
            'precip_chance': None,
            'precip_last3hours': precip_last,
        }

        # Current UTC time (aware, not naive)
        utc_now = datetime.now(tz=ZoneInfo("UTC"))
        central_now = utc_now.astimezone(ZoneInfo("US/Central"))

        forecast = requests.get(forecast_hourly_url).json()
        forecast_periods = forecast['properties']['periods']

        forecast_dicts = []
        for period in forecast_periods:
            start_time = dt.datetime.fromisoformat(period['startTime'])
            if start_time < central_now:
                continue
            if period['temperatureUnit'].upper() == 'F':
                temp = float(period['temperature'])
            elif period['temperatureUnit'].upper() == 'C':
                temp = (float(period['temperature']) * 9 / 5) + 32
            else:
                temp = None

            try:
                humidity = float(period['relativeHumidity']['value'])
            except (ValueError, KeyError):
                humidity = None
            try:
                precip_chance = float(period['probabilityOfPrecipitation']['value'])
            except (ValueError, KeyError, TypeError):
                precip_chance = None

            forecast_dict = {
                'start_time': period['startTime'],
                'end_time': period['endTime'],
                'generate_time': forecast['properties']['generatedAt'],
                'is_daytime': period.get('isDaytime', None),
                'temperature': temp,
                'feels_like': None,
                'humidity': humidity,
                'short_forecast': period.get('shortForecast', None),
                'precip_chance': precip_chance,
                'precip_last3hours': None,
            }
            forecast_dicts.append(forecast_dict)
            if len(forecast_dicts) >= num_forecast_hours:
                break

        return current_dict, forecast_dicts


    def produce_current_and_forecast_to_kafka(self, current_dict: dict, forecast_dicts: list[dict]):
        # Produce to Kafka. The value is a JSON-encoded *string* (json.dumps of
        # the dict, then serialized again by the connector) — that double
        # encoding is what downstream consumers of this topic already parse, so
        # don't "fix" it by passing the dict directly.
        self.kafka.produce(self.topic_name, value=json.dumps(current_dict), key=self.partition_key,
                           headers={'service': b'weather', 'datatype': b'current'})
        _emitted_total.inc()
        for fd in forecast_dicts:
            self.kafka.produce(self.topic_name, value=json.dumps(fd), key=self.partition_key,
                               headers={'service': b'weather', 'datatype': b'forecast'})
            _emitted_total.inc()
        self.kafka.flush()
        logger.info(f"Produced {len(forecast_dicts) + 1} weather data points to Kafka.")

        # Now write to the database
        # Use a single write_time for all rows in this batch
        write_time = now_dtz()
        try:
            self.insert_weather_batch(current_dict=current_dict, forecast_dicts=forecast_dicts, write_time=write_time)
        except Exception as e:
            logger.error("Failed to insert weather data into the database.")
            logger.exception(e, exc_info=True)


class WeatherRadarProducer:
    def __init__(self, url, lat_lon_range_list, poll_interval_seconds, kafka: KafkaProducer, db: WeatherDb):
        self.url = url
        self.poll_interval_seconds = poll_interval_seconds
        self.kafka = kafka
        self.db = db
        self.location_list = lat_lon_range_list

        self.topic_name = "weather_radar"
        self.partition_key = "0"


    def insert_weather_radar(self, radar_dicts: list[dict]):
        """
        Insert the clipped radar payloads into laddms.weather_radar using a single write_time.
        """
        write_time = now_dtz()
        for radar_dict in radar_dicts:
            radar_dict['x_easting'] = json.dumps(radar_dict['x_easting'])
            radar_dict['y_northing'] = json.dumps(radar_dict['y_northing'])
            radar_dict['radar_array'] = json.dumps(radar_dict['radar_array'])
        self.db.insert_weather_radar([{'write_time': write_time, **d} for d in radar_dicts])
        logger.info(f"Inserted {len(radar_dicts)} rows into laddms.weather_radar.")


    def wait(self):
        _sleep_responsively(self.poll_interval_seconds)


    def pull_weather_radar(self, plot_radar: bool = False):
        # Step 1: Download GeoTIFF .gz
        # Fetch directory listing page
        response = requests.get(self.url)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch directory listing: {self.url}")

        # Parse the page to extract available filenames
        soup = BeautifulSoup(response.text, 'html.parser')
        files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.tif.gz')]
        if not files:
            raise RuntimeError("No radar files found in directory listing.")

        # Get the latest file based on timestamp
        latest_file = sorted(files)[-1]
        radar_url = self.url + latest_file

        try:
            dt_comp = latest_file.strip('.tif.gz').split('_')[4:6]
            if len(dt_comp) != 2:
                raise ValueError(f"Not enough _-separated components in file name: {latest_file}")
            dt_file = dt.datetime.strptime(f'{dt_comp[0]} {dt_comp[1]}', '%Y%m%d %H%M%S').replace(tzinfo=dt.timezone.utc)
        except ValueError:
            logger.warning(f"Coundn't parse timestamp for file {latest_file}", exc_info=True)
            dt_file = None

        logger.info(f"Fetching latest Radar File: {radar_url}")
        response = requests.get(radar_url)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to download radar file: {radar_url}")

        with open("radar.tif.gz", "wb") as f:
            f.write(response.content)

        # Step 2: Unzip to GeoTIFF
        with gzip.open("radar.tif.gz", 'rb') as f_in:
            with open("radar.tif", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Step 3: Clip to 100 miles radius around Nashville
        with rasterio.open("radar.tif") as src:
            # Read RGB bands and Alpha band
            r = src.read(1)
            g = src.read(2)
            b = src.read(3)
            alpha = src.read(4)

            # Stack into RGBA image array
            rgba = np.dstack((r, g, b, alpha))

            # Generate per-pixel coordinate arrays
            rows, cols = np.meshgrid(np.arange(src.height), np.arange(src.width), indexing='ij')
            lon_flat, lat_flat = xy(src.transform, rows.flatten(), cols.flatten(), offset='center')
            lon = np.array(lon_flat).reshape(rows.shape)
            lat = np.array(lat_flat).reshape(rows.shape)

        radar_dicts = []
        for i, (center_lat, center_lon, range_miles) in enumerate(self.location_list):
            # Define a UTM CRS based on the center point
            # UTM zones are 1-60 covering longitudes from -180 to +180 in 6° steps
            zone = int((center_lon + 180) // 6) + 1
            if zone < 1:
                zone = 1
            elif zone > 60:
                zone = 60

            # Northern hemisphere uses EPSG:326xx; southern hemisphere uses EPSG:327xx
            epsg_base = 326 if center_lat >= 0 else 327
            utm_epsg = epsg_base * 100 + zone  # 32600+zone or 32700+zone
            utm_crs = CRS.from_epsg(utm_epsg)

            transformer_to_utm = Transformer.from_crs(src.crs, utm_crs, always_xy=True)

            # Define bounding box in Lat/Lon around Nashville (approx 100 miles buffer)
            buffer_m = 1609.34 * range_miles  # miles to meters
            buffer_deg = buffer_m / 111000  # Approx degrees per km
            min_lon_box = center_lon - buffer_deg
            max_lon_box = center_lon + buffer_deg
            min_lat_box = center_lat - buffer_deg
            max_lat_box = center_lat + buffer_deg

            # Find indices that fall within bounding box
            lat_mask = (lat >= min_lat_box) & (lat <= max_lat_box)
            lon_mask = (lon >= min_lon_box) & (lon <= max_lon_box)
            combined_mask = lat_mask & lon_mask

            # Get bounding indices for slicing
            valid_rows, valid_cols = np.where(combined_mask)
            row_min, row_max = valid_rows.min(), valid_rows.max()
            col_min, col_max = valid_cols.min(), valid_cols.max()

            # Slice the data arrays to Tennessee area
            rgba_slice = rgba[row_min:row_max + 1, col_min:col_max + 1, :]
            lon_slice = lon[row_min:row_max + 1, col_min:col_max + 1]
            lat_slice = lat[row_min:row_max + 1, col_min:col_max + 1]

            # Convert sliced coordinates to UTM
            utm_x_slice, utm_y_slice = transformer_to_utm.transform(lon_slice, lat_slice)

            if plot_radar is True:
                plt.imshow(rgba_slice, extent=(utm_x_slice.min(), utm_x_slice.max(), utm_y_slice.min(), utm_y_slice.max()),
                           origin='upper')
                plt.title(f"UTM Zone {zone}{'N' if center_lat >= 0 else 'S'}")
                plt.xlabel("Easting (m)")
                plt.ylabel("Northing (m)")
                plt.tight_layout()
                plt.savefig(f"radar_latest_loc{i}.png")

            radar_dict = {
                'generate_time': dt_file.isoformat(),
                'x_easting': utm_x_slice.tolist(),
                'y_northing': utm_y_slice.tolist(),
                'radar_array': rgba_slice.tolist(),
                'center_lat': center_lat,
                'center_lon': center_lon,
                'range_miles': range_miles,
                'utm_zone_epsg': utm_epsg,
            }
            radar_dicts.append(radar_dict)

        return radar_dicts


    def produce_radar_to_kafka(self, radar_dicts):
        # Same double-encoded value shape as the forecast topic — see the note in
        # WeatherForecastProducer.produce_current_and_forecast_to_kafka().
        for radar_dict in radar_dicts:
            self.kafka.produce(self.topic_name, value=json.dumps(radar_dict), key=self.partition_key,
                               headers={'service': b'weather', 'datatype': b'radar'})
            _emitted_total.inc()
        self.kafka.flush()
        logger.info(f"Produced {len(radar_dicts)} weather radar payloads to Kafka.")


def update_weather_forecast(url, poll_interval, num_forecast_hours, locations: list[tuple],
                            kafka: KafkaProducer, db: WeatherDb):
    forecast_receiver = WeatherForecastProducer(url, poll_interval, kafka=kafka, db=db)
    logger.info("Created new instance of weather forecast receiver.")
    while not _shutdown:
        for location in locations:
            lat, lon = location
            # 1) get the latest forecast
            try:
                with _fetch_seconds.time():
                    current_dict, forecast_dicts = forecast_receiver.pull_weather_forecast(
                        latitude=lat, longitude=lon, num_forecast_hours=num_forecast_hours)
            except Exception as e:
                logger.error("Failed to pull updated weather forecast.")
                logger.exception(e, exc_info=True)
                forecast_receiver.wait()
                continue
            _fetched_total.inc(len(forecast_dicts) + 1)
            # 2) produce forecast to Kafka
            try:
                forecast_receiver.produce_current_and_forecast_to_kafka(current_dict=current_dict,
                                                                        forecast_dicts=forecast_dicts)
            except Exception as e:
                logger.error("Failed to assemble and send weather forecast to Kafka.")
                logger.exception(e, exc_info=True)
            # 3) invoke WAIT on the receiver object
        forecast_receiver.wait()


def update_weather_radar(url, lat_lon_range_location_list, poll_interval, plot_radar,
                         kafka: KafkaProducer, db: WeatherDb):
    radar_receiver = WeatherRadarProducer(url, lat_lon_range_location_list, poll_interval,
                                          kafka=kafka, db=db)
    logger.info("Created new instance of weather radar receiver.")
    while not _shutdown:
        # 1) get the latest radar data
        try:
            with _fetch_seconds.time():
                rcv_data = radar_receiver.pull_weather_radar(plot_radar=plot_radar)
        except Exception as e:
            logger.error("Failed to pull updated weather radar data.")
            logger.exception(e, exc_info=True)
            radar_receiver.wait()
            continue
        _fetched_total.inc(len(rcv_data))
        # 2) produce radar data to Kafka
        try:
            radar_receiver.produce_radar_to_kafka(radar_dicts=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send radar data to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) insert to database
        try:
            radar_receiver.insert_weather_radar(radar_dicts=rcv_data)
        except Exception as e:
            logger.error("Failed to insert weather radar data.")
            logger.exception(e, exc_info=True)
        # 4) invoke WAIT on the receiver object
        radar_receiver.wait()


def main() -> None:
    global _shutdown

    # One call wires up JSON logging on stdout and the Prometheus /metrics
    # endpoint on :9100. TELEMETRY_LOG_LEVEL=DEBUG replaces the old debug flag.
    tel = configure_telemetry(service=SERVICE)
    _bind_telemetry(tel)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    logger.info("Starting 2x weather to Kafka producer threads.")

    # One producer and one connector shared by both feed threads (the confluent
    # producer and the connection pool are both thread-safe).
    with (
        KafkaProducer(KafkaEnvCredentials()) as kafka,
        WeatherDb(DbEnvCredentials(), persistent=True) as db,
    ):
        locations = [
            (float(os.environ.get('WEATHER_FORECAST_LAT')), float(os.environ.get('WEATHER_FORECAST_LON')))
        ]
        location_tuples = [
            (
                float(os.environ.get('WEATHER_RADAR_LAT')),
                float(os.environ.get('WEATHER_RADAR_LON')),
                float(os.environ.get('WEATHER_RADAR_RANGE_MI'))
            ),
        ]
        threads = [
            threading.Thread(target=thread_wrapper(update_weather_forecast, args=(
                os.environ.get('WEATHER_FORECAST_URL'),
                int(os.environ.get('WEATHER_FORECAST_UPDATE_SECS')),
                int(os.environ.get('WEATHER_NUM_FORECAST_HOURS')),
                locations,
                kafka,
                db), name="weather_forecast"), name="weather_forecast"),
            threading.Thread(target=thread_wrapper(update_weather_radar, args=(
                os.environ.get('WEATHER_RADAR_URL'),
                location_tuples,
                int(os.environ.get('WEATHER_RADAR_UPDATE_SECS')),
                bool(int(os.environ.get('WEATHER_RADAR_PLOT'))),
                kafka,
                db), name="weather_radar"), name="weather_radar"),
        ]
        for thread in threads:
            thread.start()

        # Stay in the main thread so the signal handlers above can run; the feed
        # loops check _shutdown between polls.
        while not _shutdown and any(thread.is_alive() for thread in threads):
            time.sleep(0.5)
        _shutdown = True
        for thread in threads:
            thread.join(timeout=30)

    logger.info("shutdown")


if __name__ == "__main__":
    main()
