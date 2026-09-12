"""weather2kafka — NWS forecast, NOAA MRMS radar and satellite/model clouds to Kafka + Postgres.

Four independent feeds run on their own threads with their own cadences:
    * weather_forecast     — api.weather.gov points/stations/hourly forecast
    * weather_radar        — MRMS BREF_QCD GeoTIFF, clipped to a lat/lon radius
    * weather_clouds       — GOES-19 ABI L2 observed cloud fields, 2 km
    * weather_cloud_layers — HRRR low/middle/high cloud fraction, 3 km + forecast

    Run:        python weather2kafka.py
    Logs:       JSON on stdout (Loki-friendly)
    Metrics:    /metrics endpoint on :9100 (Prometheus)
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import re
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
import netCDF4
import numpy as np
import rasterio
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyproj import CRS, Transformer
from rasterio.io import MemoryFile
from rasterio.transform import rowcol, xy

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

    def insert_weather_clouds(self, rows: list[dict]) -> None:
        self.insert("laddms.weather_clouds", rows)

    def insert_weather_cloud_layers(self, rows: list[dict]) -> None:
        self.insert("laddms.weather_cloud_layers", rows)


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


# =============================================================================
# Cloud feeds — GOES-19 ABI (observed) and HRRR (layered + forecast).
#
# Clouds are a genuinely different field from precipitation, not a by-product of
# it: on a typical overcast day 60% of this box is cloud-covered while radar
# paints echoes over ~2% of it. Neither the radar feed nor the forecast's
# `short_forecast` text answers "where are the clouds", so these two feeds do.
#
#   weather_clouds        GOES-19 ABI L2 — what the satellite sees now. 2 km,
#                         a new CONUS scan every ~10 min, ~3 min behind real
#                         time. Cloud probability, 4-level mask, top height,
#                         optical depth and top phase.
#   weather_cloud_layers  HRRR — low/middle/high cloud fraction plus cloud base
#                         and top height. 3 km, hourly, and because HRRR
#                         publishes forecast hours a viewer can animate forward
#                         rather than only render the present.
#
# Both emit the radar feed's payload shape: 2-D JSON arrays over a per-pixel UTM
# coordinate grid, so a consumer that already draws `weather_radar` draws these.
# =============================================================================

# The NOAA open-data buckets are public — anonymous HTTPS, no AWS credentials.
GOES_BUCKET_URL = "https://noaa-goes19.s3.amazonaws.com"
HRRR_BUCKET_URL = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"

# (S3 product prefix, [(netCDF variable, output column), ...]).
#
# Every product here is a CONUS ("...C") sector file on the same 2 km ABI fixed
# grid — identical x/y axes, and the scans are published in lockstep — so one
# coordinate array pair describes all of them and they can share a row.
#
# ACHA2KMC, not ACHAC: the plain ACHAC cloud-top-height product is 10 km, which
# is four pixels across a 30-mile box and useless here.
GOES_CLOUD_PRODUCTS = [
    ("ABI-L2-ACMC", [("Cloud_Probabilities", "cloud_probability"), ("ACM", "cloud_mask")]),
    ("ABI-L2-ACHA2KMC", [("HT", "cloud_top_height")]),
    ("ABI-L2-CODC", [("COD", "cloud_optical_depth")]),
    ("ABI-L2-ACTPC", [("Phase", "cloud_top_phase")]),
]

# GRIB band selectors, matched against GDAL's GRIB_ELEMENT / GRIB_SHORT_NAME
# tags after the block is opened. The matching .idx rows are (parameter, level)
# — the index is searched by name because HRRR message *numbers* shift between
# forecast hours (the cloud block sits at 112-121 in f00 but 115-124 in f03),
# so anything that hardcodes numbers silently reads the wrong fields.
HRRR_CLOUD_FIELDS = [
    ("LCDC", "low cloud layer", "0-LCY", "cloud_cover_low"),
    ("MCDC", "middle cloud layer", "0-MCY", "cloud_cover_mid"),
    ("HCDC", "high cloud layer", "0-HCY", "cloud_cover_high"),
    ("TCDC", "entire atmosphere", "0-EATM", "cloud_cover_total"),
    ("HGT", "cloud base", "0-CBL", "cloud_base_height"),
    ("HGT", "cloud top", "0-CTL", "cloud_top_height"),
]

# HRRR writes 9999 into the cloud base/top height fields where there is no
# cloud, and sets no GRIB nodata value to say so. It is unambiguous: 9999.0
# appears exactly, in 44-75% of cells, with a clean gap below it, while real
# cloud tops run well past it (19 km) as non-integral floats.
HRRR_NO_CLOUD_HEIGHT = 9999.0

# How far back to look for a usable upstream file before giving up.
GOES_SCAN_SEARCH_HOURS = 2
HRRR_RUN_SEARCH_HOURS = 6


def utm_crs_for_center(center_lat, center_lon):
    """UTM CRS and EPSG code for a center point — the radar feed's zone maths."""
    zone = int((center_lon + 180) // 6) + 1
    zone = min(60, max(1, zone))
    utm_epsg = (326 if center_lat >= 0 else 327) * 100 + zone
    return CRS.from_epsg(utm_epsg), utm_epsg


def grid_to_json_safe(array, decimals=2):
    """Round a float grid and swap every non-finite cell for null.

    json.dumps() happily writes a bare `NaN` token, which is not valid JSON and
    breaks any strict parser downstream. Cloud grids are full of gaps — clear
    pixels have no cloud-top height — so this matters on every message.
    """
    rounded = np.round(np.asarray(array, dtype='float64'), decimals)
    return np.where(np.isfinite(rounded), rounded, None).tolist()


class WeatherCloudProducer:
    """GOES-19 ABI L2 cloud fields, clipped to a lat/lon radius.

    One row per location per scan holding every field in GOES_CLOUD_PRODUCTS on
    a shared UTM coordinate grid.
    """

    def __init__(self, bucket_url, lat_lon_range_list, poll_interval_seconds, kafka: KafkaProducer, db: WeatherDb):
        self.bucket_url = bucket_url
        self.poll_interval_seconds = poll_interval_seconds
        self.kafka = kafka
        self.db = db
        self.location_list = lat_lon_range_list

        self.topic_name = "weather_clouds"
        self.partition_key = "0"


    def insert_weather_clouds(self, cloud_dicts: list[dict]):
        """Insert the clipped cloud payloads into laddms.weather_clouds."""
        write_time = now_dtz()
        rows = []
        for cloud_dict in cloud_dicts:
            row = dict(cloud_dict)
            for column, value in row.items():
                if isinstance(value, list):
                    row[column] = json.dumps(value)
            rows.append({'write_time': write_time, **row})
        self.db.insert_weather_clouds(rows)
        logger.info(f"Inserted {len(rows)} rows into laddms.weather_clouds.")


    def wait(self):
        _sleep_responsively(self.poll_interval_seconds)


    def list_product_scans(self, product):
        """Map scan-start token -> S3 key for `product` over the recent hours.

        GOES keys are laid out <product>/<year>/<day-of-year>/<hour>/, so an
        hour boundary needs both hours listed to see the newest scan.
        """
        scans = {}
        for hours_back in range(GOES_SCAN_SEARCH_HOURS):
            moment = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=hours_back)
            prefix = f"{product}/{moment.year}/{moment.timetuple().tm_yday:03d}/{moment:%H}/"
            response = requests.get(
                self.bucket_url,
                params={'list-type': '2', 'prefix': prefix, 'max-keys': '400'},
                timeout=60,
            )
            if response.status_code != 200:
                raise RuntimeError(f"Failed to list GOES product {product}: HTTP {response.status_code}")
            for key in re.findall(r'<Key>([^<]+)</Key>', response.text):
                token = re.search(r'_s(\d{14})_', key)
                if token is not None:
                    scans[token.group(1)] = key
        return scans


    def read_product_grid(self, key, variables):
        """Download one GOES file and return (columns, x_utm, y_utm, scan_start).

        The ABI fixed grid stores x/y as *scan angles*; multiplying by the
        perspective point height converts them to metres in the geostationary
        projection, which is then reprojected straight to UTM.
        """
        response = requests.get(f"{self.bucket_url}/{key}", timeout=180)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to download GOES file {key}: HTTP {response.status_code}")

        dataset = netCDF4.Dataset('inmemory.nc', memory=response.content)
        try:
            projection = dataset.variables['goes_imager_projection']
            satellite_height = float(projection.perspective_point_height)
            geos_crs = CRS.from_proj4(
                f"+proj=geos +h={satellite_height} "
                f"+lon_0={float(projection.longitude_of_projection_origin)} "
                f"+sweep={projection.sweep_angle_axis} "
                f"+a={float(projection.semi_major_axis)} +b={float(projection.semi_minor_axis)}"
            )
            # Read the coordinate axes BEFORE auto-scaling is switched off below:
            # x/y are packed int16 and are meaningless without their scale_factor.
            grid_x = np.asarray(dataset.variables['x'][:], dtype='float64') * satellite_height
            grid_y = np.asarray(dataset.variables['y'][:], dtype='float64') * satellite_height
            scan_start = dataset.time_coverage_start

            columns = {}
            windows = {}
            for center_lat, center_lon, range_miles in self.location_list:
                to_geos = Transformer.from_crs("EPSG:4326", geos_crs, always_xy=True)
                center_x, center_y = to_geos.transform(center_lon, center_lat)
                buffer_m = 1609.34 * range_miles
                x_indices = np.where((grid_x >= center_x - buffer_m) & (grid_x <= center_x + buffer_m))[0]
                y_indices = np.where((grid_y >= center_y - buffer_m) & (grid_y <= center_y + buffer_m))[0]
                if x_indices.size == 0 or y_indices.size == 0:
                    raise RuntimeError(
                        f"Location ({center_lat}, {center_lon}) falls outside the GOES CONUS sector."
                    )
                windows[(center_lat, center_lon, range_miles)] = (y_indices, x_indices)

            # Auto-masking has to come off for the data variables: ACM's
            # Cloud_Probabilities declares valid_range = [0, 1] in *physical*
            # units while storing packed uint16, so netCDF4 masks all 100% of
            # the array and the field silently reads back as entirely NaN.
            # Unpack by hand against _FillValue instead.
            dataset.set_auto_maskandscale(False)
            for variable_name, column in variables:
                variable = dataset.variables[variable_name]
                fill_value = float(getattr(variable, '_FillValue', np.nan))
                scale_factor = float(getattr(variable, 'scale_factor', 1.0))
                add_offset = float(getattr(variable, 'add_offset', 0.0))
                for location, (y_indices, x_indices) in windows.items():
                    packed = np.asarray(
                        variable[y_indices.min():y_indices.max() + 1, x_indices.min():x_indices.max() + 1],
                        dtype='float64',
                    )
                    unpacked = np.where(packed == fill_value, np.nan, packed * scale_factor + add_offset)
                    columns.setdefault(location, {})[column] = unpacked
        finally:
            dataset.close()

        return columns, grid_x, grid_y, windows, geos_crs, scan_start


    def pull_weather_clouds(self):
        # Take the newest scan present in EVERY product, not each product's own
        # newest. They are published a couple of minutes apart, so the latest
        # ACM regularly has no matching COD yet, and mixing scans would put
        # fields from different instants in one row.
        product_scans = [(product, variables, self.list_product_scans(product))
                         for product, variables in GOES_CLOUD_PRODUCTS]
        common_scans = set(product_scans[0][2])
        for _, _, scans in product_scans[1:]:
            common_scans.intersection_update(scans)
        if not common_scans:
            raise RuntimeError("No GOES scan time is present in every cloud product.")
        scan_token = max(common_scans)
        # Token is YYYYDDDHHMMSSt — the trailing digit is tenths of a second.
        generate_time = dt.datetime.strptime(scan_token[:13], '%Y%j%H%M%S').replace(tzinfo=dt.timezone.utc)
        logger.info(f"Fetching GOES cloud scan {scan_token} across {len(product_scans)} products.")

        fields_by_location = {}
        geometry = None
        for product, variables, scans in product_scans:
            columns, grid_x, grid_y, windows, geos_crs, _ = self.read_product_grid(scans[scan_token], variables)
            for location, location_columns in columns.items():
                fields_by_location.setdefault(location, {}).update(location_columns)
            geometry = (grid_x, grid_y, windows, geos_crs)

        grid_x, grid_y, windows, geos_crs = geometry
        cloud_dicts = []
        for (center_lat, center_lon, range_miles), fields in fields_by_location.items():
            y_indices, x_indices = windows[(center_lat, center_lon, range_miles)]
            window_x = grid_x[x_indices.min():x_indices.max() + 1]
            window_y = grid_y[y_indices.min():y_indices.max() + 1]
            mesh_x, mesh_y = np.meshgrid(window_x, window_y)

            utm_crs, utm_epsg = utm_crs_for_center(center_lat, center_lon)
            to_utm = Transformer.from_crs(geos_crs, utm_crs, always_xy=True)
            utm_x, utm_y = to_utm.transform(mesh_x, mesh_y)

            cloud_dict = {
                'generate_time': generate_time.isoformat(),
                'satellite': 'G19',
                'x_easting': grid_to_json_safe(utm_x),
                'y_northing': grid_to_json_safe(utm_y),
                'center_lat': center_lat,
                'center_lon': center_lon,
                'range_miles': range_miles,
                'utm_zone_epsg': utm_epsg,
            }
            for column, values in fields.items():
                cloud_dict[column] = grid_to_json_safe(values)
            cloud_dicts.append(cloud_dict)

        return cloud_dicts


    def produce_clouds_to_kafka(self, cloud_dicts):
        # Same double-encoded value shape as the other topics — see the note in
        # WeatherForecastProducer.produce_current_and_forecast_to_kafka().
        for cloud_dict in cloud_dicts:
            self.kafka.produce(self.topic_name, value=json.dumps(cloud_dict), key=self.partition_key,
                               headers={'service': b'weather', 'datatype': b'clouds'})
            _emitted_total.inc()
        self.kafka.flush()
        logger.info(f"Produced {len(cloud_dicts)} cloud payloads to Kafka.")


class WeatherCloudLayerProducer:
    """HRRR low/middle/high cloud fraction and cloud base/top height.

    One row per location per forecast hour. Forecast hour 0 is the analysis —
    the model's best estimate of the present — and anything above it lets a
    viewer animate cloud cover forward.
    """

    def __init__(self, bucket_url, lat_lon_range_list, forecast_hours, poll_interval_seconds,
                 kafka: KafkaProducer, db: WeatherDb):
        self.bucket_url = bucket_url
        self.poll_interval_seconds = poll_interval_seconds
        self.kafka = kafka
        self.db = db
        self.location_list = lat_lon_range_list
        self.forecast_hours = forecast_hours

        self.topic_name = "weather_cloud_layers"
        self.partition_key = "0"


    def insert_weather_cloud_layers(self, layer_dicts: list[dict]):
        """Insert the clipped cloud-layer payloads into laddms.weather_cloud_layers."""
        write_time = now_dtz()
        rows = []
        for layer_dict in layer_dicts:
            row = dict(layer_dict)
            for column, value in row.items():
                if isinstance(value, list):
                    row[column] = json.dumps(value)
            rows.append({'write_time': write_time, **row})
        self.db.insert_weather_cloud_layers(rows)
        logger.info(f"Inserted {len(rows)} rows into laddms.weather_cloud_layers.")


    def wait(self):
        _sleep_responsively(self.poll_interval_seconds)


    def grib_url(self, run_time, forecast_hour):
        return (f"{self.bucket_url}/hrrr.{run_time:%Y%m%d}/conus/"
                f"hrrr.t{run_time:%H}z.wrfsfcf{forecast_hour:02d}.grib2")


    def latest_run(self):
        """Newest HRRR run whose furthest needed forecast hour has been published.

        A run's files land progressively over roughly an hour, so the newest run
        directory on the bucket is regularly incomplete. Probe backwards and
        take the first run that has the last hour we intend to read.
        """
        furthest_hour = max(self.forecast_hours)
        now = dt.datetime.now(tz=dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
        for hours_back in range(1, HRRR_RUN_SEARCH_HOURS + 1):
            run_time = now - dt.timedelta(hours=hours_back)
            response = requests.head(f"{self.grib_url(run_time, furthest_hour)}.idx", timeout=30)
            if response.status_code == 200:
                return run_time
        raise RuntimeError(
            f"No HRRR run with forecast hour {furthest_hour} found in the last {HRRR_RUN_SEARCH_HOURS} hours."
        )


    def pull_cloud_layers_for_hour(self, run_time, forecast_hour):
        """Fetch and clip one HRRR forecast hour, returning a dict per location."""
        grib_url = self.grib_url(run_time, forecast_hour)

        # The .idx sidecar lists every GRIB message with its byte offset, so the
        # cloud fields can be pulled without downloading the ~130 MB file.
        index_response = requests.get(f"{grib_url}.idx", timeout=60)
        if index_response.status_code != 200:
            raise RuntimeError(f"Failed to fetch HRRR index {grib_url}.idx: HTTP {index_response.status_code}")
        index_rows = [line.split(':') for line in index_response.text.strip().split('\n')]
        message_starts = {int(row[0]): int(row[1]) for row in index_rows}

        wanted = {(parameter, level) for parameter, level, _, _ in HRRR_CLOUD_FIELDS}
        message_numbers = [int(row[0]) for row in index_rows if (row[3], row[4]) in wanted]
        if len(message_numbers) < len(wanted):
            raise RuntimeError(
                f"HRRR index for f{forecast_hour:02d} is missing cloud fields "
                f"(found {len(message_numbers)} of {len(wanted)})."
            )

        # One contiguous range over the whole cloud block instead of a request
        # per field. The block has a few unrelated messages interleaved; they
        # decode into extra bands that are simply not selected below.
        first_byte = message_starts[min(message_numbers)]
        last_message = max(message_numbers)
        last_byte = message_starts[last_message + 1] - 1 if (last_message + 1) in message_starts else ''
        block_response = requests.get(grib_url, headers={'Range': f'bytes={first_byte}-{last_byte}'}, timeout=180)
        if block_response.status_code not in (200, 206):
            raise RuntimeError(f"Failed to download HRRR block {grib_url}: HTTP {block_response.status_code}")

        valid_time = run_time + dt.timedelta(hours=forecast_hour)
        layer_dicts = []
        with MemoryFile(block_response.content) as memfile, memfile.open() as src:
            # Select bands by their GRIB tags rather than by position: the block
            # contains unrelated interleaved messages and its composition shifts
            # between forecast hours.
            band_for_column = {}
            for band in range(1, src.count + 1):
                tags = src.tags(band)
                for parameter, _, short_name, column in HRRR_CLOUD_FIELDS:
                    if tags.get('GRIB_ELEMENT') == parameter and tags.get('GRIB_SHORT_NAME') == short_name:
                        band_for_column[column] = band
            missing = [column for _, _, _, column in HRRR_CLOUD_FIELDS if column not in band_for_column]
            if missing:
                raise RuntimeError(f"HRRR block for f{forecast_hour:02d} is missing bands for: {missing}")

            to_grid = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
            for center_lat, center_lon, range_miles in self.location_list:
                center_x, center_y = to_grid.transform(center_lon, center_lat)
                buffer_m = 1609.34 * range_miles
                row_top, col_left = rowcol(src.transform, center_x - buffer_m, center_y + buffer_m)
                row_bottom, col_right = rowcol(src.transform, center_x + buffer_m, center_y - buffer_m)
                row_top = max(0, min(int(row_top), src.height - 1))
                row_bottom = max(0, min(int(row_bottom), src.height - 1))
                col_left = max(0, min(int(col_left), src.width - 1))
                col_right = max(0, min(int(col_right), src.width - 1))
                window = rasterio.windows.Window.from_slices(
                    (row_top, row_bottom + 1), (col_left, col_right + 1))

                layer_dict = {
                    'generate_time': run_time.isoformat(),
                    'valid_time': valid_time.isoformat(),
                    'forecast_hour': forecast_hour,
                    'center_lat': center_lat,
                    'center_lon': center_lon,
                    'range_miles': range_miles,
                }
                for column, band in band_for_column.items():
                    values = src.read(band, window=window).astype('float64')
                    if column.endswith('_height'):
                        values = np.where(values == HRRR_NO_CLOUD_HEIGHT, np.nan, values)
                    layer_dict[column] = grid_to_json_safe(values)

                grid_rows, grid_cols = np.meshgrid(
                    np.arange(row_top, row_bottom + 1), np.arange(col_left, col_right + 1), indexing='ij')
                flat_x, flat_y = xy(src.transform, grid_rows.flatten(), grid_cols.flatten(), offset='center')
                projected_x = np.array(flat_x).reshape(grid_rows.shape)
                projected_y = np.array(flat_y).reshape(grid_rows.shape)

                utm_crs, utm_epsg = utm_crs_for_center(center_lat, center_lon)
                to_utm = Transformer.from_crs(src.crs, utm_crs, always_xy=True)
                utm_x, utm_y = to_utm.transform(projected_x, projected_y)
                layer_dict['x_easting'] = grid_to_json_safe(utm_x)
                layer_dict['y_northing'] = grid_to_json_safe(utm_y)
                layer_dict['utm_zone_epsg'] = utm_epsg
                layer_dicts.append(layer_dict)

        return layer_dicts


    def pull_weather_cloud_layers(self):
        run_time = self.latest_run()
        logger.info(f"Fetching HRRR cloud layers from run {run_time:%Y-%m-%d %H}z, "
                    f"forecast hours {self.forecast_hours}.")
        layer_dicts = []
        for forecast_hour in self.forecast_hours:
            layer_dicts.extend(self.pull_cloud_layers_for_hour(run_time, forecast_hour))
        return layer_dicts


    def produce_cloud_layers_to_kafka(self, layer_dicts):
        # Same double-encoded value shape as the other topics.
        for layer_dict in layer_dicts:
            self.kafka.produce(self.topic_name, value=json.dumps(layer_dict), key=self.partition_key,
                               headers={'service': b'weather', 'datatype': b'cloud_layers'})
            _emitted_total.inc()
        self.kafka.flush()
        logger.info(f"Produced {len(layer_dicts)} cloud layer payloads to Kafka.")


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


def update_weather_clouds(url, lat_lon_range_location_list, poll_interval,
                          kafka: KafkaProducer, db: WeatherDb):
    cloud_receiver = WeatherCloudProducer(url, lat_lon_range_location_list, poll_interval,
                                          kafka=kafka, db=db)
    logger.info("Created new instance of weather cloud receiver.")
    while not _shutdown:
        # 1) get the latest GOES cloud scan
        try:
            with _fetch_seconds.time():
                rcv_data = cloud_receiver.pull_weather_clouds()
        except Exception as e:
            logger.error("Failed to pull updated GOES cloud data.")
            logger.exception(e, exc_info=True)
            cloud_receiver.wait()
            continue
        _fetched_total.inc(len(rcv_data))
        # 2) produce cloud data to Kafka
        try:
            cloud_receiver.produce_clouds_to_kafka(cloud_dicts=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send cloud data to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) insert to database
        try:
            cloud_receiver.insert_weather_clouds(cloud_dicts=rcv_data)
        except Exception as e:
            logger.error("Failed to insert weather cloud data.")
            logger.exception(e, exc_info=True)
        # 4) invoke WAIT on the receiver object
        cloud_receiver.wait()


def update_weather_cloud_layers(url, lat_lon_range_location_list, forecast_hours, poll_interval,
                                kafka: KafkaProducer, db: WeatherDb):
    layer_receiver = WeatherCloudLayerProducer(url, lat_lon_range_location_list, forecast_hours,
                                               poll_interval, kafka=kafka, db=db)
    logger.info("Created new instance of weather cloud layer receiver.")
    while not _shutdown:
        # 1) get the latest HRRR run
        try:
            with _fetch_seconds.time():
                rcv_data = layer_receiver.pull_weather_cloud_layers()
        except Exception as e:
            logger.error("Failed to pull updated HRRR cloud layer data.")
            logger.exception(e, exc_info=True)
            layer_receiver.wait()
            continue
        _fetched_total.inc(len(rcv_data))
        # 2) produce cloud layers to Kafka
        try:
            layer_receiver.produce_cloud_layers_to_kafka(layer_dicts=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send cloud layer data to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) insert to database
        try:
            layer_receiver.insert_weather_cloud_layers(layer_dicts=rcv_data)
        except Exception as e:
            logger.error("Failed to insert weather cloud layer data.")
            logger.exception(e, exc_info=True)
        # 4) invoke WAIT on the receiver object
        layer_receiver.wait()


def main() -> None:
    global _shutdown

    # One call wires up JSON logging on stdout and the Prometheus /metrics
    # endpoint on :9100. TELEMETRY_LOG_LEVEL=DEBUG replaces the old debug flag.
    tel = configure_telemetry(service=SERVICE)
    _bind_telemetry(tel)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    logger.info("Starting 4x weather to Kafka producer threads.")

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
        cloud_location_tuples = [
            (
                float(os.environ.get('WEATHER_CLOUD_LAT')),
                float(os.environ.get('WEATHER_CLOUD_LON')),
                float(os.environ.get('WEATHER_CLOUD_RANGE_MI'))
            ),
        ]
        # "0,1,2,3" -> [0, 1, 2, 3]. Hour 0 is the HRRR analysis (the model's
        # present); the rest are the forecast hours a viewer animates through.
        cloud_layer_forecast_hours = [
            int(hour) for hour in os.environ.get('WEATHER_CLOUD_LAYER_FORECAST_HOURS').split(',')
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
            threading.Thread(target=thread_wrapper(update_weather_clouds, args=(
                os.environ.get('WEATHER_CLOUD_URL'),
                cloud_location_tuples,
                int(os.environ.get('WEATHER_CLOUD_UPDATE_SECS')),
                kafka,
                db), name="weather_clouds"), name="weather_clouds"),
            threading.Thread(target=thread_wrapper(update_weather_cloud_layers, args=(
                os.environ.get('WEATHER_CLOUD_LAYER_URL'),
                cloud_location_tuples,
                cloud_layer_forecast_hours,
                int(os.environ.get('WEATHER_CLOUD_LAYER_UPDATE_SECS')),
                kafka,
                db), name="weather_cloud_layers"), name="weather_cloud_layers"),
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
