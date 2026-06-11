"""Open-Meteo historical (archive) weather client.

For a set of airports and a single date we fetch hourly temperature /
precipitation / weather_code in the airport's LOCAL time (timezone=auto), so the
hourly timestamps line up directly with the flights' local schedule. One request
per airport per day keeps API load proportional to the increment (only airports
that actually appear in the day's flights are queried).

Returns a DataFrame: airport_dk, ts_hour_local, temp_c, precip_mm, weather_code.
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from .config import WEATHER
from .logging_utils import get_logger

log = get_logger(__name__)

OUTPUT_COLUMNS = ["airport_dk", "ts_hour_local", "temp_c", "precip_mm", "weather_code"]


def _fetch_one(session: requests.Session, airport_dk: str,
               lat: float, lon: float, date_str: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": ",".join(WEATHER["hourly"]),
        "timezone": "auto",
    }
    last_err = None
    for attempt in range(1, int(WEATHER.get("retries", 4)) + 1):
        try:
            resp = session.get(WEATHER["base_url"], params=params,
                               timeout=WEATHER.get("timeout_sec", 30))
            if resp.status_code == 429:  # rate limited — back off and retry
                raise requests.HTTPError("429 Too Many Requests")
            resp.raise_for_status()
            hourly = resp.json().get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                return pd.DataFrame(columns=OUTPUT_COLUMNS)
            out = pd.DataFrame({
                "airport_dk": airport_dk,
                "ts_hour_local": pd.to_datetime(times),
                "temp_c": hourly.get("temperature_2m"),
                "precip_mm": hourly.get("precipitation"),
                "weather_code": hourly.get("weather_code"),
            })
            return out
        except Exception as err:  # noqa: BLE001 - retry any transient failure
            last_err = err
            sleep_s = WEATHER.get("backoff_sec", 2) * attempt
            log.warning("weather fetch %s attempt %d failed: %s (retry in %ss)",
                        airport_dk, attempt, err, sleep_s)
            time.sleep(sleep_s)
    log.error("weather fetch for %s gave up: %s", airport_dk, last_err)
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def fetch_weather(airports: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """airports: DataFrame with columns airport_dk, lat, lon."""
    if airports is None or airports.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    frames = []
    max_workers = int(WEATHER.get("max_workers", 4))
    with requests.Session() as session, \
            ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_one, session, row.airport_dk,
                        float(row.lat), float(row.lon), date_str): row.airport_dk
            for row in airports.itertuples(index=False)
        }
        for fut in as_completed(futures):
            df = fut.result()
            if not df.empty:
                frames.append(df)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    result = pd.concat(frames, ignore_index=True)
    # weather_code is an integer WMO code; use nullable Int64 so COPY writes
    # "3"/"<NA>" (not "3.0") into the int column.
    result["weather_code"] = pd.to_numeric(
        result["weather_code"], errors="coerce").astype("Int64")
    log.info("Fetched %d hourly weather rows for %s airports on %s",
             len(result), airports["airport_dk"].nunique(), date_str)
    return result[OUTPUT_COLUMNS]
