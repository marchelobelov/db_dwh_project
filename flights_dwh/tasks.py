"""Task callables for the flights DWH DAG.

Each function is a thin orchestration step that leans on common/* helpers and the
SQL files in sql/. The DAG file wires them together; keeping logic here makes the
DAG importable and the steps unit-testable.

Two distinct "dates" exist because the S3 folders are named with a date that is
shifted from the real flight date inside the files (e.g. folder 2026-03-01 holds
flights for 2024-03-01):

  * SOURCE date  = the S3 folder name. Identifies which file to read.
    Selected by params start_date+end_date / process_date / the run's ds
    (see `_iter_source_dates`).
  * FLIGHT date  = the real FlightDate from the file content. This is the
    warehouse partition key (`flight_dt`) used by every downstream layer and by
    the weather lookup (so Open-Meteo is queried for the real date). Derived from
    what extract actually loaded (see `_flight_dates`).

Every step is idempotent (delete + insert / upsert), so re-running is safe.
"""
import datetime as dt

import pandas as pd

from .common import airports, pg_io, s3_io, weather
from .common.config import SCHEMAS, CARRIERS_SEED_PATH, S3_SOURCE_PREFIX
from .common.logging_utils import get_logger
from .config.columns import SOURCE_TO_ODS, ODS_LOAD_COLUMNS

log = get_logger(__name__)

# DDL files executed (in order) on every run — all CREATE ... IF NOT EXISTS.
DDL_FILES = [
    "ddl/00_schemas.sql",
    "ddl/10_ods.sql",
    "ddl/20_stg.sql",
    "ddl/30_dds.sql",
    "ddl/40_dm.sql",
]

# Layer tables truncated on a full reload (no FKs between layers -> any order).
_RELOAD_TABLES = [
    f"{SCHEMAS['ods']}.flights_raw",
    f"{SCHEMAS['stg']}.flights",
    f"{SCHEMAS['stg']}.weather",
    f"{SCHEMAS['dds']}.fct_flight_completed",
    f"{SCHEMAS['dds']}.fct_flight_cancelled",
    f"{SCHEMAS['dm']}.airport_quality",
    f"{SCHEMAS['dm']}.route_load",
    f"{SCHEMAS['dm']}.carrier_quality",
    f"{SCHEMAS['dm']}.carrier_delay_reasons",
    f"{SCHEMAS['dm']}.carrier_cancellations",
    f"{SCHEMAS['dm']}.weather_delays",
]

DM_FILES = [
    "dm/dm_airport_quality.sql",
    "dm/dm_route_load.sql",
    "dm/dm_carrier_quality.sql",
    "dm/dm_carrier_delay_reasons.sql",
    "dm/dm_carrier_cancellations.sql",
    "dm/dm_weather_delays.sql",
]


def _iter_source_dates(context) -> list[str]:
    """S3 folder dates (YYYY-MM-DD) to read for this run."""
    params = context.get("params") or {}
    start, end = params.get("start_date"), params.get("end_date")
    if start and end:
        s = dt.date.fromisoformat(start)
        e = dt.date.fromisoformat(end)
        if e < s:
            s, e = e, s
        dates = [(s + dt.timedelta(days=i)).isoformat()
                 for i in range((e - s).days + 1)]
        log.info("Reading S3 folder range %s..%s (%d days)", start, end, len(dates))
        return dates
    one = params.get("process_date") or context["ds"]
    return [one]


def _flight_dates(context) -> list[str]:
    """Real flight dates (from file content) loaded for this run's S3 folders.

    Driven by what extract actually wrote to ODS, so we never assume a fixed
    folder-vs-content offset.
    """
    folders = _iter_source_dates(context)
    prefixes = [f"{S3_SOURCE_PREFIX}/{f}/%" for f in folders]
    sql = (f"SELECT DISTINCT flight_dt FROM {SCHEMAS['ods']}.flights_raw "
           f"WHERE source_file LIKE ANY(%(prefixes)s) ORDER BY flight_dt")
    df = pg_io.get_dataframe(sql, params={"prefixes": prefixes})
    dates = ([] if df.empty
             else pd.to_datetime(df["flight_dt"]).dt.strftime("%Y-%m-%d").tolist())
    log.info("Flight dates in scope: %s", dates)
    return dates


# --- pipeline steps -----------------------------------------------------------

def init_ddl(**context):
    """Create schemas/tables/indexes if missing. On full_reload, truncate layers."""
    for ddl in DDL_FILES:
        pg_io.run_sql_file(ddl, autocommit=True)
    if bool((context.get("params") or {}).get("full_reload")):
        log.warning("full_reload=True -> truncating all layer tables")
        pg_io.truncate(*_RELOAD_TABLES)


def load_reference(**context):
    """Load ourairports (+tz) and the carrier seed. Airports only when needed."""
    refresh = bool((context.get("params") or {}).get("refresh_airports"))
    have = pg_io.get_scalar(f"SELECT count(*) FROM {SCHEMAS['stg']}.airports") or 0
    if have and not refresh:
        log.info("stg.airports already has %s rows; skip reload", have)
    else:
        raw_df, clean_df = airports.build_airports()
        pg_io.truncate(f"{SCHEMAS['ods']}.airports_raw")
        pg_io.copy_dataframe(raw_df, f"{SCHEMAS['ods']}.airports_raw", airports.RAW_COLUMNS)
        pg_io.truncate(f"{SCHEMAS['stg']}.airports")
        pg_io.copy_dataframe(clean_df, f"{SCHEMAS['stg']}.airports", airports.CLEAN_COLUMNS)

    carriers = pd.read_csv(CARRIERS_SEED_PATH, dtype=str)
    pg_io.truncate(f"{SCHEMAS['stg']}.carriers")
    pg_io.copy_dataframe(carriers, f"{SCHEMAS['stg']}.carriers",
                         ["carrier_code", "carrier_name"])


def _extract_one(source_date: str):
    """Read one S3 folder, keep needed columns, COPY into ods.flights_raw.

    `flight_dt` is taken from the file content (the real FlightDate), which may
    differ from `source_date` (the folder name).
    """
    raw = s3_io.read_partition(source_date)
    if raw.empty:
        log.warning("No source files in folder %s — nothing to load into ODS", source_date)
        return

    # Case-insensitive column resolution; tolerate missing source columns.
    # Seed the index from `raw` so scalar-None columns broadcast to full length.
    lower_map = {c.lower(): c for c in raw.columns}
    out = pd.DataFrame(index=raw.index)
    for src, ods_col in SOURCE_TO_ODS.items():
        actual = lower_map.get(src.lower())
        out[ods_col] = raw[actual] if actual is not None else None
    out["source_file"] = raw["source_file"] if "source_file" in raw.columns else None

    out["flight_dt"] = pd.to_datetime(out["flight_date"], errors="coerce").dt.date
    dropped = int(out["flight_dt"].isna().sum())
    if dropped:
        log.warning("Dropping %d rows with unparseable FlightDate", dropped)
    out = out[out["flight_dt"].notna()]
    log.info("Folder %s -> flight dates %s",
             source_date, sorted({str(d) for d in out["flight_dt"].unique()}))

    table = f"{SCHEMAS['ods']}.flights_raw"
    # Idempotent per source folder: re-reading a folder replaces exactly its rows
    # (delete by source_file, since flight_dt may differ from the folder name).
    prefix = f"{S3_SOURCE_PREFIX}/{source_date}/%"
    pg_io.run_sql(f"DELETE FROM {table} WHERE source_file LIKE %(p)s",
                  params={"p": prefix}, autocommit=True)
    pg_io.copy_dataframe(out, table, ODS_LOAD_COLUMNS)


def extract_s3_to_ods(**context):
    for d in _iter_source_dates(context):
        _extract_one(d)


def ods_to_stg(**context):
    for d in _flight_dates(context):
        pg_io.run_sql_file("stg/stg_flights.sql", params={"process_date": d})


def _load_weather_one(date: str):
    sql = (
        f"SELECT DISTINCT f.origin_code AS airport_dk, a.lat, a.lon "
        f"FROM {SCHEMAS['stg']}.flights f "
        f"JOIN {SCHEMAS['stg']}.airports a ON a.airport_dk = f.origin_code "
        f"WHERE f.flight_dt = %(d)s AND a.lat IS NOT NULL AND a.lon IS NOT NULL"
    )
    airports_df = pg_io.get_dataframe(sql, params={"d": date})
    if airports_df.empty:
        log.warning("No origin airports with coordinates for %s; skip weather", date)
        return
    wdf = weather.fetch_weather(airports_df, date)
    if wdf.empty:
        log.warning("Open-Meteo returned no weather rows for %s", date)
        return
    pg_io.copy_upsert(
        wdf, f"{SCHEMAS['stg']}.weather", weather.OUTPUT_COLUMNS,
        conflict_cols=["airport_dk", "ts_hour_local"],
        update_cols=["temp_c", "precip_mm", "weather_code"],
    )


def load_weather(**context):
    """Fetch Open-Meteo weather for the airports in each flight date (real date)."""
    for d in _flight_dates(context):
        _load_weather_one(d)


def stg_to_dds(**context):
    # Dimensions are slowly-changing — refresh once per run, not per day.
    pg_io.run_sql_file("dds/dds_dim_airport.sql")
    pg_io.run_sql_file("dds/dds_dim_carrier.sql")
    for d in _flight_dates(context):
        pg_io.run_sql_file("dds/dds_fct_flight_completed.sql", params={"process_date": d})
        pg_io.run_sql_file("dds/dds_fct_flight_cancelled.sql", params={"process_date": d})


def dds_to_dm(**context):
    for d in _flight_dates(context):
        for dm_file in DM_FILES:
            pg_io.run_sql_file(dm_file, params={"process_date": d})


def dq_checks(**context):
    """Lightweight sanity checks per day; raise on anything that means broken output."""
    for date in _flight_dates(context):
        p = {"d": date}
        stg_cnt = pg_io.get_scalar(
            f"SELECT count(*) FROM {SCHEMAS['stg']}.flights WHERE flight_dt = %(d)s", p) or 0
        comp_cnt = pg_io.get_scalar(
            f"SELECT count(*) FROM {SCHEMAS['dds']}.fct_flight_completed WHERE flight_dt = %(d)s", p) or 0
        canc_cnt = pg_io.get_scalar(
            f"SELECT count(*) FROM {SCHEMAS['dds']}.fct_flight_cancelled WHERE flight_dt = %(d)s", p) or 0
        log.info("DQ %s: stg=%d dds_completed=%d dds_cancelled=%d",
                 date, stg_cnt, comp_cnt, canc_cnt)

        if stg_cnt > 0 and (comp_cnt + canc_cnt) == 0:
            raise ValueError(f"STG has {stg_cnt} rows for {date} but DDS facts are empty")

        bad_rate = pg_io.get_scalar(
            f"SELECT count(*) FROM {SCHEMAS['dm']}.airport_quality "
            f"WHERE flight_dt = %(d)s AND (cancel_rate < 0 OR cancel_rate > 1)", p) or 0
        if bad_rate:
            raise ValueError(f"{bad_rate} airport_quality rows have cancel_rate outside [0,1]")
