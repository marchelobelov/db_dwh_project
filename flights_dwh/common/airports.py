"""ourairports reference loader + IANA timezone resolution.

ourairports has no timezone column, so we derive the IANA tz from lat/lon with
timezonefinder once here; the result is persisted into stg.airports and reused
by DDS to build local timestamptz values. Returns two frames:
  * raw_df   -> ods.airports_raw (selected columns, as text);
  * clean_df -> stg.airports (deduped, US-only, with tz_name).
"""
import pandas as pd

from .config import AIRPORTS_URL, AIRPORTS_COUNTRIES, AIRPORTS_TYPES
from .logging_utils import get_logger

log = get_logger(__name__)

# Columns kept raw in ods.airports_raw.
RAW_COLUMNS = [
    "id", "ident", "type", "name", "latitude_deg", "longitude_deg",
    "iso_country", "iso_region", "municipality", "iata_code",
    "local_code", "gps_code",
]

# Final clean columns (== stg.airports / dds.dim_airport layout).
CLEAN_COLUMNS = [
    "airport_dk", "ident", "name", "city", "region", "country",
    "lat", "lon", "tz_name",
]

# Prefer bigger airports when two share a code.
_TYPE_RANK = {"large_airport": 0, "medium_airport": 1, "small_airport": 2}


def _download() -> pd.DataFrame:
    log.info("Downloading airports reference from %s", AIRPORTS_URL)
    df = pd.read_csv(AIRPORTS_URL, dtype=str, low_memory=False)
    log.info("Downloaded %d airport rows", len(df))
    return df


def build_airports() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = _download()

    if AIRPORTS_COUNTRIES:
        df = df[df["iso_country"].isin(AIRPORTS_COUNTRIES)]
    if AIRPORTS_TYPES:
        df = df[df["type"].isin(AIRPORTS_TYPES)]

    # Make sure every raw column exists even if upstream drops one.
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None
    raw_df = df[RAW_COLUMNS].copy()

    clean = df.copy()
    # Airport business key: IATA when present, else FAA local code, else ICAO ident.
    clean["airport_dk"] = (
        clean["iata_code"].fillna("").str.strip()
        .replace("", pd.NA)
        .fillna(clean["local_code"])
        .fillna(clean["ident"])
    )
    clean = clean[clean["airport_dk"].notna() & (clean["airport_dk"] != "")]
    clean["lat"] = pd.to_numeric(clean["latitude_deg"], errors="coerce")
    clean["lon"] = pd.to_numeric(clean["longitude_deg"], errors="coerce")
    clean = clean[clean["lat"].notna() & clean["lon"].notna()]

    clean["tz_name"] = _resolve_timezones(clean["lat"], clean["lon"])

    clean = clean.rename(columns={
        "municipality": "city",
        "iso_region": "region",
        "iso_country": "country",
    })

    # Deduplicate on the business key, keeping the largest airport.
    clean["_rank"] = clean["type"].map(_TYPE_RANK).fillna(9)
    clean = (clean.sort_values(["airport_dk", "_rank"])
                  .drop_duplicates(subset=["airport_dk"], keep="first"))
    clean_df = clean[CLEAN_COLUMNS].copy()

    log.info("Prepared %d clean airports (of %d raw US rows)",
             len(clean_df), len(raw_df))
    return raw_df, clean_df


def _resolve_timezones(lat: pd.Series, lon: pd.Series) -> pd.Series:
    """Vectorized-ish lat/lon -> IANA tz via timezonefinder (single instance)."""
    from timezonefinder import TimezoneFinder

    tf = TimezoneFinder()

    def lookup(row):
        try:
            return tf.timezone_at(lat=row[0], lng=row[1])
        except Exception:  # pragma: no cover - defensive
            return None

    pairs = pd.DataFrame({"lat": lat.values, "lon": lon.values})
    return pairs.apply(lambda r: lookup((r["lat"], r["lon"])), axis=1).values
