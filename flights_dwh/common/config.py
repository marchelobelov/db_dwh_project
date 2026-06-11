"""Loads non-secret config from config/pipeline.yaml and derives schema names.

Everything tweakable (schema tag, conn ids, bucket, weather params) lives in the
YAML; secrets stay in Airflow Connections. Import `CONFIG`, `SCHEMAS`, the conn
ids and the resolved paths from here — never hardcode them elsewhere.
"""
import os
import yaml

# .../flights_dwh
PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(PACKAGE_DIR, "config")
SQL_DIR = os.path.join(PACKAGE_DIR, "sql")
CARRIERS_SEED_PATH = os.path.join(CONFIG_DIR, "carriers_seed.csv")

_CONFIG_PATH = os.path.join(CONFIG_DIR, "pipeline.yaml")

with open(_CONFIG_PATH, "r", encoding="utf-8") as _f:
    CONFIG = yaml.safe_load(_f)

# --- schema names: mblv_ods / mblv_stg / mblv_dds / mblv_dm -------------------
TAG = CONFIG["tag"]
_PATTERN = CONFIG["schema_pattern"]
SCHEMAS = {layer: _PATTERN.format(tag=TAG, layer=layer)
           for layer in ("ods", "stg", "dds", "dm")}

# --- connections --------------------------------------------------------------
POSTGRES_CONN_ID = CONFIG["connections"]["postgres_conn_id"]
S3_CONN_ID = CONFIG["connections"]["s3_conn_id"]

# --- s3 -----------------------------------------------------------------------
S3_BUCKET = CONFIG["s3"]["bucket"]
S3_SOURCE_PREFIX = CONFIG["s3"]["source_prefix"]
S3_ENDPOINT_URL = CONFIG["s3"].get("endpoint_url")
S3_REGION = CONFIG["s3"].get("region")

# --- references ---------------------------------------------------------------
AIRPORTS_URL = CONFIG["airports"]["url"]
AIRPORTS_COUNTRIES = CONFIG["airports"].get("countries", ["US"])
AIRPORTS_TYPES = CONFIG["airports"].get("types")

WEATHER = CONFIG["weather"]
