"""Loads .sql files from sql/ and injects the (config-driven) schema names.

SQL files reference layers as ``{ods}`` / ``{stg}`` / ``{dds}`` / ``{dm}`` and
runtime parameters as psycopg2 named binds ``%(process_date)s``. We str.format
the schema names first (trusted config values), then psycopg2 binds the params.

Rules for SQL authors:
  * use ``{ods}.table`` etc. for schema-qualified names;
  * never write a literal ``{`` or ``}`` (would break str.format);
  * never write a bare ``%`` (would break psycopg2 binding) — use mod()/division.
"""
import os

from .config import SQL_DIR, SCHEMAS


def render_sql(rel_path: str) -> str:
    """Read sql/<rel_path> and substitute schema names."""
    path = os.path.join(SQL_DIR, rel_path)
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    return raw.format(**SCHEMAS)
