"""Postgres IO helpers built on Airflow's PostgresHook.

Everything talks to the `edu_dwh_postgres` connection. We expose:
  * run_sql / run_sql_file     — execute statements (optionally a .sql file);
  * copy_dataframe             — fast bulk load via COPY FROM STDIN;
  * copy_upsert                — COPY into a TEMP table then INSERT .. ON CONFLICT;
  * get_dataframe / get_scalar — read helpers;
  * truncate.
"""
import io

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .config import POSTGRES_CONN_ID
from .logging_utils import get_logger
from .sql import render_sql

log = get_logger(__name__)

# Sentinel written for NULLs in the COPY stream (must not collide with real data).
_NULL_TOKEN = "\\N"


def get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def run_sql(sql: str, params: dict | None = None, autocommit: bool = False) -> None:
    """Execute one (possibly multi-statement) SQL string in a single transaction."""
    get_hook().run(sql, autocommit=autocommit, parameters=params)


def run_sql_file(rel_path: str, params: dict | None = None,
                 autocommit: bool = False) -> None:
    """Render sql/<rel_path> (schema substitution) and execute it."""
    log.info("Running SQL file %s params=%s", rel_path, params)
    run_sql(render_sql(rel_path), params=params, autocommit=autocommit)


def get_dataframe(sql: str, params: dict | None = None):
    return get_hook().get_pandas_df(sql, parameters=params)


def get_scalar(sql: str, params: dict | None = None):
    row = get_hook().get_first(sql, parameters=params)
    return row[0] if row else None


def truncate(*tables: str) -> None:
    if not tables:
        return
    run_sql("TRUNCATE TABLE " + ", ".join(tables) + ";", autocommit=True)


def _df_to_csv_buffer(df, columns) -> io.StringIO:
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, na_rep=_NULL_TOKEN)
    buf.seek(0)
    return buf


def copy_dataframe(df, table: str, columns: list[str]) -> int:
    """Bulk load a DataFrame into `table` via COPY. Returns rows loaded."""
    if df is None or df.empty:
        log.info("copy_dataframe: nothing to load into %s", table)
        return 0
    buf = _df_to_csv_buffer(df, columns)
    col_list = ", ".join(columns)
    hook = get_hook()
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} ({col_list}) "
                f"FROM STDIN WITH (FORMAT csv, NULL '{_NULL_TOKEN}')",
                buf,
            )
        conn.commit()
    finally:
        conn.close()
    log.info("Loaded %d rows into %s", len(df), table)
    return len(df)


def copy_upsert(df, table: str, columns: list[str],
                conflict_cols: list[str], update_cols: list[str]) -> int:
    """COPY into a TEMP table, then INSERT .. ON CONFLICT DO UPDATE into `table`."""
    if df is None or df.empty:
        log.info("copy_upsert: nothing to upsert into %s", table)
        return 0
    buf = _df_to_csv_buffer(df, columns)
    col_list = ", ".join(columns)
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
    hook = get_hook()
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE _upsert_tmp "
                f"(LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP;"
            )
            cur.copy_expert(
                f"COPY _upsert_tmp ({col_list}) "
                f"FROM STDIN WITH (FORMAT csv, NULL '{_NULL_TOKEN}')",
                buf,
            )
            cur.execute(
                f"INSERT INTO {table} ({col_list}) "
                f"SELECT {col_list} FROM _upsert_tmp "
                f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause};"
            )
        conn.commit()
    finally:
        conn.close()
    log.info("Upserted %d rows into %s", len(df), table)
    return len(df)
