"""US flights DWH — daily batch DAG (S3 -> ods -> stg -> dds -> dm).

Source: s3://gsbdwhdata/flights_us_data/YYYY-MM-DD/*.csv.gz (BTS On-Time Performance).
Enriched with the ourairports reference (+ IANA timezone) and Open-Meteo weather.
Builds two DDS fact tables (completed / cancelled flights) and per-chart DM marts
for the DataLens dashboard.

Run a specific day:   trigger with config  {"process_date": "2024-01-01"}.
Run a date range:     trigger with config  {"start_date": "2024-01-01", "end_date": "2024-03-31"}.
Full rebuild:         trigger with config  {"full_reload": true}  (+ a date range).
Refresh airports:     trigger with config  {"refresh_airports": true}.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

# Make the DAG self-locating: add the folder containing this file (where the
# `flights_dwh` package lives) to sys.path. Airflow only auto-adds the TOP dags
# folder, so when the bucket sync drops this DAG into a nested subfolder the
# sibling package would otherwise be unimportable -> ModuleNotFoundError.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from flights_dwh import tasks

default_args = {
    "owner": "mblv",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="flights_us_dwh_v7",
    description="US domestic flights DWH: S3 -> ods -> stg -> dds -> dm",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dwh", "flights", "mblv"],
    params={
        "process_date": Param(
            default=None, type=["null", "string"],
            description="Override the processed day (YYYY-MM-DD). Defaults to the run's ds.",
        ),
        "start_date": Param(
            default=None, type=["null", "string"],
            description="Range start (YYYY-MM-DD). With end_date, one run processes every day in [start, end].",
        ),
        "end_date": Param(
            default=None, type=["null", "string"],
            description="Range end (YYYY-MM-DD), inclusive. Used together with start_date.",
        ),
        "full_reload": Param(
            default=False, type="boolean",
            description="Truncate all layer tables before loading (use with a backfill).",
        ),
        "refresh_airports": Param(
            default=False, type="boolean",
            description="Re-download the airports reference even if already loaded.",
        ),
    },
    doc_md=__doc__,
) as dag:

    init_ddl = PythonOperator(
        task_id="init_ddl", python_callable=tasks.init_ddl)

    load_reference = PythonOperator(
        task_id="load_reference", python_callable=tasks.load_reference)

    extract_s3_to_ods = PythonOperator(
        task_id="extract_s3_to_ods", python_callable=tasks.extract_s3_to_ods)

    ods_to_stg = PythonOperator(
        task_id="ods_to_stg", python_callable=tasks.ods_to_stg)

    load_weather = PythonOperator(
        task_id="load_weather", python_callable=tasks.load_weather)

    stg_to_dds = PythonOperator(
        task_id="stg_to_dds", python_callable=tasks.stg_to_dds)

    dds_to_dm = PythonOperator(
        task_id="dds_to_dm", python_callable=tasks.dds_to_dm)

    dq_checks = PythonOperator(
        task_id="dq_checks", python_callable=tasks.dq_checks)

    init_ddl >> [load_reference, extract_s3_to_ods]
    extract_s3_to_ods >> ods_to_stg >> load_weather
    [ods_to_stg, load_weather, load_reference] >> stg_to_dds >> dds_to_dm >> dq_checks
