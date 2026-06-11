"""US flights DWH — Airflow + Python + SQL pipeline (Inmon-style, batch).

Layers (one Postgres DB, schema per layer, tagged to avoid collisions with
other students):  ods -> stg -> dds -> dm.
"""
