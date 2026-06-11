-- stg.flights -> dds.fct_flight_cancelled (cancelled = 1).
-- Scheduled departure localized to origin tz; enriched with origin weather.
-- Idempotent per partition (delete + insert). Param: %(process_date)s.

DELETE FROM {dds}.fct_flight_cancelled WHERE flight_dt = %(process_date)s;

INSERT INTO {dds}.fct_flight_cancelled (
    flight_dt, carrier_flight_num, sched_dttm_local,
    origin_airport_dk, dest_airport_dk, carrier_code, cancellation_code,
    origin_temp_c, origin_precip_mm, origin_weather_code
)
SELECT DISTINCT ON (carrier_flight_num, sched_dttm_local, origin_airport_dk)
    flight_dt, carrier_flight_num, sched_dttm_local,
    origin_airport_dk, dest_airport_dk, carrier_code, cancellation_code,
    origin_temp_c, origin_precip_mm, origin_weather_code
FROM (
    SELECT
        f.flight_dt,
        f.carrier_flight_num,
        (f.sched_dep_local AT TIME ZONE COALESCE(o.tz_name, 'UTC')) AS sched_dttm_local,
        f.origin_code AS origin_airport_dk,
        f.dest_code   AS dest_airport_dk,
        f.carrier_code,
        f.cancellation_code,
        w.temp_c       AS origin_temp_c,
        w.precip_mm    AS origin_precip_mm,
        w.weather_code AS origin_weather_code
    FROM {stg}.flights f
    LEFT JOIN {dds}.dim_airport o
           ON o.airport_dk = f.origin_code
    LEFT JOIN {stg}.weather w
           ON w.airport_dk = f.origin_code
          AND w.ts_hour_local = date_trunc('hour', f.sched_dep_local)
    WHERE f.flight_dt = %(process_date)s
      AND f.cancelled = 1
      AND f.sched_dep_local IS NOT NULL
      AND f.carrier_flight_num IS NOT NULL
) s
ORDER BY carrier_flight_num, sched_dttm_local, origin_airport_dk, dest_airport_dk;
