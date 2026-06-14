-- stg.flights -> dds.fct_flight_completed (cancelled = 0).
-- Localizes scheduled departure to the origin airport's tz (timestamptz),
-- derives actual departure = scheduled + dep_delay, and enriches with the
-- origin airport's weather for that local hour.
-- Idempotent per partition (delete + insert). Param: %(process_date)s.

DELETE FROM {dds}.fct_flight_completed WHERE flight_dt = %(process_date)s;

INSERT INTO {dds}.fct_flight_completed (
    flight_dt, carrier_flight_num, flight_dttm_local, actual_dep_dttm_local,
    origin_airport_dk, dest_airport_dk, carrier_code, tail_num, distance_mi,
    dep_delay_min, arr_delay_min, arr_del15,
    carrier_delay_min, weather_delay_min, nas_delay_min,
    security_delay_min, late_aircraft_min,
    origin_temp_c, origin_precip_mm, origin_weather_code
)
SELECT DISTINCT ON (carrier_flight_num, flight_dttm_local, origin_airport_dk)
    flight_dt, carrier_flight_num, flight_dttm_local, actual_dep_dttm_local,
    origin_airport_dk, dest_airport_dk, carrier_code, tail_num, distance_mi,
    dep_delay_min, arr_delay_min, arr_del15,
    carrier_delay_min, weather_delay_min, nas_delay_min,
    security_delay_min, late_aircraft_min,
    origin_temp_c, origin_precip_mm, origin_weather_code
FROM (
    SELECT
        f.flight_dt,
        f.carrier_flight_num,
        (f.sched_dep_local AT TIME ZONE COALESCE(o.tz_name, 'UTC')) AS flight_dttm_local,
        (f.sched_dep_local AT TIME ZONE COALESCE(o.tz_name, 'UTC'))
            + (COALESCE(f.dep_delay, 0) * interval '1 minute')      AS actual_dep_dttm_local,
        f.origin_code AS origin_airport_dk,
        f.dest_code   AS dest_airport_dk,
        f.carrier_code,
        f.tail_num,
        f.distance_mi,
        f.dep_delay AS dep_delay_min,
        (
            COALESCE(carrier_delay_min, 0)
            + COALESCE(weather_delay_min, 0)
            + COALESCE(nas_delay_min, 0)
            + COALESCE(security_delay_min, 0)
            + COALESCE(late_aircraft_min, 0)
        ) AS arr_delay_min,
        f.arr_del15,
        f.carrier_delay_min,
        f.weather_delay_min,
        f.nas_delay_min,
        f.security_delay_min,
        f.late_aircraft_min,
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
      AND f.cancelled = 0
      AND f.sched_dep_local IS NOT NULL
      AND f.carrier_flight_num IS NOT NULL
) s
ORDER BY carrier_flight_num, flight_dttm_local, origin_airport_dk, dest_airport_dk;
