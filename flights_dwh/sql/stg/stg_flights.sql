-- ods.flights_raw -> stg.flights : type-cast + deduplicate one day partition.
-- Idempotent: delete the partition, then re-insert the deduped rows.
-- Param: %(process_date)s (the day being processed).

DELETE FROM {stg}.flights WHERE flight_dt = %(process_date)s;

INSERT INTO {stg}.flights (
    flight_dt, carrier_code, carrier_flight_num, tail_num, origin_code, dest_code,
    origin_city_name, origin_state, dest_city_name, dest_state,
    crs_dep_time, sched_dep_local, dep_delay,
    crs_arr_time, sched_arr_local, arr_delay, arr_del15,
    cancelled, cancellation_code, diverted,
    distance_mi, distance_group,
    carrier_delay_min, weather_delay_min, nas_delay_min,
    security_delay_min, late_aircraft_min
)
SELECT
    flight_dt,
    carrier_code,
    carrier_flight_num,
    tail_num,
    origin_code,
    dest_code,
    origin_city_name,
    origin_state,
    dest_city_name,
    dest_state,
    ct  AS crs_dep_time,
    -- Combine FlightDate + scheduled HHMM into a naive local timestamp.
    -- HHMM=2400 naturally rolls to next-day 00:00 via +24h.
    CASE WHEN ct IS NULL THEN NULL
         ELSE flight_dt::timestamp
              + make_interval(hours => ct / 100, mins => mod(ct, 100))
    END AS sched_dep_local,
    dep_delay_num AS dep_delay,
    cat AS crs_arr_time,
    CASE WHEN cat IS NULL THEN NULL
         ELSE flight_dt::timestamp
              + make_interval(hours => cat / 100, mins => mod(cat, 100))
    END AS sched_arr_local,
    arr_delay_num AS arr_delay,
    COALESCE(round(arr_del15_num)::int, 0) AS arr_del15,
    COALESCE(round(cancelled_num)::int, 0) AS cancelled,
    cancellation_code,
    COALESCE(round(diverted_num)::int, 0)  AS diverted,
    distance_num     AS distance_mi,
    distance_grp_num AS distance_group,
    carrier_delay_num,
    weather_delay_num,
    nas_delay_num,
    security_delay_num,
    late_aircraft_num
FROM (
    SELECT
        flight_dt,
        carrier_code,
        NULLIF(btrim(carrier_flight_num), '') AS carrier_flight_num,
        tail_num,
        origin_code,
        dest_code,
        origin_city_name,
        origin_state,
        dest_city_name,
        dest_state,
        cancellation_code,
        NULLIF(btrim(crs_dep_time), '')::int     AS ct,
        NULLIF(btrim(crs_arr_time), '')::int     AS cat,
        NULLIF(btrim(dep_delay), '')::numeric    AS dep_delay_num,
        NULLIF(btrim(arr_delay), '')::numeric    AS arr_delay_num,
        NULLIF(btrim(arr_del15), '')::numeric    AS arr_del15_num,
        NULLIF(btrim(cancelled), '')::numeric    AS cancelled_num,
        NULLIF(btrim(diverted), '')::numeric     AS diverted_num,
        NULLIF(btrim(distance_mi), '')::numeric  AS distance_num,
        NULLIF(btrim(distance_group), '')::numeric::int AS distance_grp_num,
        NULLIF(btrim(carrier_delay_min), '')::numeric   AS carrier_delay_num,
        NULLIF(btrim(weather_delay_min), '')::numeric   AS weather_delay_num,
        NULLIF(btrim(nas_delay_min), '')::numeric       AS nas_delay_num,
        NULLIF(btrim(security_delay_min), '')::numeric  AS security_delay_num,
        NULLIF(btrim(late_aircraft_min), '')::numeric   AS late_aircraft_num,
        ROW_NUMBER() OVER (
            PARTITION BY flight_dt, carrier_code,
                         NULLIF(btrim(carrier_flight_num), ''),
                         origin_code, dest_code,
                         NULLIF(btrim(crs_dep_time), '')::int
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM {ods}.flights_raw
    WHERE flight_dt = %(process_date)s
) d
WHERE rn = 1;
