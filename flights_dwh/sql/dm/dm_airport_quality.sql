-- Chart 1 (geomap): per-airport quality for the day. Counts departures from the
-- origin airport across both fact tables. Param: %(process_date)s.

DELETE FROM {dm}.airport_quality WHERE flight_dt = %(process_date)s;

INSERT INTO {dm}.airport_quality (
    flight_dt, airport_dk, airport_name, region, lat, lon,
    total_flights, cancelled_cnt, delayed_cnt, avg_arr_delay,
    cancel_rate, delay_rate
)
WITH base AS (
    SELECT flight_dt, origin_airport_dk AS airport_dk,
           0 AS is_cancelled,
           CASE WHEN arr_delay_min >= 15 THEN 1 ELSE 0 END AS is_delayed,
           arr_delay_min AS arr_delay
    FROM {dds}.fct_flight_completed
    WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, origin_airport_dk, 1, 0, NULL
    FROM {dds}.fct_flight_cancelled
    WHERE flight_dt = %(process_date)s
)
SELECT
    b.flight_dt,
    b.airport_dk,
    a.name   AS airport_name,
    a.region,
    a.lat,
    a.lon,
    count(*)                              AS total_flights,
    sum(b.is_cancelled)                   AS cancelled_cnt,
    sum(b.is_delayed)                     AS delayed_cnt,
    round(avg(b.arr_delay), 2)            AS avg_arr_delay,
    round(sum(b.is_cancelled)::numeric / nullif(count(*), 0), 4)             AS cancel_rate,
    round(sum(b.is_delayed)::numeric
          / nullif(count(*) - sum(b.is_cancelled), 0), 4)                    AS delay_rate
FROM base b
LEFT JOIN {dds}.dim_airport a ON a.airport_dk = b.airport_dk
GROUP BY b.flight_dt, b.airport_dk, a.name, a.region, a.lat, a.lon;
