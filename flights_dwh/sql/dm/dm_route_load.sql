-- Chart 2: busiest origin->dest pairs for the day (pin origin or dest in DataLens).
-- Param: %(process_date)s.

DELETE FROM {dm}.route_load WHERE flight_dt = %(process_date)s;

INSERT INTO {dm}.route_load (
    flight_dt, origin_dk, dest_dk,
    origin_name, origin_lat, origin_lon,
    dest_name, dest_lat, dest_lon,
    flights_cnt, cancelled_cnt, delayed_cnt
)
WITH base AS (
    SELECT flight_dt, origin_airport_dk AS origin_dk, dest_airport_dk AS dest_dk,
           0 AS is_cancelled,
           CASE WHEN arr_delay_min >= 15 THEN 1 ELSE 0 END AS is_delayed
    FROM {dds}.fct_flight_completed
    WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, origin_airport_dk, dest_airport_dk, 1, 0
    FROM {dds}.fct_flight_cancelled
    WHERE flight_dt = %(process_date)s
)
SELECT
    b.flight_dt,
    b.origin_dk,
    b.dest_dk,
    o.name AS origin_name, o.lat AS origin_lat, o.lon AS origin_lon,
    d.name AS dest_name,   d.lat AS dest_lat,   d.lon AS dest_lon,
    count(*)            AS flights_cnt,
    sum(b.is_cancelled) AS cancelled_cnt,
    sum(b.is_delayed)   AS delayed_cnt
FROM base b
LEFT JOIN {dds}.dim_airport o ON o.airport_dk = b.origin_dk
LEFT JOIN {dds}.dim_airport d ON d.airport_dk = b.dest_dk
WHERE b.dest_dk IS NOT NULL
GROUP BY b.flight_dt, b.origin_dk, b.dest_dk,
         o.name, o.lat, o.lon, d.name, d.lat, d.lon;
