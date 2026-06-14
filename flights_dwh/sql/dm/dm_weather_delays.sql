-- Chart 4: delays / cancellations vs temperature, weather code and origin region.
-- Grain (day, region, temp_bucket, weather_code). Param: %(process_date)s.

DELETE FROM {dm}.weather_delays WHERE flight_dt = %(process_date)s;

INSERT INTO {dm}.weather_delays (
    flight_dt, region, temp_bucket, weather_code,
    total_flights, delayed_cnt, cancelled_cnt, avg_arr_delay
)
WITH base AS (
    SELECT f.flight_dt,
           COALESCE(a.region, 'UNKNOWN')        AS region,
           f.origin_temp_c                      AS temp_c,
           COALESCE(f.origin_weather_code, -1)  AS weather_code,
           0                                    AS is_cancelled,
           CASE WHEN COALESCE(f.arr_delay_min, 0) >= 15 THEN 1 ELSE 0 END AS is_delayed,
           COALESCE(f.arr_delay_min, 0)                      AS arr_delay
    FROM {dds}.fct_flight_completed f
    LEFT JOIN {dds}.dim_airport a ON a.airport_dk = f.origin_airport_dk
    WHERE f.flight_dt = %(process_date)s
    UNION ALL
    SELECT f.flight_dt,
           COALESCE(a.region, 'UNKNOWN'),
           f.origin_temp_c,
           COALESCE(f.origin_weather_code, -1),
           1, 0, NULL
    FROM {dds}.fct_flight_cancelled f
    LEFT JOIN {dds}.dim_airport a ON a.airport_dk = f.origin_airport_dk
    WHERE f.flight_dt = %(process_date)s
)
SELECT
    b.flight_dt,
    b.region,
    CASE
        WHEN b.temp_c IS NULL THEN 'unknown'
        WHEN b.temp_c <  0  THEN '1. <0'
        WHEN b.temp_c < 10  THEN '2. 0-10'
        WHEN b.temp_c < 20  THEN '3. 10-20'
        WHEN b.temp_c < 30  THEN '4. 20-30'
        ELSE '5. 30+'
    END AS temp_bucket,
    b.weather_code,
    count(*)                   AS total_flights,
    sum(b.is_delayed)          AS delayed_cnt,
    sum(b.is_cancelled)        AS cancelled_cnt,
    round(avg(b.arr_delay), 2) AS avg_arr_delay
FROM base b
GROUP BY b.flight_dt, b.region, 3, b.weather_code;
