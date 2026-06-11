-- Chart 3a: per-airline quality for the day. Param: %(process_date)s.

DELETE FROM {dm}.carrier_quality WHERE flight_dt = %(process_date)s;

INSERT INTO {dm}.carrier_quality (
    flight_dt, carrier_code, carrier_name,
    total_flights, cancelled_cnt, delayed_cnt, avg_arr_delay,
    cancel_rate, delay_rate
)
WITH base AS (
    SELECT flight_dt, carrier_code,
           0 AS is_cancelled,
           CASE WHEN arr_delay_min >= 15 THEN 1 ELSE 0 END AS is_delayed,
           arr_delay_min AS arr_delay
    FROM {dds}.fct_flight_completed
    WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, carrier_code, 1, 0, NULL
    FROM {dds}.fct_flight_cancelled
    WHERE flight_dt = %(process_date)s
)
SELECT
    b.flight_dt,
    b.carrier_code,
    c.carrier_name,
    count(*)                   AS total_flights,
    sum(b.is_cancelled)        AS cancelled_cnt,
    sum(b.is_delayed)          AS delayed_cnt,
    round(avg(b.arr_delay), 2) AS avg_arr_delay,
    round(sum(b.is_cancelled)::numeric / nullif(count(*), 0), 4)          AS cancel_rate,
    round(sum(b.is_delayed)::numeric
          / nullif(count(*) - sum(b.is_cancelled), 0), 4)                 AS delay_rate
FROM base b
LEFT JOIN {dds}.dim_carrier c ON c.carrier_code = b.carrier_code
GROUP BY b.flight_dt, b.carrier_code, c.carrier_name;
