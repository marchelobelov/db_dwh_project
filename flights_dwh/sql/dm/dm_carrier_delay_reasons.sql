-- Chart 3b: delay minutes per airline split by reason (tidy/long format).
-- One row per (day, carrier, reason). Param: %(process_date)s.

DELETE FROM {dm}.carrier_delay_reasons WHERE flight_dt = %(process_date)s;

INSERT INTO {dm}.carrier_delay_reasons (
    flight_dt, carrier_code, carrier_name, reason, total_min, flights_cnt
)
WITH unpivoted AS (
    SELECT flight_dt, carrier_code, 'carrier'       AS reason, carrier_delay_min     AS mins
    FROM {dds}.fct_flight_completed WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, carrier_code, 'weather',       weather_delay_min
    FROM {dds}.fct_flight_completed WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, carrier_code, 'nas',           nas_delay_min
    FROM {dds}.fct_flight_completed WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, carrier_code, 'security',      security_delay_min
    FROM {dds}.fct_flight_completed WHERE flight_dt = %(process_date)s
    UNION ALL
    SELECT flight_dt, carrier_code, 'late_aircraft', late_aircraft_min
    FROM {dds}.fct_flight_completed WHERE flight_dt = %(process_date)s
)
SELECT
    u.flight_dt,
    u.carrier_code,
    c.carrier_name,
    u.reason,
    COALESCE(sum(u.mins), 0)          AS total_min,
    count(*) FILTER (WHERE u.mins > 0) AS flights_cnt
FROM unpivoted u
LEFT JOIN {dds}.dim_carrier c ON c.carrier_code = u.carrier_code
GROUP BY u.flight_dt, u.carrier_code, c.carrier_name, u.reason;
