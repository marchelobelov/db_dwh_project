-- Chart 3c: cancellations per airline split by cause code (tidy/long format).
-- BTS codes: A=Carrier, B=Weather, C=NAS, D=Security. Param: %(process_date)s.

DELETE FROM {dm}.carrier_cancellations WHERE flight_dt = %(process_date)s;

INSERT INTO {dm}.carrier_cancellations (
    flight_dt, carrier_code, carrier_name,
    cancellation_code, cancellation_reason, cancelled_cnt
)
SELECT
    x.flight_dt,
    x.carrier_code,
    c.carrier_name,
    x.cancellation_code,
    CASE x.cancellation_code
        WHEN 'A' THEN 'Carrier'
        WHEN 'B' THEN 'Weather'
        WHEN 'C' THEN 'NAS'
        WHEN 'D' THEN 'Security'
        ELSE 'Unknown'
    END AS cancellation_reason,
    count(*) AS cancelled_cnt
FROM (
    SELECT flight_dt, carrier_code,
           COALESCE(NULLIF(btrim(cancellation_code), ''), 'U') AS cancellation_code
    FROM {dds}.fct_flight_cancelled
    WHERE flight_dt = %(process_date)s
) x
LEFT JOIN {dds}.dim_carrier c ON c.carrier_code = x.carrier_code
GROUP BY x.flight_dt, x.carrier_code, c.carrier_name, x.cancellation_code;
