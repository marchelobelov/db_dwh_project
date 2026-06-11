-- DM: one mart per dashboard chart. Grain is (flight_dt x dimension) so DataLens
-- can aggregate over any selected date range.

-- Chart 1 — geomap: quality per airport (departures).
CREATE TABLE IF NOT EXISTS {dm}.airport_quality (
    flight_dt      date NOT NULL,
    airport_dk     text NOT NULL,
    airport_name   text,
    region         text,
    lat            numeric,
    lon            numeric,
    total_flights  bigint,
    cancelled_cnt  bigint,
    delayed_cnt    bigint,
    avg_arr_delay  numeric,
    cancel_rate    numeric,
    delay_rate     numeric,
    PRIMARY KEY (flight_dt, airport_dk)
);
CREATE INDEX IF NOT EXISTS ix_dm_airport_quality_dt ON {dm}.airport_quality (flight_dt);

-- Chart 2 — busiest airport pairs (pin origin or dest in DataLens).
CREATE TABLE IF NOT EXISTS {dm}.route_load (
    flight_dt      date NOT NULL,
    origin_dk      text NOT NULL,
    dest_dk        text NOT NULL,
    origin_name    text,
    origin_lat     numeric,
    origin_lon     numeric,
    dest_name      text,
    dest_lat       numeric,
    dest_lon       numeric,
    flights_cnt    bigint,
    cancelled_cnt  bigint,
    delayed_cnt    bigint,
    PRIMARY KEY (flight_dt, origin_dk, dest_dk)
);
CREATE INDEX IF NOT EXISTS ix_dm_route_load_dt ON {dm}.route_load (flight_dt);

-- Chart 3a — quality per airline.
CREATE TABLE IF NOT EXISTS {dm}.carrier_quality (
    flight_dt      date NOT NULL,
    carrier_code   text NOT NULL,
    carrier_name   text,
    total_flights  bigint,
    cancelled_cnt  bigint,
    delayed_cnt    bigint,
    avg_arr_delay  numeric,
    cancel_rate    numeric,
    delay_rate     numeric,
    PRIMARY KEY (flight_dt, carrier_code)
);
CREATE INDEX IF NOT EXISTS ix_dm_carrier_quality_dt ON {dm}.carrier_quality (flight_dt);

-- Chart 3b — delay minutes per airline, split by reason (tidy/long format).
CREATE TABLE IF NOT EXISTS {dm}.carrier_delay_reasons (
    flight_dt     date NOT NULL,
    carrier_code  text NOT NULL,
    carrier_name  text,
    reason        text NOT NULL,      -- carrier|weather|nas|security|late_aircraft
    total_min     numeric,
    flights_cnt   bigint,
    PRIMARY KEY (flight_dt, carrier_code, reason)
);
CREATE INDEX IF NOT EXISTS ix_dm_carrier_reasons_dt ON {dm}.carrier_delay_reasons (flight_dt);

-- Chart 3c — cancellations per airline, split by cause code (tidy/long format).
CREATE TABLE IF NOT EXISTS {dm}.carrier_cancellations (
    flight_dt          date NOT NULL,
    carrier_code       text NOT NULL,
    carrier_name       text,
    cancellation_code  text NOT NULL,    -- A|B|C|D (raw)
    cancellation_reason text,            -- Carrier|Weather|NAS|Security
    cancelled_cnt      bigint,
    PRIMARY KEY (flight_dt, carrier_code, cancellation_code)
);
CREATE INDEX IF NOT EXISTS ix_dm_carrier_cancel_dt ON {dm}.carrier_cancellations (flight_dt);

-- Chart 4 — delays/cancellations vs temperature / weather / region.
CREATE TABLE IF NOT EXISTS {dm}.weather_delays (
    flight_dt      date NOT NULL,
    region         text NOT NULL,
    temp_bucket    text NOT NULL,
    weather_code   int  NOT NULL,
    total_flights  bigint,
    delayed_cnt    bigint,
    cancelled_cnt  bigint,
    avg_arr_delay  numeric,
    PRIMARY KEY (flight_dt, region, temp_bucket, weather_code)
);
CREATE INDEX IF NOT EXISTS ix_dm_weather_delays_dt ON {dm}.weather_delays (flight_dt);
