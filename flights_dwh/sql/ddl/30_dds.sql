-- DDS (Inmon detailed store): dimensions + two fact tables
-- (successful vs cancelled flights), times as local timestamptz.

CREATE TABLE IF NOT EXISTS {dds}.dim_airport (
    airport_dk text PRIMARY KEY,
    ident      text,
    name       text,
    city       text,
    region     text,        -- iso_region, e.g. US-NY
    country    text,
    lat        numeric,
    lon        numeric,
    tz_name    text
);

CREATE TABLE IF NOT EXISTS {dds}.dim_carrier (
    carrier_code text PRIMARY KEY,
    carrier_name text
);

-- Successful (non-cancelled) flights.
CREATE TABLE IF NOT EXISTS {dds}.fct_flight_completed (
    flight_dt              date NOT NULL,
    carrier_flight_num     text NOT NULL,
    flight_dttm_local      timestamptz NOT NULL,   -- scheduled departure, local
    actual_dep_dttm_local  timestamptz,            -- scheduled + dep_delay
    origin_airport_dk      text NOT NULL,
    dest_airport_dk        text,
    carrier_code           text,
    tail_num               text,
    distance_mi            numeric,
    dep_delay_min          numeric,
    arr_delay_min          numeric,
    arr_del15              int,
    carrier_delay_min      numeric,
    weather_delay_min      numeric,
    nas_delay_min          numeric,
    security_delay_min     numeric,
    late_aircraft_min      numeric,
    origin_temp_c          numeric,
    origin_precip_mm       numeric,
    origin_weather_code    int,
    processed_dttm         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (carrier_flight_num, flight_dttm_local, origin_airport_dk)
);
CREATE INDEX IF NOT EXISTS ix_dds_completed_dt ON {dds}.fct_flight_completed (flight_dt);
CREATE INDEX IF NOT EXISTS ix_dds_completed_origin ON {dds}.fct_flight_completed (origin_airport_dk);
CREATE INDEX IF NOT EXISTS ix_dds_completed_carrier ON {dds}.fct_flight_completed (carrier_code);

-- Cancelled flights.
CREATE TABLE IF NOT EXISTS {dds}.fct_flight_cancelled (
    flight_dt           date NOT NULL,
    carrier_flight_num  text NOT NULL,
    sched_dttm_local    timestamptz NOT NULL,      -- scheduled departure, local
    origin_airport_dk   text NOT NULL,
    dest_airport_dk     text,
    carrier_code        text,
    cancellation_code   text,
    origin_temp_c       numeric,
    origin_precip_mm    numeric,
    origin_weather_code int,
    processed_dttm      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (carrier_flight_num, sched_dttm_local, origin_airport_dk)
);
CREATE INDEX IF NOT EXISTS ix_dds_cancelled_dt ON {dds}.fct_flight_cancelled (flight_dt);
CREATE INDEX IF NOT EXISTS ix_dds_cancelled_origin ON {dds}.fct_flight_cancelled (origin_airport_dk);
CREATE INDEX IF NOT EXISTS ix_dds_cancelled_carrier ON {dds}.fct_flight_cancelled (carrier_code);
