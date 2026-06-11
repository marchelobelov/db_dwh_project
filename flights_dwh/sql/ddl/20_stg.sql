-- STG: deduplicated + typed flights, cleaned reference data, weather cache.

CREATE TABLE IF NOT EXISTS {stg}.flights (
    flight_dt           date NOT NULL,
    carrier_code        text,
    carrier_flight_num  text,
    tail_num            text,
    origin_code         text,
    dest_code           text,
    origin_city_name    text,
    origin_state        text,
    dest_city_name      text,
    dest_state          text,
    crs_dep_time        int,
    sched_dep_local     timestamp,         -- naive local wall-clock (no tz yet)
    dep_delay           numeric,           -- signed minutes
    crs_arr_time        int,
    sched_arr_local     timestamp,
    arr_delay           numeric,           -- signed minutes
    arr_del15           int,
    cancelled           int,
    cancellation_code   text,
    diverted            int,
    distance_mi         numeric,
    distance_group      int,
    carrier_delay_min   numeric,
    weather_delay_min   numeric,
    nas_delay_min       numeric,
    security_delay_min  numeric,
    late_aircraft_min   numeric,
    _loaded_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_flights_dt ON {stg}.flights (flight_dt);
CREATE INDEX IF NOT EXISTS ix_stg_flights_origin_dt ON {stg}.flights (origin_code, flight_dt);
-- Heal tables created before tail_num was added to the model.
ALTER TABLE {stg}.flights ADD COLUMN IF NOT EXISTS tail_num text;

CREATE TABLE IF NOT EXISTS {stg}.airports (
    airport_dk  text PRIMARY KEY,
    ident       text,
    name        text,
    city        text,
    region      text,
    country     text,
    lat         numeric,
    lon         numeric,
    tz_name     text,
    _loaded_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {stg}.carriers (
    carrier_code text PRIMARY KEY,
    carrier_name text
);

CREATE TABLE IF NOT EXISTS {stg}.weather (
    airport_dk    text NOT NULL,
    ts_hour_local timestamp NOT NULL,      -- naive local hour matching flights
    temp_c        numeric,
    precip_mm     numeric,
    weather_code  int,
    _loaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (airport_dk, ts_hour_local)
);
