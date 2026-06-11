-- ODS: raw rewrite of S3 source, only needed columns, everything as text.
CREATE TABLE IF NOT EXISTS {ods}.flights_raw (
    flight_dt           date    NOT NULL,
    flight_date         text,
    carrier_code        text,
    tail_num            text,
    carrier_flight_num  text,
    origin_code         text,
    origin_city_name    text,
    origin_state        text,
    dest_code           text,
    dest_city_name      text,
    dest_state          text,
    crs_dep_time        text,
    dep_delay           text,
    dep_del15           text,
    crs_arr_time        text,
    arr_delay           text,
    arr_del15           text,
    cancelled           text,
    cancellation_code   text,
    diverted            text,
    distance_mi         text,
    distance_group      text,
    carrier_delay_min   text,
    weather_delay_min   text,
    nas_delay_min       text,
    security_delay_min  text,
    late_aircraft_min   text,
    source_file         text,
    _loaded_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ods_flights_raw_dt ON {ods}.flights_raw (flight_dt);

-- Raw ourairports reference (selected columns, as text).
CREATE TABLE IF NOT EXISTS {ods}.airports_raw (
    id            text,
    ident         text,
    type          text,
    name          text,
    latitude_deg  text,
    longitude_deg text,
    iso_country   text,
    iso_region    text,
    municipality  text,
    iata_code     text,
    local_code    text,
    gps_code      text,
    _loaded_at    timestamptz NOT NULL DEFAULT now()
);
