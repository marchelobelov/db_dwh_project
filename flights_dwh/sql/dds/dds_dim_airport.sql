-- stg.airports -> dds.dim_airport (full upsert; the reference is small).
INSERT INTO {dds}.dim_airport
    (airport_dk, ident, name, city, region, country, lat, lon, tz_name)
SELECT airport_dk, ident, name, city, region, country, lat, lon, tz_name
FROM {stg}.airports
ON CONFLICT (airport_dk) DO UPDATE SET
    ident   = EXCLUDED.ident,
    name    = EXCLUDED.name,
    city    = EXCLUDED.city,
    region  = EXCLUDED.region,
    country = EXCLUDED.country,
    lat     = EXCLUDED.lat,
    lon     = EXCLUDED.lon,
    tz_name = EXCLUDED.tz_name;
