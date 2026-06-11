-- Build dds.dim_carrier from the seed, then backfill any codes seen in flights
-- but missing from the seed (name falls back to the code).
INSERT INTO {dds}.dim_carrier (carrier_code, carrier_name)
SELECT carrier_code, carrier_name
FROM {stg}.carriers
ON CONFLICT (carrier_code) DO UPDATE SET carrier_name = EXCLUDED.carrier_name;

INSERT INTO {dds}.dim_carrier (carrier_code, carrier_name)
SELECT DISTINCT f.carrier_code, f.carrier_code
FROM {stg}.flights f
LEFT JOIN {dds}.dim_carrier d ON d.carrier_code = f.carrier_code
WHERE f.carrier_code IS NOT NULL
  AND d.carrier_code IS NULL
ON CONFLICT (carrier_code) DO NOTHING;
