-- Create the four layer schemas (idempotent). Names come from config (tag=mblv).
CREATE SCHEMA IF NOT EXISTS {ods};
CREATE SCHEMA IF NOT EXISTS {stg};
CREATE SCHEMA IF NOT EXISTS {dds};
CREATE SCHEMA IF NOT EXISTS {dm};
