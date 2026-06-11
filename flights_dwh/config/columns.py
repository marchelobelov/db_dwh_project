"""Source (BTS On-Time Performance CSV) -> ODS column mapping.

Only the columns required by our analytical task are carried into the model.
ODS keeps everything as raw text (no casting); typing happens in STG.

The source CSV uses PascalCase headers; case may vary on the server, so the
extractor matches case-insensitively and tolerates missing columns.
"""
from collections import OrderedDict

# source_header -> ods_column
SOURCE_TO_ODS = OrderedDict([
    ("FlightDate", "flight_date"),
    ("Reporting_Airline", "carrier_code"),
    ("Tail_Number", "tail_num"),
    ("Flight_Number_Reporting_Airline", "carrier_flight_num"),
    ("Origin", "origin_code"),
    ("OriginCityName", "origin_city_name"),
    ("OriginState", "origin_state"),
    ("Dest", "dest_code"),
    ("DestCityName", "dest_city_name"),
    ("DestState", "dest_state"),
    ("CRSDepTime", "crs_dep_time"),
    ("DepDelay", "dep_delay"),
    ("DepDel15", "dep_del15"),
    ("CRSArrTime", "crs_arr_time"),
    ("ArrDelay", "arr_delay"),
    ("ArrDel15", "arr_del15"),
    ("Cancelled", "cancelled"),
    ("CancellationCode", "cancellation_code"),
    ("Diverted", "diverted"),
    ("Distance", "distance_mi"),
    ("DistanceGroup", "distance_group"),
    ("CarrierDelay", "carrier_delay_min"),
    ("WeatherDelay", "weather_delay_min"),
    ("NASDelay", "nas_delay_min"),
    ("SecurityDelay", "security_delay_min"),
    ("LateAircraftDelay", "late_aircraft_min"),
])

SOURCE_COLUMNS = list(SOURCE_TO_ODS.keys())
ODS_COLUMNS = list(SOURCE_TO_ODS.values())          # raw text columns, in load order

# The column FlightDate is also parsed into the partition key `flight_dt` (date).
SOURCE_DATE_COLUMN = "FlightDate"

# Columns physically written by COPY into ods.flights_raw (in order).
ODS_LOAD_COLUMNS = ["flight_dt"] + ODS_COLUMNS + ["source_file"]
