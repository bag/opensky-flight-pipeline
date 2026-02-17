# Databricks notebook source
# MAGIC %md
# MAGIC # Landing Zone Poller
# MAGIC Fetches OpenSky data and writes JSON to ADLS landing zone

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS flight_tracking.bronze.landing_zone;

# COMMAND ----------

# Databricks notebook source
import requests
import json
from datetime import datetime

LANDING_ZONE = "/Volumes/flight_tracking/bronze/landing_zone/raw"
dbutils.fs.mkdirs(LANDING_ZONE)

FIELD_NAMES = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source"
]

def fetch_and_write():
    resp = requests.get("https://opensky-network.org/api/states/all")
    resp.raise_for_status()
    data = resp.json()
    
    # Transform arrays into objects with named fields
    states = []
    for state in data.get("states", []):
        states.append(dict(zip(FIELD_NAMES, state)))
    
    output = {"time": data["time"], "states": states}
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{LANDING_ZONE}/opensky_{timestamp}.json"
    
    dbutils.fs.put(path, json.dumps(output), overwrite=True)
    return path, len(states)

path, count = fetch_and_write()
print(f"Wrote {count} records to {path}")

