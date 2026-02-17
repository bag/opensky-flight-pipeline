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

# Create raw folder if needed
dbutils.fs.mkdirs(LANDING_ZONE)

def fetch_and_write():
    resp = requests.get("https://opensky-network.org/api/states/all")
    resp.raise_for_status()
    data = resp.json()
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{LANDING_ZONE}/opensky_{timestamp}.json"
    
    dbutils.fs.put(path, json.dumps(data), overwrite=True)
    return path, len(data.get("states", []))

path, count = fetch_and_write()
print(f"Wrote {count} records to {path}")

