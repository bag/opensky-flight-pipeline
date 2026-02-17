# Databricks notebook source
# MAGIC %md
# MAGIC # Landing Zone Poller
# MAGIC Fetches OpenSky data and writes JSON to ADLS landing zone

# COMMAND ----------

import requests
import json
from datetime import datetime

# COMMAND ----------

STORAGE_ACCOUNT = "openskylandingzone"
CONTAINER = "opensky-landing"
LANDING_ZONE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    dbutils.secrets.get(scope="opensky", key="storage-key")
)

# COMMAND ----------

def fetch_and_write():
    resp = requests.get("https://opensky-network.org/api/states/all")
    resp.raise_for_status()
    data = resp.json()
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{LANDING_ZONE}/opensky_{timestamp}.json"
    
    dbutils.fs.put(path, json.dumps(data), overwrite=True)
    
    return path, len(data.get("states", []))

# COMMAND ----------

path, count = fetch_and_write()
print(f"Wrote {count} records to {path}")
