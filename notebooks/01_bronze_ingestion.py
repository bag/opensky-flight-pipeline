# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: OpenSky Flight State Ingestion
# MAGIC Raw ADS-B data from OpenSky Network API

# COMMAND ----------

import requests
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS flight_tracking;
# MAGIC CREATE SCHEMA IF NOT EXISTS flight_tracking.bronze;

# COMMAND ----------

BRONZE_TABLE = "flight_tracking.bronze.opensky_states"

SCHEMA = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("sensors", StringType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", IntegerType())
])

# COMMAND ----------

def fetch_opensky():
    resp = requests.get("https://opensky-network.org/api/states/all")
    resp.raise_for_status()
    return resp.json()

def ingest():
    data = fetch_opensky()
    if not data.get("states"):
        print("No states returned")
        return 0
    
    df = spark.createDataFrame(data["states"], schema=SCHEMA)
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_timestamp", lit(data["time"]))
    
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    return df.count()

# COMMAND ----------

count = ingest()
print(f"Ingested {count} records to {BRONZE_TABLE}")
