# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Flight State Transformations
# MAGIC Deduplicated, quality-checked, enriched flight data

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, abs as spark_abs, lag, lead,
    to_timestamp, date_format, row_number, expr
)
from pyspark.sql.window import Window

# COMMAND ----------

BRONZE_TABLE = "flight_tracking.bronze.opensky_states"
SILVER_TABLE = "flight_tracking.silver.flight_states"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS flight_tracking.silver;

# COMMAND ----------

def transform_to_silver():
    bronze_df = spark.read.table(BRONZE_TABLE)
    
    # Dedupe: keep latest record per aircraft per source timestamp
    window = Window.partitionBy("icao24", "_source_timestamp").orderBy(col("_ingested_at").desc())
    deduped = bronze_df.withColumn("_row_num", row_number().over(window)) \
                       .filter(col("_row_num") == 1) \
                       .drop("_row_num")
    
    # Data quality flags
    silver_df = deduped.withColumn(
        "_has_position", 
        col("latitude").isNotNull() & col("longitude").isNotNull()
    ).withColumn(
        "_has_velocity",
        col("velocity").isNotNull()
    ).withColumn(
        "_is_valid_altitude",
        (col("baro_altitude").isNull()) | (col("baro_altitude").between(-500, 60000))
    )
    
    # Derived fields
    silver_df = silver_df.withColumn(
        "speed_kmh", 
        col("velocity") * 3.6
    ).withColumn(
        "is_ascending",
        when(col("vertical_rate") > 0, True)
        .when(col("vertical_rate") < 0, False)
        .otherwise(None)
    ).withColumn(
        "flight_date",
        date_format(to_timestamp(col("_source_timestamp")), "yyyy-MM-dd")
    ).withColumn(
        "altitude_category",
        when(col("on_ground") == True, "ground")
        .when(col("baro_altitude") < 3000, "low")
        .when(col("baro_altitude") < 10000, "mid")
        .otherwise("high")
    )
    
    # Write to Silver
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_TABLE)
    
    return silver_df.count()

# COMMAND ----------

count = transform_to_silver()
print(f"Wrote {count} records to {SILVER_TABLE}")
