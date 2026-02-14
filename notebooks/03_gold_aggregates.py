# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Flight Analytics
# MAGIC Aggregated, analytics-ready tables

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, avg, max, min, sum,
    countDistinct, round as spark_round,
    current_timestamp
)

# COMMAND ----------

SILVER_TABLE = "flight_tracking.silver.flight_states"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS flight_tracking.gold;

# COMMAND ----------

silver_df = spark.read.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flights by Country

# COMMAND ----------

flights_by_country = silver_df.filter(col("_has_position")) \
    .groupBy("origin_country", "flight_date") \
    .agg(
        countDistinct("icao24").alias("unique_aircraft"),
        count("*").alias("position_reports"),
        spark_round(avg("speed_kmh"), 1).alias("avg_speed_kmh"),
        spark_round(avg("baro_altitude"), 0).alias("avg_altitude_m")
    ) \
    .orderBy(col("unique_aircraft").desc())

flights_by_country.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("flight_tracking.gold.flights_by_country")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Altitude Distribution

# COMMAND ----------

altitude_distribution = silver_df.filter(col("_has_position")) \
    .groupBy("flight_date", "altitude_category") \
    .agg(
        countDistinct("icao24").alias("unique_aircraft"),
        count("*").alias("position_reports"),
        spark_round(avg("speed_kmh"), 1).alias("avg_speed_kmh")
    )

altitude_distribution.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("flight_tracking.gold.altitude_distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hourly Traffic Summary

# COMMAND ----------

from pyspark.sql.functions import hour, to_timestamp

hourly_traffic = silver_df.filter(col("_has_position")) \
    .withColumn("hour", hour(to_timestamp(col("_source_timestamp")))) \
    .groupBy("flight_date", "hour") \
    .agg(
        countDistinct("icao24").alias("unique_aircraft"),
        sum(when(col("on_ground") == True, 1).otherwise(0)).alias("on_ground"),
        sum(when(col("is_ascending") == True, 1).otherwise(0)).alias("ascending"),
        sum(when(col("is_ascending") == False, 1).otherwise(0)).alias("descending")
    ) \
    .orderBy("flight_date", "hour")

hourly_traffic.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("flight_tracking.gold.hourly_traffic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Stats

# COMMAND ----------

print("=== Gold Layer Summary ===")
for table in ["flights_by_country", "altitude_distribution", "hourly_traffic"]:
    cnt = spark.read.table(f"flight_tracking.gold.{table}").count()
    print(f"{table}: {cnt} rows")
