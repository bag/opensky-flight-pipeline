# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Streaming Ingestion with Auto Loader
# MAGIC Continuously ingests JSON files from landing zone

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

LANDING_ZONE = "/Volumes/flight_tracking/bronze/landing_zone/raw"
CHECKPOINT_PATH = "/Volumes/flight_tracking/bronze/landing_zone/checkpoints"
BRONZE_TABLE = "flight_tracking.bronze.opensky_states_streaming"


# COMMAND ----------

state_schema = StructType([
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

json_schema = StructType([
    StructField("time", LongType()),
    StructField("states", ArrayType(state_schema))
])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Stream

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .schema(json_schema)
    .load(LANDING_ZONE)
)

# COMMAND ----------

from pyspark.sql.functions import explode, lit

exploded = (
    raw_stream
    .select(
        col("time").alias("_source_timestamp"),
        explode(col("states")).alias("state"),
        col("_metadata.file_path").alias("_source_file")
    )
    .select(
        col("_source_timestamp"),
        col("_source_file"),
        col("state.icao24").alias("icao24"),
        col("state.callsign").alias("callsign"),
        col("state.origin_country").alias("origin_country"),
        col("state.time_position").alias("time_position"),
        col("state.last_contact").alias("last_contact"),
        col("state.longitude").alias("longitude"),
        col("state.latitude").alias("latitude"),
        col("state.baro_altitude").alias("baro_altitude"),
        col("state.on_ground").alias("on_ground"),
        col("state.velocity").alias("velocity"),
        col("state.true_track").alias("true_track"),
        col("state.vertical_rate").alias("vertical_rate"),
        col("state.sensors").alias("sensors"),
        col("state.geo_altitude").alias("geo_altitude"),
        col("state.squawk").alias("squawk"),
        col("state.spi").alias("spi"),
        col("state.position_source").alias("position_source")
    )
    .withColumn("_ingested_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream to Delta

# COMMAND ----------

query = (
    exploded.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)  # Process all available files, then stop
    # For continuous: .trigger(processingTime="1 minute")
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

print(f"Streaming complete. Records in table: {spark.read.table(BRONZE_TABLE).count()}")
