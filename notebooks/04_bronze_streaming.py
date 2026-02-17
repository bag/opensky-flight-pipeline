# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Streaming Ingestion with Auto Loader
# MAGIC Continuously ingests JSON files from landing zone

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, input_file_name, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

LANDING_ZONE = "/Volumes/flight_tracking/bronze/landing_zone/raw"
CHECKPOINT_PATH = "/Volumes/flight_tracking/bronze/landing_zone/checkpoints"
BRONZE_TABLE = "flight_tracking.bronze.opensky_states_streaming"


# COMMAND ----------

state_schema = ArrayType(
    StructType([
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
)

json_schema = StructType([
    StructField("time", LongType()),
    StructField("states", state_schema)
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
        input_file_name().alias("_source_file")
    )
    .select(
        col("_source_timestamp"),
        col("_source_file"),
        col("state")[0].alias("icao24"),
        col("state")[1].alias("callsign"),
        col("state")[2].alias("origin_country"),
        col("state")[3].cast("long").alias("time_position"),
        col("state")[4].cast("long").alias("last_contact"),
        col("state")[5].cast("double").alias("longitude"),
        col("state")[6].cast("double").alias("latitude"),
        col("state")[7].cast("double").alias("baro_altitude"),
        col("state")[8].cast("boolean").alias("on_ground"),
        col("state")[9].cast("double").alias("velocity"),
        col("state")[10].cast("double").alias("true_track"),
        col("state")[11].cast("double").alias("vertical_rate"),
        col("state")[12].alias("sensors"),
        col("state")[13].cast("double").alias("geo_altitude"),
        col("state")[14].alias("squawk"),
        col("state")[15].cast("boolean").alias("spi"),
        col("state")[16].cast("int").alias("position_source")
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
