import os

# ============================================================
# Windows local Spark configuration
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["HADOOP_OPTS"] = "-Djava.library.path="

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from config.settings import (
    KAFKA_BROKER,
    KAFKA_TOPIC_PATTERN,
    BRONZE_PATH,
    BRONZE_CHECKPOINT_PATH,
)

# ============================================================
# RetailHub - Kafka -> Bronze Streaming
# ============================================================

APP_NAME = "RetailHub-Kafka-To-Bronze"



# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .master("local[3]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("RetailHub Kafka -> Bronze Streaming")
print("=" * 70)
print(f"Kafka      : {KAFKA_BROKER}")
print(f"Topic      : {KAFKA_TOPIC_PATTERN}")
print(f"Bronze     : {BRONZE_PATH}")
print(f"Checkpoint : {BRONZE_CHECKPOINT_PATH}")
print("=" * 70)


# ============================================================
# Kafka Input
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BROKER
    )
    .option(
        "subscribePattern",
        KAFKA_TOPIC_PATTERN
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)


# ============================================================
# Kafka metadata + COMPLETE original event JSON
# ============================================================

kafka_events = (
    raw_stream
    .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("event_json"),
    )
)


# ============================================================
# Bronze
#
# IMPORTANT:
# We preserve the COMPLETE original JSON.
# No cleaning.
# No deduplication.
# No parsing.
# No filtering of dirty events.
#
# Cleaning happens in Silver.
# ============================================================

bronze_stream = (
    kafka_events
    .withColumn(
        "bronze_ingestion_time",
        current_timestamp()
    )
)


# ============================================================
# Write Bronze as Parquet
# ============================================================

query = (
    bronze_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option(
        "checkpointLocation",
        BRONZE_CHECKPOINT_PATH
    )
    .trigger(processingTime="5 seconds")
    .start()
)


print()
print("Streaming query started.")
print("Waiting for Kafka events...")
print("Press Ctrl+C to stop.")
print()

query.awaitTermination()