from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

from config.settings import (
    KAFKA_BROKER,
    FRAUD_TOPIC_PATTERN,
    FRAUD_CHECKPOINT_PATH,
)

KAFKA_BOOTSTRAP = KAFKA_BROKER
TOPIC_PATTERN = FRAUD_TOPIC_PATTERN
CHECKPOINT = FRAUD_CHECKPOINT_PATH

DDOS_EVENT_THRESHOLD = 30
SCRAPER_EVENT_THRESHOLD = 8
SCRAPER_SESSION_THRESHOLD = 4

spark = (
    SparkSession.builder
    .appName("RetailHub-RealTime-Fraud")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("RETAILHUB REAL-TIME FRAUD DETECTOR")
print("=" * 70)
print(f"DDoS threshold              : > {DDOS_EVENT_THRESHOLD} events / 10 sec")
print(f"Scraper event threshold     : >= {SCRAPER_EVENT_THRESHOLD} events / 10 sec")
print(f"Scraper session threshold   : >= {SCRAPER_SESSION_THRESHOLD} sessions / 10 sec")
print("Malformed timestamps        : safely ignored")
print("Ground-truth fields ignored : YES")
print("Exact distinct sessions     : YES")
print(f"Checkpoint                  : {CHECKPOINT}")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("context", StructType([
        StructField("ip_address", StringType(), True),
        StructField("device_id", StringType(), True),
    ]), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribePattern", TOPIC_PATTERN)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = raw.select(
    "topic", "partition", "offset", "timestamp",
    F.from_json(F.col("value").cast("string"), event_schema).alias("e"),
)

# Spark 4.2 ANSI-safe timestamp parsing:
# malformed values such as INVALID_TIMESTAMP_2026 become NULL.
events = parsed.select(
    "topic", "partition", "offset", "timestamp",
    F.col("e.event_id").alias("event_id"),
    F.expr("try_to_timestamp(e.event_time)").alias("event_time"),
    F.col("e.session_id").alias("session_id"),
    F.col("e.customer_id").alias("customer_id"),
    F.col("e.event_type").alias("event_type"),
    F.col("e.context.ip_address").alias("ip_address"),
    F.col("e.context.device_id").alias("device_id"),
)

# Malformed timestamps cannot participate in event-time windows.
valid_events = events.filter(
    F.col("event_time").isNotNull() &
    F.col("ip_address").isNotNull()
)

windowed = (
    valid_events
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        F.window("event_time", "10 seconds", "5 seconds"),
        F.col("ip_address"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.size(
            F.collect_set(
                F.when(F.col("session_id").isNotNull(), F.col("session_id"))
            )
        ).alias("distinct_session_count"),
        F.size(
            F.collect_set(
                F.when(F.col("device_id").isNotNull(), F.col("device_id"))
            )
        ).alias("distinct_device_count"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    )
)

alerts = (
    windowed
    .withColumn(
        "fraud_type",
        F.when(
            F.col("event_count") > DDOS_EVENT_THRESHOLD,
            F.lit("DDOS"),
        ).when(
            (F.col("event_count") >= SCRAPER_EVENT_THRESHOLD) &
            (F.col("distinct_session_count") >= SCRAPER_SESSION_THRESHOLD),
            F.lit("COOKIE_CLEARING_SCRAPER"),
        )
    )
    .filter(F.col("fraud_type").isNotNull())
    .withColumn("alert_time", F.current_timestamp())
    .select(
        "window", "ip_address", "event_count",
        "distinct_session_count", "distinct_device_count",
        "first_event_time", "last_event_time",
        "fraud_type", "alert_time",
    )
)

query = (
    alerts.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 100)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping fraud detector...")
    query.stop()
    spark.stop()
