from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

KAFKA_BROKER = "localhost:9092"
TOPIC_PATTERN = "retail_.*_events"

spark = (
    SparkSession.builder
    .appName("RetailHub-FraudGuard-Test")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# Minimal event schema
# ---------------------------------------------------------

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),

    StructField(
        "context",
        StructType([
            StructField("ip_address", StringType(), True),
            StructField("device_id", StringType(), True),
        ]),
        True,
    ),

    StructField(
        "metadata",
        StructType([
            StructField("amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("order_value", DoubleType(), True),
            StructField("forced_test", BooleanType(), True),
        ]),
        True,
    ),
])

# ---------------------------------------------------------
# ONE Kafka streaming source
# ---------------------------------------------------------

kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribePattern", TOPIC_PATTERN)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ---------------------------------------------------------
# Parse events
# ---------------------------------------------------------

events = (
    kafka
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schema
        ).alias("event")
    )
    .select("event.*")
)

# ---------------------------------------------------------
# HIGH VALUE TRANSACTION TEST
#
# One order > ₹50,000 should generate a signal.
# NO windows.
# NO watermark.
# NO stateful aggregation.
# ---------------------------------------------------------

fraud_signals = (
    events
    .filter(
        (F.lower(F.trim(F.col("event_type"))) == "order_created")
        &
        (
            F.coalesce(
                F.col("metadata.order_value"),
                F.col("metadata.total_amount"),
                F.col("metadata.amount"),
                F.lit(0.0),
            )
            > 50000
        )
    )
    .select(
        F.col("event_id"),
        F.col("event_time"),
        F.col("customer_id"),
        F.col("event_type"),
        F.coalesce(
            F.col("metadata.order_value"),
            F.col("metadata.total_amount"),
            F.col("metadata.amount"),
        ).alias("amount"),
        F.lit("HIGH_VALUE_TRANSACTION").alias("fraud_type"),
        F.lit("TRANSACTION").alias("rule_source"),
        F.lit("CRITICAL_TEST").alias("test_mode"),
    )
)

# ---------------------------------------------------------
# Console output
# ---------------------------------------------------------

query = (
    fraud_signals
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "./checkpoints/fraud_test"
    )
    .start()
)

print("=" * 70)
print("RETAILHUB FRAUDGUARD - MINIMAL TEST")
print("=" * 70)
print(f"Kafka Broker : {KAFKA_BROKER}")
print(f"Topic Pattern: {TOPIC_PATTERN}")
print("Rule         : HIGH_VALUE_TRANSACTION")
print("Threshold    : 50000")
print("=" * 70)
print("Waiting for test event...")
print()

query.awaitTermination()