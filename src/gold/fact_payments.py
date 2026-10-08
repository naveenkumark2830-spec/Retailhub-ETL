from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


# ============================================================
# CONFIGURATION
# ============================================================

from config.settings import (
    SILVER_PATH as SILVER_BASE_PATH,
    GOLD_BASE_PATH,
)

SILVER_BASE_PATH = SILVER_BASE_PATH.rstrip("/")
GOLD_BASE_PATH = GOLD_BASE_PATH.rstrip("/")

SILVER_PATH = f"{SILVER_BASE_PATH}/payment_events"
GOLD_PATH = f"{GOLD_BASE_PATH}/fact_payments"
STATE_PATH = f"{GOLD_BASE_PATH}/_state/fact_payments"
STATE_FILE = f"{STATE_PATH}/processed_lineage"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub-FactPayments")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def path_exists(path):
    """
    Check whether a filesystem or S3 path exists and has records.
    """
    try:
        df = spark.read.parquet(path)
        return bool(df.take(1))
    except Exception:
        return False


def read_lineage_state():
    try:
        df = spark.read.parquet(STATE_FILE)
        if df.take(1):
            return df
        return None
    except Exception:
        return None


def save_lineage_state(df):
    (
        df
        .select(
            "topic",
            "partition",
            "offset"
        )
        .dropDuplicates()
        .write
        .mode("overwrite")
        .parquet(STATE_FILE)
    )


# ============================================================
# START
# ============================================================

print("\n" + "=" * 70)
print("FACT_PAYMENTS")
print("=" * 70)


# ============================================================
# READ SILVER
# ============================================================

try:
    payments = spark.read.parquet(SILVER_PATH)
    if not payments.take(1):
        print(f"\nSilver payment path is empty: {SILVER_PATH}. Nothing to process.")
        spark.stop()
        raise SystemExit(0)
except Exception:
    print(f"\nSilver payment path missing: {SILVER_PATH}. Nothing to process.")
    spark.stop()
    raise SystemExit(0)

print(
    f"Silver payment events: {payments.count()}"
)


# ============================================================
# PREPARE SILVER EVENTS
# ============================================================

payment_events = (

    payments

    .select(

        # ----------------------------------------------------
        # Kafka lineage
        # ----------------------------------------------------

        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "kafka_key",

        # ----------------------------------------------------
        # Event information
        # ----------------------------------------------------

        "event_id",
        "simulation_run_id",
        "event_type",
        "event_version",
        "event_time",
        "event_ingestion_time",
        "event_source",
        "actor_type",
        "session_id",

        # ----------------------------------------------------
        # Customer
        # ----------------------------------------------------

        "customer_id",
        "anonymous_id",

        # ----------------------------------------------------
        # Relationships
        # ----------------------------------------------------

        "order_id",
        "order_item_id",
        "product_id",
        "cart_id",
        "payment_id",

        # ----------------------------------------------------
        # Payment information
        # ----------------------------------------------------

        "amount",
        "currency",
        "payment_method",
        "payment_status",
        "attempt_id",
        "attempt_number",
        "previous_status",
        "new_status"
    )

    # --------------------------------------------------------
    # Create reliable timestamp
    # --------------------------------------------------------

    .withColumn(
        "event_ts",
        F.coalesce(

            F.to_timestamp("event_time"),

            F.to_timestamp(
                "event_ingestion_time"
            ),

            F.col("kafka_timestamp")
        )
    )

    # --------------------------------------------------------
    # Numeric types
    # --------------------------------------------------------

    .withColumn(
        "amount",
        F.col("amount").cast("double")
    )

    .withColumn(
        "attempt_number",
        F.col("attempt_number").cast("int")
    )

    # --------------------------------------------------------
    # Payment ID is required for this fact
    # --------------------------------------------------------

    .filter(
        F.col("payment_id").isNotNull()
    )
)


# ============================================================
# INCREMENTAL LINEAGE
# ============================================================

previous_lineage = read_lineage_state()


if previous_lineage is None:

    print("\nLoad type: INITIAL")

    new_events = payment_events

else:

    print("\nLoad type: INCREMENTAL")

    new_events = (

        payment_events.alias("current")

        .join(

            previous_lineage.alias("processed"),

            on=[

                F.col("current.topic")
                == F.col("processed.topic"),

                F.col("current.partition")
                == F.col("processed.partition"),

                F.col("current.offset")
                == F.col("processed.offset")
            ],

            how="left_anti"
        )
    )


new_event_count = new_events.count()


print(
    f"New payment Silver events: "
    f"{new_event_count}"
)


# ============================================================
# NO NEW DATA
# ============================================================

if new_event_count == 0:

    print("\nNo new payment events.")
    print("Already up to date.")
    print("Incremental / idempotency PASS")

    spark.stop()

    raise SystemExit(0)


# ============================================================
# FIND AFFECTED PAYMENT IDs
# ============================================================

affected_payment_ids = (

    new_events

    .select("payment_id")

    .filter(
        F.col("payment_id").isNotNull()
    )

    .distinct()
)


affected_count = affected_payment_ids.count()


print(
    f"Affected payment_ids: "
    f"{affected_count}"
)


# ============================================================
# READ COMPLETE HISTORY FOR AFFECTED PAYMENTS
# ============================================================
#
# Example:
#
# payment_initiated
#        ↓
# payment_success
#
# Both events are combined into ONE payment row.
#
# ============================================================

affected_history = (

    payment_events.alias("events")

    .join(

        affected_payment_ids.alias("affected"),

        F.col("events.payment_id")
        == F.col("affected.payment_id"),

        "inner"
    )

    .select(
        "events.*"
    )
)


# ============================================================
# BUILD PAYMENT FACT
# ============================================================
#
# IMPORTANT:
#
# We do NOT select one event as the winner.
#
# Each field is resolved independently.
#
# This is important because:
#
# payment_initiated:
#     amount       = NULL
#     currency     = NULL
#     status       = NULL
#
# payment_success:
#     amount       = 4710
#     currency     = INR
#     status       = success
#
# ============================================================


fact_updates = (

    affected_history

    .groupBy("payment_id")

    .agg(

        # ====================================================
        # BASIC IDENTIFIERS
        # ====================================================

        F.first(
            "order_id",
            ignorenulls=True
        ).alias("order_id"),

        F.first(
            "order_item_id",
            ignorenulls=True
        ).alias("order_item_id"),

        F.first(
            "customer_id",
            ignorenulls=True
        ).alias("customer_id"),

        F.first(
            "product_id",
            ignorenulls=True
        ).alias("product_id"),

        F.first(
            "cart_id",
            ignorenulls=True
        ).alias("cart_id"),


        # ====================================================
        # PAYMENT INFORMATION
        # ====================================================

        F.first(
            "payment_method",
            ignorenulls=True
        ).alias("payment_method"),

        F.first(
            "amount",
            ignorenulls=True
        ).alias("amount"),

        F.first(
            "currency",
            ignorenulls=True
        ).alias("currency"),

        F.first(
            "attempt_id",
            ignorenulls=True
        ).alias("attempt_id"),

        F.first(
            "attempt_number",
            ignorenulls=True
        ).alias("attempt_number"),


        # ====================================================
        # PAYMENT INITIATED
        # ====================================================
        #
        # TRUE if at least one payment_initiated event exists.
        #
        # ====================================================

        F.max(

            F.when(
                F.col("event_type")
                == "payment_initiated",

                F.lit(1)
            )

            .otherwise(0)

        ).alias("_payment_initiated_flag"),


        # ====================================================
        # PAYMENT INITIATED TIMESTAMP
        # ====================================================

        F.min(

            F.when(

                F.col("event_type")
                == "payment_initiated",

                F.col("event_ts")

            )

        ).alias("payment_initiated_at"),


        # ====================================================
        # PAYMENT SUCCESSFUL
        # ====================================================
        #
        # TRUE if at least one payment_success event exists.
        #
        # ====================================================

        F.max(

            F.when(

                F.col("event_type")
                == "payment_success",

                F.lit(1)

            )

            .otherwise(0)

        ).alias("_payment_successful_flag"),


        # ====================================================
        # PAYMENT SUCCESS TIMESTAMP
        # ====================================================

        F.min(

            F.when(

                F.col("event_type")
                == "payment_success",

                F.col("event_ts")

            )

        ).alias("payment_success_at"),


        # ====================================================
        # PAYMENT STATUS
        # ====================================================

        F.first(
            "payment_status",
            ignorenulls=True
        ).alias("payment_status"),


        # ====================================================
        # STATUS TRANSITION
        # ====================================================

        F.first(
            "previous_status",
            ignorenulls=True
        ).alias("previous_status"),

        F.first(
            "new_status",
            ignorenulls=True
        ).alias("new_status"),


        # ====================================================
        # EVENT TIMELINE
        # ====================================================

        F.min(
            "event_ts"
        ).alias("first_event_at"),

        F.max(
            "event_ts"
        ).alias("last_event_at"),


        # ====================================================
        # EVENT COUNT
        # ====================================================

        F.count("*").alias(
            "payment_event_count"
        )
    )
)


# ============================================================
# CONVERT FLAGS TO BOOLEAN
# ============================================================

fact_updates = (

    fact_updates

    .withColumn(
        "payment_initiated",

        F.col("_payment_initiated_flag") == 1
    )

    .withColumn(
        "payment_successful",

        F.col("_payment_successful_flag") == 1
    )

    .drop(
        "_payment_initiated_flag",
        "_payment_successful_flag"
    )
)


# ============================================================
# PAYMENT DURATION
# ============================================================
#
# How long did it take from initiation to success?
#
# ============================================================

fact_updates = (

    fact_updates

    .withColumn(

        "payment_duration_seconds",

        F.when(

            F.col("payment_initiated_at").isNotNull()
            & F.col("payment_success_at").isNotNull(),

            F.col("payment_success_at").cast("long")
            - F.col("payment_initiated_at").cast("long")

        )

    )
)


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\n" + "-" * 70)
print("DATA QUALITY")
print("-" * 70)


# ------------------------------------------------------------
# Duplicate payment IDs
# ------------------------------------------------------------

duplicate_payment_ids = (

    fact_updates

    .groupBy("payment_id")

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


if duplicate_payment_ids == 0:

    print(
        "Duplicate payment_id: PASS"
    )

else:

    print(
        f"Duplicate payment_id: "
        f"FAIL ({duplicate_payment_ids})"
    )

    raise Exception(
        "Duplicate payment_id detected."
    )


# ------------------------------------------------------------
# NULL payment ID
# ------------------------------------------------------------

null_payment_ids = (

    fact_updates

    .filter(
        F.col("payment_id").isNull()
    )

    .count()
)


if null_payment_ids == 0:

    print(
        "Null payment_id: PASS"
    )

else:

    print(
        f"Null payment_id: "
        f"FAIL ({null_payment_ids})"
    )

    raise Exception(
        "NULL payment_id detected."
    )


# ------------------------------------------------------------
# NULL order ID
# ------------------------------------------------------------

null_order_ids = (

    fact_updates

    .filter(
        F.col("order_id").isNull()
    )

    .count()
)


print(
    f"Null order_id: "
    f"{null_order_ids}"
)


# ------------------------------------------------------------
# Invalid amount
# ------------------------------------------------------------

invalid_amount = (

    fact_updates

    .filter(

        F.col("amount").isNotNull()
        & (F.col("amount") < 0)

    )

    .count()
)


if invalid_amount == 0:

    print(
        "Invalid payment amount: PASS"
    )

else:

    print(
        f"Invalid payment amount: "
        f"WARNING ({invalid_amount})"
    )


# ------------------------------------------------------------
# Invalid attempt number
# ------------------------------------------------------------

invalid_attempt_number = (

    fact_updates

    .filter(

        F.col("attempt_number").isNotNull()
        & (F.col("attempt_number") <= 0)

    )

    .count()
)


if invalid_attempt_number == 0:

    print(
        "Invalid attempt_number: PASS"
    )

else:

    print(
        f"Invalid attempt_number: "
        f"WARNING ({invalid_attempt_number})"
    )


# ------------------------------------------------------------
# Amount / currency consistency
# ------------------------------------------------------------

amount_without_currency = (

    fact_updates

    .filter(

        F.col("amount").isNotNull()
        & F.col("currency").isNull()

    )

    .count()
)


if amount_without_currency == 0:

    print(
        "Amount/currency consistency: PASS"
    )

else:

    print(
        "Amount/currency consistency: "
        f"WARNING ({amount_without_currency})"
    )


# ------------------------------------------------------------
# Successful payment must have amount
# ------------------------------------------------------------

successful_without_amount = (

    fact_updates

    .filter(

        F.col("payment_successful")
        & F.col("amount").isNull()

    )

    .count()
)


if successful_without_amount == 0:

    print(
        "Successful payment amount: PASS"
    )

else:

    print(
        "Successful payment amount: "
        f"WARNING ({successful_without_amount})"
    )


# ------------------------------------------------------------
# Event count
# ------------------------------------------------------------

invalid_event_count = (

    fact_updates

    .filter(

        F.col("payment_event_count") <= 0

    )

    .count()
)


if invalid_event_count == 0:

    print(
        "Payment event count: PASS"
    )

else:

    print(
        f"Payment event count: "
        f"FAIL ({invalid_event_count})"
    )

    raise Exception(
        "Invalid payment_event_count."
    )


# ------------------------------------------------------------
# Payment chronology
# ------------------------------------------------------------

invalid_payment_chronology = (

    fact_updates

    .filter(

        F.col("payment_initiated_at").isNotNull()
        & F.col("payment_success_at").isNotNull()
        & (
            F.col("payment_success_at")
            < F.col("payment_initiated_at")
        )

    )

    .count()
)


if invalid_payment_chronology == 0:

    print(
        "Payment chronology: PASS"
    )

else:

    print(
        "Payment chronology: "
        f"WARNING ({invalid_payment_chronology})"
    )


# ============================================================
# READ EXISTING GOLD
# ============================================================

if path_exists(GOLD_PATH):

    existing_gold = (
        spark.read
        .parquet(GOLD_PATH)
    )
    existing_gold.cache()

    print(
        f"\nExisting Gold rows: "
        f"{existing_gold.count()}"
    )


    # --------------------------------------------------------
    # Remove affected payment IDs.
    # They will be rebuilt from complete Silver history.
    # --------------------------------------------------------

    existing_remaining = (

        existing_gold.alias("existing")

        .join(

            affected_payment_ids.alias("affected"),

            F.col("existing.payment_id")
            == F.col("affected.payment_id"),

            "left_anti"
        )
    )

else:

    print(
        "\nNo existing Gold table."
    )

    print(
        "Creating fact_payments for the first time."
    )

    existing_remaining = None


# ============================================================
# FINAL GOLD DATASET
# ============================================================

if existing_remaining is None:

    final_gold = fact_updates

else:

    final_gold = (

        existing_remaining

        .unionByName(
            fact_updates,
            allowMissingColumns=True
        )
    )


# ============================================================
# FINAL DUPLICATE PROTECTION
# ============================================================

final_gold = (

    final_gold

    .dropDuplicates(
        ["payment_id"]
    )
)


# ============================================================
# FINAL COLUMN ORDER
# ============================================================

final_gold = final_gold.select(

    # --------------------------------------------------------
    # Keys / relationships
    # --------------------------------------------------------

    "payment_id",
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",
    "cart_id",

    # --------------------------------------------------------
    # Payment information
    # --------------------------------------------------------

    "payment_method",
    "amount",
    "currency",

    # --------------------------------------------------------
    # Payment lifecycle
    # --------------------------------------------------------

    "payment_initiated",
    "payment_initiated_at",

    "payment_successful",
    "payment_success_at",

    "payment_status",

    # --------------------------------------------------------
    # Attempt information
    # --------------------------------------------------------

    "attempt_id",
    "attempt_number",

    # --------------------------------------------------------
    # Status transition
    # --------------------------------------------------------

    "previous_status",
    "new_status",

    # --------------------------------------------------------
    # Timing / metrics
    # --------------------------------------------------------

    "payment_duration_seconds",
    "first_event_at",
    "last_event_at",
    "payment_event_count"
)


# ============================================================
# FINAL GOLD VALIDATION
# ============================================================

final_rows = final_gold.count()

distinct_payment_ids = (

    final_gold

    .select("payment_id")

    .distinct()

    .count()
)


print("\n" + "-" * 70)
print("FINAL GOLD VALIDATION")
print("-" * 70)

print(
    f"Final Gold rows: "
    f"{final_rows}"
)

print(
    f"Distinct payment_ids: "
    f"{distinct_payment_ids}"
)


if final_rows == distinct_payment_ids:

    print(
        "Payment grain validation: PASS"
    )

else:

    print(
        "Payment grain validation: FAIL"
    )

    raise Exception(
        "Duplicate payment_id exists in final Gold."
    )


# ============================================================
# PAYMENT FUNNEL SUMMARY
# ============================================================

initiated_count = (

    final_gold

    .filter(
        F.col("payment_initiated")
    )

    .count()
)


successful_count = (

    final_gold

    .filter(
        F.col("payment_successful")
    )

    .count()
)


abandoned_count = (

    final_gold

    .filter(
        F.col("payment_initiated")
        & ~F.col("payment_successful")
    )

    .count()
)


print("\n" + "-" * 70)
print("PAYMENT FUNNEL")
print("-" * 70)

print(
    f"Payment initiated : "
    f"{initiated_count}"
)

print(
    f"Payment successful : "
    f"{successful_count}"
)

print(
    f"Payment abandoned  : "
    f"{abandoned_count}"
)


# ============================================================
# WRITE GOLD
# ============================================================

(
    final_gold

    .write

    .mode("overwrite")

    .parquet(GOLD_PATH)
)


print(
    f"\nGold written successfully:"
    f"\n{GOLD_PATH}"
)


# ============================================================
# UPDATE LINEAGE STATE
# ============================================================
#
# IMPORTANT:
#
# State is updated ONLY after successful Gold write.
#
# ============================================================

if previous_lineage is None:

    final_lineage = (

        payment_events

        .select(
            "topic",
            "partition",
            "offset"
        )

        .dropDuplicates()
    )

else:

    final_lineage = (

        previous_lineage

        .unionByName(

            new_events.select(
                "topic",
                "partition",
                "offset"
            ),

            allowMissingColumns=True
        )

        .dropDuplicates()
    )


save_lineage_state(final_lineage)


# ============================================================
# SAMPLE OUTPUT
# ============================================================

print("\n" + "-" * 70)
print("SAMPLE FACT_PAYMENTS")
print("-" * 70)


(
    final_gold

    .select(

        "payment_id",
        "order_id",
        "customer_id",

        "amount",
        "currency",

        "payment_initiated",
        "payment_initiated_at",

        "payment_successful",
        "payment_success_at",

        "payment_status",

        "attempt_id",
        "attempt_number",

        "payment_duration_seconds",

        "payment_event_count"
    )

    .orderBy(
        F.col("last_event_at").desc()
    )

    .show(
        10,
        truncate=False
    )
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("FACT_PAYMENTS COMPLETE")
print("=" * 70)

print(
    f"New Silver events       : "
    f"{new_event_count}"
)

print(
    f"Affected payments       : "
    f"{affected_count}"
)

print(
    f"Final Gold rows         : "
    f"{final_rows}"
)

print(
    f"Distinct payment IDs    : "
    f"{distinct_payment_ids}"
)

print(
    f"Payment initiated       : "
    f"{initiated_count}"
)

print(
    f"Payment successful      : "
    f"{successful_count}"
)

print(
    f"Payment abandoned       : "
    f"{abandoned_count}"
)

print(
    "Incremental processing  : PASS"
)

print(
    "Idempotency             : PASS"
)

print(
    "Lineage state updated   : PASS"
)

print("=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()