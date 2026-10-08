# ================================================================
# RETAILHUB - GOLD fact_orders
# FINAL HARDENED VERSION
#
# Incremental strategy:
#   - Silver is the source of truth.
#   - Track processed Kafka lineage:
#       topic + partition + offset
#   - New lineage identifies affected order_ids.
#   - Affected orders are rebuilt from ALL Silver lifecycle history.
#   - Existing unaffected Gold orders are retained.
#   - Parquet has no MERGE, so Gold directory is rewritten.
#
# DQ:
#   - Duplicate order_id -> FATAL
#   - Null order_id/order_created_at/order_date -> FATAL
#   - Financial inconsistency -> WARNING
#   - Referential/orphan lifecycle events -> WARNING
#   - Lifecycle chronology problems -> WARNING
#   - Negative lifecycle latency -> WARNING
#   - Incremental rerun with no new lineage -> no-op
# ================================================================

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ================================================================
# CONFIG
# ================================================================

from config.settings import (
    SILVER_PATH as SILVER_BASE_PATH,
    GOLD_BASE_PATH,
)

SILVER_BASE_PATH = SILVER_BASE_PATH.rstrip("/")
GOLD_BASE_PATH = GOLD_BASE_PATH.rstrip("/")
GOLD_PATH = f"{GOLD_BASE_PATH}/fact_orders"
STATE_PATH = f"{GOLD_BASE_PATH}/_state/fact_orders"

LIFECYCLE_FOLDERS = [
    "order_events",
    "payment_events",
    "fulfillment_events",
    "delivery_events",
]

MONEY_TOLERANCE = 0.01


# ================================================================
# SPARK
# ================================================================

spark = (
    SparkSession.builder
    .appName("RetailHub_Gold_FactOrders")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ================================================================
# HELPERS
# ================================================================

def read_domain(folder):
    path = f"{SILVER_BASE_PATH}/{folder}"
    try:
        df = spark.read.parquet(path)
        # Check if df has records
        if df.take(1):
            return df
        return None
    except Exception:
        return None


def safe_count(df):
    return df.count()


# ================================================================
# READ SILVER
# ================================================================

order_events = read_domain("order_events")

if order_events is None:
    print("Primary Silver domain (order_events) missing or empty. Nothing to process.")
    spark.stop()
    raise SystemExit(0)

payment_events = read_domain("payment_events")
if payment_events is None:
    payment_events = spark.createDataFrame([], order_events.schema)

fulfillment_events = read_domain("fulfillment_events")
if fulfillment_events is None:
    fulfillment_events = spark.createDataFrame([], order_events.schema)

delivery_events = read_domain("delivery_events")
if delivery_events is None:
    delivery_events = spark.createDataFrame([], order_events.schema)


# ================================================================
# 1. BUILD COMPLETE LIFECYCLE LINEAGE
# ================================================================

def lineage_df(df):
    return (
        df.select(
            "order_id",
            "event_id",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event_time",
            "event_type",
        )
        .filter(F.col("order_id").isNotNull())
    )


all_events = (
    lineage_df(order_events)
    .unionByName(lineage_df(payment_events))
    .unionByName(lineage_df(fulfillment_events))
    .unionByName(lineage_df(delivery_events))
)


# ================================================================
# 2. READ PROCESSED LINEAGE STATE
# ================================================================

try:
    processed_state = (
        spark.read.parquet(STATE_PATH)
        .select("topic", "partition", "offset")
        .dropDuplicates()
    )
except Exception:
    processed_state = None


# ================================================================
# 3. DETERMINE NEW EVENTS
# ================================================================

if processed_state is None:

    new_events = all_events

    print("No previous fact_orders lineage state.")
    print("Running initial Gold load.")

else:

    new_events = (
        all_events.alias("e")
        .join(
            processed_state.alias("s"),
            (
                (F.col("e.topic") == F.col("s.topic"))
                &
                (F.col("e.partition") == F.col("s.partition"))
                &
                (F.col("e.offset") == F.col("s.offset"))
            ),
            "left_anti",
        )
    )


new_events_count = new_events.count()

affected_order_ids = (
    new_events
    .select("order_id")
    .distinct()
)

affected_order_count = affected_order_ids.count()


print()
print("========================================")
print("FACT ORDERS INCREMENTAL DETECTION")
print("========================================")
print(f"New lifecycle Silver events : {new_events_count}")
print(f"Affected order IDs          : {affected_order_count}")


# ================================================================
# 4. IDEMPOTENT NO-OP
# ================================================================

if affected_order_count == 0:

    print()
    print("No new lifecycle events.")
    print("FACT ORDERS IS ALREADY UP TO DATE.")
    print("Incremental/idempotency check: PASS")

    spark.stop()
    raise SystemExit(0)


# ================================================================
# 5. REBUILD ALL HISTORY FOR AFFECTED ORDERS
# ================================================================

affected_order_events = (
    order_events
    .join(affected_order_ids, "order_id", "inner")
)

affected_payment_events = (
    payment_events
    .join(affected_order_ids, "order_id", "inner")
)

affected_fulfillment_events = (
    fulfillment_events
    .join(affected_order_ids, "order_id", "inner")
)

affected_delivery_events = (
    delivery_events
    .join(affected_order_ids, "order_id", "inner")
)


# ================================================================
# 6. ORDER CREATED = AUTHORITATIVE ORDER ATTRIBUTES
# ================================================================

order_created = (
    affected_order_events
    .filter(F.col("event_type") == "order_created")
)


order_created_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("event_time").asc_nulls_last(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("partition").asc(),
        F.col("offset").asc(),
    )
)


order_anchor = (
    order_created
    .withColumn("_rn", F.row_number().over(order_created_window))
    .filter(F.col("_rn") == 1)
    .select(
        "order_id",
        "customer_id",
        "anonymous_id",
        "country",
        "state",
        "city",
        "shipping_city",
        "shipping_state",
        "shipping_country",
        "currency",
        "subtotal",
        "discount_amount",
        "coupon_discount",
        "tax_amount",
        "shipping_fee",
        "delivery_fee",
        "total_amount",
        "delivery_option",
        "warehouse_id",
        "event_time",
    )
    .withColumnRenamed("event_time", "order_created_at")
    .withColumn(
        "order_value",
        F.col("total_amount")
    )
)


# ================================================================
# 7. PAYMENT SUMMARY
# ================================================================

payment_summary = (
    affected_payment_events
    .groupBy("order_id")
    .agg(

        F.min(
            F.when(
                F.col("event_type") == "payment_success",
                F.col("event_time")
            )
        ).alias("payment_success_at"),

        F.first(
            F.when(
                F.col("event_type") == "payment_success",
                F.col("payment_id")
            ),
            ignorenulls=True
        ).alias("payment_id"),

        F.first(
            F.when(
                F.col("event_type") == "payment_success",
                F.col("payment_method")
            ),
            ignorenulls=True
        ).alias("payment_method"),

        F.first(
            F.when(
                F.col("event_type") == "payment_success",
                F.col("currency")
            ),
            ignorenulls=True
        ).alias("payment_currency"),

        F.first(
            F.when(
                F.col("event_type") == "payment_success",
                F.col("amount")
            ),
            ignorenulls=True
        ).alias("payment_amount"),

        F.sum(
            F.when(
                F.col("event_type") == "payment_success",
                1
            ).otherwise(0)
        ).alias("payment_success_count"),

        F.sum(
            F.when(
                F.col("event_type") == "payment_failed",
                1
            ).otherwise(0)
        ).alias("payment_failed_count"),

        F.sum(
            F.when(
                F.col("event_type") == "payment_retry",
                1
            ).otherwise(0)
        ).alias("payment_retry_count"),
    )
)


# ================================================================
# 8. FULFILLMENT SUMMARY
# ================================================================

fulfillment_summary = (
    affected_fulfillment_events
    .groupBy("order_id")
    .agg(

        F.min(
            F.when(
                F.col("event_type") == "shipment_created",
                F.col("event_time")
            )
        ).alias("shipment_created_at"),

        F.min(
            F.when(
                F.col("event_type") == "order_packed",
                F.col("event_time")
            )
        ).alias("packed_at"),

        F.min(
            F.when(
                F.col("event_type") == "order_shipped",
                F.col("event_time")
            )
        ).alias("shipped_at"),

        F.first(
            "shipment_id",
            ignorenulls=True
        ).alias("shipment_id"),

        F.first(
            "tracking_number",
            ignorenulls=True
        ).alias("tracking_number"),

        F.first(
            "carrier",
            ignorenulls=True
        ).alias("carrier"),
    )
)


# ================================================================
# 9. DELIVERY SUMMARY
# ================================================================

delivery_summary = (
    affected_delivery_events
    .groupBy("order_id")
    .agg(

        F.min(
            F.when(
                F.col("event_type") == "out_for_delivery",
                F.col("event_time")
            )
        ).alias("out_for_delivery_at"),

        F.min(
            F.when(
                F.col("event_type") == "delivered",
                F.col("event_time")
            )
        ).alias("delivered_at"),

        F.min(
            F.when(
                F.col("event_type") == "delivery_failed",
                F.col("event_time")
            )
        ).alias("delivery_failed_at"),
    )
)


# ================================================================
# 10. ORDER CONFIRM / CANCEL
# ================================================================

order_status_times = (
    affected_order_events
    .groupBy("order_id")
    .agg(

        F.min(
            F.when(
                F.col("event_type") == "order_confirmed",
                F.col("event_time")
            )
        ).alias("order_confirmed_at"),

        F.min(
            F.when(
                F.col("event_type") == "order_cancelled",
                F.col("event_time")
            )
        ).alias("cancelled_at"),
    )
)


# ================================================================
# 11. COMPLETE LIFECYCLE TIMELINE
# ================================================================

lifecycle_events = (
    affected_order_events
    .select(
        "order_id",
        "event_time",
        "event_type"
    )
    .unionByName(
        affected_payment_events.select(
            "order_id",
            "event_time",
            "event_type"
        )
    )
    .unionByName(
        affected_fulfillment_events.select(
            "order_id",
            "event_time",
            "event_type"
        )
    )
    .unionByName(
        affected_delivery_events.select(
            "order_id",
            "event_time",
            "event_type"
        )
    )
)


last_event = (
    lifecycle_events
    .groupBy("order_id")
    .agg(
        F.max("event_time")
        .alias("last_order_event_at")
    )
)


first_event = (
    lifecycle_events
    .groupBy("order_id")
    .agg(
        F.min("event_time")
        .alias("first_event_time")
    )
)


# ================================================================
# 12. LATEST ORDER STATUS
# ================================================================

status_events = (
    affected_order_events
    .select(
        "order_id",
        "event_type",
        "event_time",
        "new_status",
        "kafka_timestamp",
        "partition",
        "offset",
    )
    .unionByName(
        affected_fulfillment_events.select(
            "order_id",
            "event_type",
            "event_time",
            "new_status",
            "kafka_timestamp",
            "partition",
            "offset",
        )
    )
    .unionByName(
        affected_delivery_events.select(
            "order_id",
            "event_type",
            "event_time",
            "new_status",
            "kafka_timestamp",
            "partition",
            "offset",
        )
    )
)


status_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("event_time").desc_nulls_last(),
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("partition").desc(),
        F.col("offset").desc(),
    )
)


latest_status = (
    status_events
    .withColumn(
        "_rn",
        F.row_number().over(status_window)
    )
    .filter(F.col("_rn") == 1)
    .select(
        "order_id",
        F.upper(
            F.coalesce(
                F.col("new_status"),
                F.col("event_type")
            )
        ).alias("order_status")
    )
)


# ================================================================
# 13. BUILD UPDATED ORDERS
# ================================================================

updated_orders = (
    order_anchor

    .join(
        order_status_times,
        "order_id",
        "left"
    )

    .join(
        payment_summary,
        "order_id",
        "left"
    )

    .join(
        fulfillment_summary,
        "order_id",
        "left"
    )

    .join(
        delivery_summary,
        "order_id",
        "left"
    )

    .join(
        first_event,
        "order_id",
        "left"
    )

    .join(
        last_event,
        "order_id",
        "left"
    )

    .join(
        latest_status,
        "order_id",
        "left"
    )

    .withColumn(
        "is_paid",
        F.col("payment_success_at").isNotNull()
    )

    .withColumn(
        "is_delivered",
        F.col("delivered_at").isNotNull()
    )

    .withColumn(
        "is_cancelled",
        F.col("cancelled_at").isNotNull()
    )

    .withColumn(
        "order_to_payment_seconds",
        F.when(
            F.col("order_created_at").isNotNull()
            &
            F.col("payment_success_at").isNotNull(),

            F.col("payment_success_at").cast("double")
            -
            F.col("order_created_at").cast("double")
        )
    )

    .withColumn(
        "order_to_ship_seconds",
        F.when(
            F.col("order_created_at").isNotNull()
            &
            F.col("shipped_at").isNotNull(),

            F.col("shipped_at").cast("double")
            -
            F.col("order_created_at").cast("double")
        )
    )

    .withColumn(
        "order_to_delivery_seconds",
        F.when(
            F.col("order_created_at").isNotNull()
            &
            F.col("delivered_at").isNotNull(),

            F.col("delivered_at").cast("double")
            -
            F.col("order_created_at").cast("double")
        )
    )

    .withColumn(
        "order_date",
        F.to_date("order_created_at")
    )
)


# ================================================================
# 14. ORPHAN ORDER DETECTION
# ================================================================

orphan_orders = (
    affected_order_ids
    .join(
        order_anchor.select("order_id"),
        "order_id",
        "left_anti"
    )
)

orphan_count = orphan_orders.count()


if orphan_count > 0:

    print()
    print("WARNING: ORPHAN ORDERS DETECTED")
    print("----------------------------------------")
    print(
        f"Lifecycle order IDs without order_created : "
        f"{orphan_count}"
    )

    orphan_orders.show(100, truncate=False)


# ================================================================
# 15. SELECT FINAL FACT SCHEMA
# ================================================================

updated_orders = updated_orders.select(

    "order_id",

    "customer_id",
    "anonymous_id",

    "order_date",

    "order_created_at",
    "order_confirmed_at",

    "payment_success_at",

    "shipment_created_at",
    "packed_at",
    "shipped_at",

    "out_for_delivery_at",
    "delivered_at",

    "cancelled_at",

    "order_status",

    "is_paid",
    "is_delivered",
    "is_cancelled",

    "payment_id",
    "payment_method",
    "payment_currency",
    "payment_amount",

    "currency",

    "order_value",
    "subtotal",
    "discount_amount",
    "coupon_discount",
    "tax_amount",
    "shipping_fee",
    "delivery_fee",
    "total_amount",

    "payment_success_count",
    "payment_failed_count",
    "payment_retry_count",

    "shipment_id",
    "tracking_number",
    "carrier",

    "delivery_option",
    "warehouse_id",

    "country",
    "state",
    "city",

    "shipping_city",
    "shipping_state",
    "shipping_country",

    "order_to_payment_seconds",
    "order_to_ship_seconds",
    "order_to_delivery_seconds",

    "first_event_time",
    "last_order_event_at",
)


# ================================================================
# 16. READ EXISTING GOLD
# ================================================================

try:
    existing_gold = spark.read.parquet(GOLD_PATH)
    # Materialize to memory to allow safe overwrite of GOLD_PATH without stage read conflicts
    existing_gold.cache()
    if not existing_gold.take(1):
        existing_gold = None
except Exception:
    existing_gold = None


# ================================================================
# 17. INCREMENTAL UPSERT
# ================================================================

if existing_gold is not None:

    unchanged_orders = (
        existing_gold
        .join(
            affected_order_ids,
            "order_id",
            "left_anti"
        )
    )

    final_gold = (
        unchanged_orders
        .unionByName(updated_orders)
    )

else:

    final_gold = updated_orders


# ================================================================
# 18. FATAL DQ CHECKS
# ================================================================

print()
print("========================================")
print("FACT ORDERS DATA QUALITY")
print("========================================")


# ------------------------------------------------
# Duplicate order_id
# ------------------------------------------------

duplicate_orders = (
    final_gold
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_orders.count()


if duplicate_count > 0:

    print(
        f"FATAL: Duplicate order IDs : "
        f"{duplicate_count}"
    )

    duplicate_orders.show(100, truncate=False)

    raise Exception(
        "DATA QUALITY FAILED: duplicate order_id detected"
    )


# ------------------------------------------------
# Null order_id
# ------------------------------------------------

null_order_id_count = (
    final_gold
    .filter(F.col("order_id").isNull())
    .count()
)


if null_order_id_count > 0:

    raise Exception(
        "DATA QUALITY FAILED: NULL order_id detected"
    )


# ------------------------------------------------
# Null order_created_at
# ------------------------------------------------

null_created_count = (
    final_gold
    .filter(F.col("order_created_at").isNull())
    .count()
)


if null_created_count > 0:

    raise Exception(
        "DATA QUALITY FAILED: "
        "order_created_at is NULL"
    )


# ------------------------------------------------
# Null order_date
# ------------------------------------------------

null_order_date_count = (
    final_gold
    .filter(F.col("order_date").isNull())
    .count()
)


if null_order_date_count > 0:

    raise Exception(
        "DATA QUALITY FAILED: order_date is NULL"
    )


# ================================================================
# 19. FINANCIAL DQ
# ================================================================

financial_check = (
    final_gold
    .filter(
        F.col("subtotal").isNotNull()
        &
        F.col("discount_amount").isNotNull()
        &
        F.col("coupon_discount").isNotNull()
        &
        F.col("tax_amount").isNotNull()
        &
        F.col("shipping_fee").isNotNull()
        &
        F.col("delivery_fee").isNotNull()
        &
        F.col("total_amount").isNotNull()
    )
    .withColumn(
        "_expected_total",

        F.col("subtotal")
        -
        F.col("discount_amount")
        -
        F.col("coupon_discount")
        +
        F.col("tax_amount")
        +
        F.col("shipping_fee")
        +
        F.col("delivery_fee")
    )
    .filter(
        F.abs(
            F.col("_expected_total")
            -
            F.col("total_amount")
        )
        >
        F.lit(MONEY_TOLERANCE)
    )
)


financial_error_count = financial_check.count()


if financial_error_count > 0:

    print(
        f"WARNING: Financial inconsistencies : "
        f"{financial_error_count}"
    )

    financial_check.select(
        "order_id",
        "subtotal",
        "discount_amount",
        "coupon_discount",
        "tax_amount",
        "shipping_fee",
        "delivery_fee",
        "total_amount",
        "_expected_total",
    ).show(100, truncate=False)

else:

    print("Financial consistency : PASS")


# ================================================================
# 20. ORDER VALUE VALIDATION
# ================================================================

order_value_errors = (
    final_gold
    .filter(
        F.col("order_value").isNotNull()
        &
        F.col("total_amount").isNotNull()
        &
        (
            F.abs(
                F.col("order_value")
                -
                F.col("total_amount")
            )
            >
            F.lit(MONEY_TOLERANCE)
        )
    )
)

order_value_error_count = order_value_errors.count()


if order_value_error_count > 0:

    print(
        f"WARNING: order_value != total_amount : "
        f"{order_value_error_count}"
    )

else:

    print("order_value validation : PASS")


# ================================================================
# 21. PAYMENT AMOUNT VALIDATION
# ================================================================

payment_amount_errors = (
    final_gold
    .filter(
        F.col("payment_success_at").isNotNull()
        &
        F.col("payment_amount").isNotNull()
        &
        F.col("total_amount").isNotNull()
        &
        (
            F.abs(
                F.col("payment_amount")
                -
                F.col("total_amount")
            )
            >
            F.lit(MONEY_TOLERANCE)
        )
    )
)


payment_amount_error_count = payment_amount_errors.count()


if payment_amount_error_count > 0:

    print(
        f"WARNING: Payment amount mismatch : "
        f"{payment_amount_error_count}"
    )

else:

    print("Payment amount validation : PASS")


# ================================================================
# 22. LIFECYCLE TIMESTAMP VALIDATION
# ================================================================

chronology_checks = (

    (
        F.col("order_confirmed_at").isNotNull()
        &
        (
            F.col("order_confirmed_at")
            <
            F.col("order_created_at")
        )
    )

    |

    (
        F.col("payment_success_at").isNotNull()
        &
        (
            F.col("payment_success_at")
            <
            F.col("order_created_at")
        )
    )

    |

    (
        F.col("shipment_created_at").isNotNull()
        &
        (
            F.col("shipment_created_at")
            <
            F.col("order_created_at")
        )
    )

    |

    (
        F.col("packed_at").isNotNull()
        &
        F.col("shipment_created_at").isNotNull()
        &
        (
            F.col("packed_at")
            <
            F.col("shipment_created_at")
        )
    )

    |

    (
        F.col("shipped_at").isNotNull()
        &
        F.col("packed_at").isNotNull()
        &
        (
            F.col("shipped_at")
            <
            F.col("packed_at")
        )
    )

    |

    (
        F.col("out_for_delivery_at").isNotNull()
        &
        F.col("shipped_at").isNotNull()
        &
        (
            F.col("out_for_delivery_at")
            <
            F.col("shipped_at")
        )
    )

    |

    (
        F.col("delivered_at").isNotNull()
        &
        F.col("out_for_delivery_at").isNotNull()
        &
        (
            F.col("delivered_at")
            <
            F.col("out_for_delivery_at")
        )
    )
)


chronology_errors = (
    final_gold
    .filter(chronology_checks)
)


chronology_error_count = chronology_errors.count()


if chronology_error_count > 0:

    print(
        f"WARNING: Lifecycle chronology anomalies : "
        f"{chronology_error_count}"
    )

    chronology_errors.select(
        "order_id",
        "order_created_at",
        "order_confirmed_at",
        "payment_success_at",
        "shipment_created_at",
        "packed_at",
        "shipped_at",
        "out_for_delivery_at",
        "delivered_at",
    ).show(100, truncate=False)

else:

    print("Lifecycle chronology : PASS")


# ================================================================
# 23. NEGATIVE LATENCY VALIDATION
# ================================================================

negative_latency = (
    final_gold
    .filter(
        (
            F.col("order_to_payment_seconds") < 0
        )
        |
        (
            F.col("order_to_ship_seconds") < 0
        )
        |
        (
            F.col("order_to_delivery_seconds") < 0
        )
    )
)


negative_latency_count = negative_latency.count()


if negative_latency_count > 0:

    print(
        f"WARNING: Negative lifecycle latency : "
        f"{negative_latency_count}"
    )

    negative_latency.select(
        "order_id",
        "order_to_payment_seconds",
        "order_to_ship_seconds",
        "order_to_delivery_seconds",
    ).show(100, truncate=False)

else:

    print("Lifecycle latency : PASS")


# ================================================================
# 24. REFERENTIAL / ORPHAN CHECK
# ================================================================

print()
print("Referential integrity")
print("---------------------")

if orphan_count > 0:

    print(
        f"WARNING: {orphan_count} lifecycle order IDs "
        f"have no order_created event."
    )

    print(
        "These records are excluded from fact_orders."
    )

else:

    print("Orphan order check : PASS")


# ================================================================
# 25. FINAL COUNTS
# ================================================================

total_rows = final_gold.count()

distinct_orders = (
    final_gold
    .select("order_id")
    .distinct()
    .count()
)


if total_rows != distinct_orders:

    raise Exception(
        "DATA QUALITY FAILED: "
        "row count != distinct order count"
    )


print()
print("========================================")
print("FINAL fact_orders DQ SUMMARY")
print("========================================")

print(f"Final Gold rows             : {total_rows}")
print(f"Distinct order IDs          : {distinct_orders}")
print(f"Orphan order IDs            : {orphan_count}")
print(f"Financial warnings          : {financial_error_count}")
print(f"Order value warnings        : {order_value_error_count}")
print(f"Payment amount warnings     : {payment_amount_error_count}")
print(f"Chronology warnings         : {chronology_error_count}")
print(f"Negative latency warnings   : {negative_latency_count}")


# ================================================================
# 26. WRITE GOLD
# ================================================================

(
    final_gold
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)


# ================================================================
# 27. UPDATE LINEAGE STATE
# ================================================================

if processed_state is None:

    new_state = (
        all_events
        .select(
            "topic",
            "partition",
            "offset"
        )
        .dropDuplicates()
    )

else:

    new_state = (
        processed_state
        .unionByName(
            all_events.select(
                "topic",
                "partition",
                "offset"
            )
        )
        .dropDuplicates()
    )


(
    new_state
    .write
    .mode("overwrite")
    .parquet(STATE_PATH)
)


# ================================================================
# 28. COMPLETE
# ================================================================

print()
print("========================================")
print("FACT ORDERS GOLD LOAD COMPLETE")
print("========================================")

print(f"New lifecycle events : {new_events_count}")
print(f"Affected orders      : {affected_order_count}")
print(f"Final Gold orders    : {total_rows}")
print(f"Orphan orders        : {orphan_count}")
print(f"Gold path            : {GOLD_PATH}")
print(f"State path           : {STATE_PATH}")

print()
print("Sample fact_orders:")
print("----------------------------------------")

(
    final_gold
    .orderBy("order_created_at")
    .show(20, truncate=False)
)


spark.stop()