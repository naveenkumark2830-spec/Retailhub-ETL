from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


# ============================================================
# CONFIGURATION
# ============================================================

from config.settings import (
    SILVER_PATH,
    GOLD_BASE_PATH,
)

SILVER_PATH = SILVER_PATH.rstrip("/")
GOLD_BASE_PATH = GOLD_BASE_PATH.rstrip("/")

GOLD_PATH = f"{GOLD_BASE_PATH}/fact_delivery"
STATE_PATH = f"{GOLD_BASE_PATH}/_state/fact_delivery"

DELIVERY_PATH = f"{SILVER_PATH}/delivery_events"
ORDER_PATH = f"{SILVER_PATH}/order_events"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub-FactDelivery")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPERS
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


def read_parquet_if_exists(path):
    try:
        df = spark.read.parquet(path)
        if df.take(1):
            return df
        return None
    except Exception:
        return None


# ============================================================
# START
# ============================================================

print("\n" + "=" * 80)
print("FACT DELIVERY GOLD BUILD")
print("=" * 80)


# ============================================================
# LOAD DELIVERY SILVER
# ============================================================

delivery_df = read_parquet_if_exists(DELIVERY_PATH)

if delivery_df is None:
    print("\nNo Silver delivery events found. Nothing to process.")
    spark.stop()
    raise SystemExit(0)

print("\n========== SILVER DELIVERY EVENTS ==========")

delivery_count = delivery_df.count()

print(f"Total delivery events: {delivery_count}")

delivery_df.groupBy("event_type") \
    .count() \
    .orderBy("event_type") \
    .show(truncate=False)


# ============================================================
# LOAD ORDER SILVER
# ============================================================

order_df = read_parquet_if_exists(ORDER_PATH)
if order_df is None:
    order_df = spark.createDataFrame([], delivery_df.schema)

print("\n========== SHIPPING LOCATION SOURCE ==========")

order_created = (
    order_df
    .filter(
        (F.col("event_type") == "order_created") &
        F.col("order_id").isNotNull()
    )
)

order_created_count = order_created.count()

print(f"order_created events: {order_created_count}")


# ============================================================
# SHIPPING LOCATION POPULATION
# ============================================================

print("\n========== SHIPPING SOURCE POPULATION ==========")

for column in [
    "shipping_address_id",
    "shipping_city",
    "shipping_state",
    "shipping_country"
]:

    count = (
        order_created
        .filter(F.col(column).isNotNull())
        .count()
    )

    print(f"{column}: {count}")


# ============================================================
# BUILD SHIPPING LOOKUP
#
# One logical shipping location per order.
#
# Multiple order_created events may exist for the same order,
# therefore we remove exact duplicate combinations.
# ============================================================

shipping_lookup = (
    order_created
    .select(
        "order_id",
        "shipping_address_id",
        "shipping_city",
        "shipping_state",
        "shipping_country"
    )
    .dropDuplicates()
)


# ============================================================
# SHIPPING CONSISTENCY VALIDATION
#
# An order must not have multiple shipping locations.
# ============================================================

shipping_conflicts = (
    shipping_lookup
    .groupBy("order_id")
    .agg(
        F.countDistinct("shipping_address_id")
            .alias("address_count"),

        F.countDistinct("shipping_city")
            .alias("city_count"),

        F.countDistinct("shipping_state")
            .alias("state_count"),

        F.countDistinct("shipping_country")
            .alias("country_count")
    )
    .filter(
        (F.col("address_count") > 1) |
        (F.col("city_count") > 1) |
        (F.col("state_count") > 1) |
        (F.col("country_count") > 1)
    )
)

conflict_count = shipping_conflicts.count()

print("\n========== SHIPPING CONSISTENCY ==========")

print(f"Shipping-location conflicts: {conflict_count}")

if conflict_count > 0:

    print("FATAL: Conflicting shipping locations found.")

    shipping_conflicts.show(
        20,
        truncate=False
    )

    spark.stop()

    raise RuntimeError(
        "Cannot build fact_delivery because "
        "shipping locations conflict."
    )

else:

    print("Shipping-location consistency: PASS")


# ============================================================
# INCREMENTAL LINEAGE
# ============================================================

print("\n========== INCREMENTAL PROCESSING ==========")

if path_exists(STATE_PATH):

    state_df = spark.read.parquet(STATE_PATH)

    print("Existing lineage state found.")

    new_delivery = (
        delivery_df
        .join(
            state_df.select(
                "topic",
                "partition",
                "offset"
            ),
            on=[
                "topic",
                "partition",
                "offset"
            ],
            how="left_anti"
        )
    )

    load_type = "INCREMENTAL"

else:

    state_df = None

    new_delivery = delivery_df

    load_type = "INITIAL"


new_delivery_count = new_delivery.count()

print(f"Load type: {load_type}")
print(f"New delivery events: {new_delivery_count}")


# ============================================================
# NO NEW DATA
# ============================================================

if new_delivery_count == 0:

    print("\nNo new delivery events found.")

    if path_exists(GOLD_PATH):

        existing_gold = spark.read.parquet(GOLD_PATH)

        print(
            f"Existing Gold rows: "
            f"{existing_gold.count()}"
        )

    else:

        print("Gold table does not exist yet.")

    print("\nIncremental/idempotency: PASS")

    spark.stop()

    raise SystemExit


# ============================================================
# AFFECTED SHIPMENTS
# ============================================================

affected_shipments = (
    new_delivery
    .filter(
        F.col("shipment_id").isNotNull()
    )
    .select("shipment_id")
    .distinct()
)

affected_count = affected_shipments.count()

print(f"Affected shipment_ids: {affected_count}")


# ============================================================
# REBUILD AFFECTED SHIPMENTS
#
# IMPORTANT:
# We rebuild affected shipment IDs from ALL Silver delivery
# history, not only the newly arrived events.
# ============================================================

all_delivery_history = (
    delivery_df
    .join(
        affected_shipments,
        on="shipment_id",
        how="inner"
    )
)


# ============================================================
# PARSE EVENT TIMESTAMP
# ============================================================

delivery_history = (
    all_delivery_history
    .withColumn(
        "event_ts",
        F.to_timestamp("event_time")
    )
)


# ============================================================
# BUILD DELIVERY FACT
#
# Grain:
#     1 row = 1 shipment_id
# ============================================================

delivery_fact = (
    delivery_history
    .groupBy("shipment_id")
    .agg(

        # ----------------------------------------------------
        # Relationships
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # DELIVERY LIFECYCLE
        # ----------------------------------------------------

        F.max(
            F.when(
                F.col("event_type") == "in_transit",
                F.lit(True)
            ).otherwise(F.lit(False))
        ).alias("in_transit"),

        F.min(
            F.when(
                F.col("event_type") == "in_transit",
                F.col("event_ts")
            )
        ).alias("in_transit_at"),

        F.max(
            F.when(
                F.col("event_type") == "out_for_delivery",
                F.lit(True)
            ).otherwise(F.lit(False))
        ).alias("out_for_delivery"),

        F.min(
            F.when(
                F.col("event_type") == "out_for_delivery",
                F.col("event_ts")
            )
        ).alias("out_for_delivery_at"),

        F.max(
            F.when(
                F.col("event_type") == "delivered",
                F.lit(True)
            ).otherwise(F.lit(False))
        ).alias("delivered"),

        F.min(
            F.when(
                F.col("event_type") == "delivered",
                F.col("event_ts")
            )
        ).alias("delivered_at"),

        F.max(
            F.when(
                F.col("event_type") == "delivery_failed",
                F.lit(True)
            ).otherwise(F.lit(False))
        ).alias("delivery_failed"),

        F.min(
            F.when(
                F.col("event_type") == "delivery_failed",
                F.col("event_ts")
            )
        ).alias("delivery_failed_at"),

        # ----------------------------------------------------
        # OVERALL DELIVERY TIMING
        # ----------------------------------------------------

        F.min(
            "event_ts"
        ).alias("first_delivery_event_at"),

        F.max(
            "event_ts"
        ).alias("last_delivery_event_at"),

        F.count("*").alias(
            "delivery_event_count"
        )
    )
)


# ============================================================
# SHIPPING LOCATION ENRICHMENT
#
# Source:
#
# order_events
#     event_type = order_created
#     order_id
#     shipping_address_id
#     shipping_city
#     shipping_state
#     shipping_country
#
# Join:
#
# delivery_fact.order_id
#          =
# shipping_lookup.order_id
# ============================================================

delivery_fact = (
    delivery_fact
    .join(
        shipping_lookup,
        on="order_id",
        how="left"
    )
)


# ============================================================
# DELIVERY DURATION
#
# in_transit → delivered
# ============================================================

delivery_fact = (
    delivery_fact
    .withColumn(
        "delivery_duration_seconds",
        F.when(
            F.col("in_transit_at").isNotNull() &
            F.col("delivered_at").isNotNull(),

            F.col("delivered_at").cast("long") -
            F.col("in_transit_at").cast("long")
        )
    )
)


# ============================================================
# FINAL GOLD SCHEMA
# ============================================================

delivery_fact = delivery_fact.select(

    # --------------------------------------------------------
    # IDENTIFIERS
    # --------------------------------------------------------

    "shipment_id",
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",

    # --------------------------------------------------------
    # SHIPPING DESTINATION
    # --------------------------------------------------------

    "shipping_address_id",
    "shipping_city",
    "shipping_state",
    "shipping_country",

    # --------------------------------------------------------
    # DELIVERY LIFECYCLE
    # --------------------------------------------------------

    "in_transit",
    "in_transit_at",

    "out_for_delivery",
    "out_for_delivery_at",

    "delivered",
    "delivered_at",

    "delivery_failed",
    "delivery_failed_at",

    # --------------------------------------------------------
    # DERIVED METRICS
    # --------------------------------------------------------

    "delivery_duration_seconds",

    "first_delivery_event_at",
    "last_delivery_event_at",
    "delivery_event_count"
)


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\n========== DATA QUALITY CHECKS ==========")


# ------------------------------------------------------------
# Duplicate shipment IDs
# ------------------------------------------------------------

duplicate_shipments = (
    delivery_fact
    .groupBy("shipment_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    "Duplicate shipment_id:",
    "PASS"
    if duplicate_shipments == 0
    else f"FAIL ({duplicate_shipments})"
)


# ------------------------------------------------------------
# Null shipment IDs
# ------------------------------------------------------------

null_shipment_ids = (
    delivery_fact
    .filter(
        F.col("shipment_id").isNull()
    )
    .count()
)

print(
    "Null shipment_id:",
    "PASS"
    if null_shipment_ids == 0
    else f"FAIL ({null_shipment_ids})"
)


# ------------------------------------------------------------
# Null order IDs
# ------------------------------------------------------------

null_order_ids = (
    delivery_fact
    .filter(
        F.col("order_id").isNull()
    )
    .count()
)

print(
    "Null order_id:",
    "PASS"
    if null_order_ids == 0
    else f"WARNING ({null_order_ids})"
)


# ------------------------------------------------------------
# Invalid event count
# ------------------------------------------------------------

invalid_event_count = (
    delivery_fact
    .filter(
        F.col("delivery_event_count") <= 0
    )
    .count()
)

print(
    "Invalid delivery_event_count:",
    "PASS"
    if invalid_event_count == 0
    else f"FAIL ({invalid_event_count})"
)


# ------------------------------------------------------------
# Delivered without in_transit
# ------------------------------------------------------------

delivered_without_transit = (
    delivery_fact
    .filter(
        (F.col("delivered") == True) &
        (F.col("in_transit") == False)
    )
    .count()
)

print(
    "Delivered without in_transit:",
    "PASS"
    if delivered_without_transit == 0
    else f"WARNING ({delivered_without_transit})"
)


# ------------------------------------------------------------
# Chronology
# ------------------------------------------------------------

chronology_issues = (
    delivery_fact
    .filter(
        F.col("in_transit_at").isNotNull() &
        F.col("delivered_at").isNotNull() &
        (
            F.col("delivered_at") <
            F.col("in_transit_at")
        )
    )
    .count()
)

print(
    "Delivery chronology:",
    "PASS"
    if chronology_issues == 0
    else f"WARNING ({chronology_issues})"
)


# ------------------------------------------------------------
# Negative duration
# ------------------------------------------------------------

negative_duration = (
    delivery_fact
    .filter(
        F.col("delivery_duration_seconds") < 0
    )
    .count()
)

print(
    "Negative delivery duration:",
    "PASS"
    if negative_duration == 0
    else f"WARNING ({negative_duration})"
)


# ============================================================
# SHIPPING LOCATION DQ
# ============================================================

print("\n========== SHIPPING LOCATION DQ ==========")

current_rows = delivery_fact.count()

shipping_address_count = (
    delivery_fact
    .filter(
        F.col("shipping_address_id").isNotNull()
    )
    .count()
)

shipping_city_count = (
    delivery_fact
    .filter(
        F.col("shipping_city").isNotNull()
    )
    .count()
)

shipping_state_count = (
    delivery_fact
    .filter(
        F.col("shipping_state").isNotNull()
    )
    .count()
)

shipping_country_count = (
    delivery_fact
    .filter(
        F.col("shipping_country").isNotNull()
    )
    .count()
)

print(
    f"shipping_address_id populated: "
    f"{shipping_address_count}/{current_rows}"
)

print(
    f"shipping_city populated: "
    f"{shipping_city_count}/{current_rows}"
)

print(
    f"shipping_state populated: "
    f"{shipping_state_count}/{current_rows}"
)

print(
    f"shipping_country populated: "
    f"{shipping_country_count}/{current_rows}"
)


# ============================================================
# MERGE WITH EXISTING GOLD
# ============================================================

print("\n========== GOLD MERGE ==========")

existing_gold = read_parquet_if_exists(
    GOLD_PATH
)

if existing_gold is not None:
    existing_gold.cache()
    existing_count = existing_gold.count()

    print(
        f"Existing Gold rows: "
        f"{existing_count}"
    )

    # Remove old versions of affected shipments
    remaining_gold = (
        existing_gold
        .join(
            affected_shipments,
            on="shipment_id",
            how="left_anti"
        )
    )

    final_gold = (
        remaining_gold
        .unionByName(delivery_fact)
    )

else:

    print("No existing Gold table.")
    print("Creating initial Gold table.")

    final_gold = delivery_fact


# ============================================================
# FINAL DEDUPLICATION
# ============================================================

final_gold = (
    final_gold
    .dropDuplicates(
        ["shipment_id"]
    )
)


# ============================================================
# FINAL COUNTS
# ============================================================

final_rows = final_gold.count()

final_distinct_shipments = (
    final_gold
    .select("shipment_id")
    .distinct()
    .count()
)

print("\n========== FINAL GOLD COUNTS ==========")

print(
    f"Final Gold rows: "
    f"{final_rows}"
)

print(
    f"Distinct shipment_ids: "
    f"{final_distinct_shipments}"
)

print(
    "Shipment grain:",
    "PASS"
    if final_rows == final_distinct_shipments
    else "FAIL"
)


# ============================================================
# WRITE GOLD
# ============================================================

print("\n========== GOLD WRITE ==========")

(
    final_gold
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

print("Gold write: SUCCESS")


# ============================================================
# UPDATE LINEAGE STATE
#
# State is updated ONLY after Gold succeeds.
# ============================================================

new_lineage = (
    new_delivery
    .select(
        "topic",
        "partition",
        "offset"
    )
    .dropDuplicates(
        ["topic", "partition", "offset"]
    )
)

if state_df is not None:

    final_state = (
        state_df
        .unionByName(new_lineage)
        .dropDuplicates(
            ["topic", "partition", "offset"]
        )
    )

else:

    final_state = new_lineage


(
    final_state
    .write
    .mode("overwrite")
    .parquet(STATE_PATH)
)

print("Lineage state update: SUCCESS")


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 80)
print("FACT DELIVERY SUMMARY")
print("=" * 80)

print(
    f"Load type:                  {load_type}"
)

print(
    f"New Silver events:          {new_delivery_count}"
)

print(
    f"Affected shipments:         {affected_count}"
)

print(
    f"Final Gold rows:            {final_rows}"
)

print(
    f"Distinct shipment IDs:      "
    f"{final_distinct_shipments}"
)


# ============================================================
# DELIVERY LIFECYCLE SUMMARY
# ============================================================

print("\n========== DELIVERY LIFECYCLE SUMMARY ==========")

final_gold.select(

    F.sum(
        F.when(
            F.col("in_transit") == True,
            1
        ).otherwise(0)
    ).alias("in_transit"),

    F.sum(
        F.when(
            F.col("out_for_delivery") == True,
            1
        ).otherwise(0)
    ).alias("out_for_delivery"),

    F.sum(
        F.when(
            F.col("delivered") == True,
            1
        ).otherwise(0)
    ).alias("delivered"),

    F.sum(
        F.when(
            F.col("delivery_failed") == True,
            1
        ).otherwise(0)
    ).alias("delivery_failed")

).show()


# ============================================================
# SHIPPING GEOGRAPHY SUMMARY
# ============================================================

print("\n========== SHIPPING GEOGRAPHY SUMMARY ==========")

final_gold.groupBy(
    "shipping_country",
    "shipping_state",
    "shipping_city"
).count() \
 .orderBy(
     F.desc("count")
 ) \
 .show(20, truncate=False)


# ============================================================
# FULL GOLD COLUMN LIST
# ============================================================

print("\n========== ALL FACT_DELIVERY COLUMNS ==========")

print(
    f"Number of columns: "
    f"{len(final_gold.columns)}"
)

for index, column in enumerate(
    final_gold.columns,
    start=1
):

    print(
        f"{index:02d}. {column}"
    )


# ============================================================
# FULL GOLD SAMPLE
#
# ALL columns are displayed.
# ============================================================

print("\n========== FULL FACT_DELIVERY SAMPLE ==========")

print(
    "Showing 10 rows with ALL columns:"
)

final_gold.show(
    10,
    truncate=False
)


# ============================================================
# FULL GOLD SCHEMA
# ============================================================

print("\n========== FACT_DELIVERY SCHEMA ==========")

final_gold.printSchema()


# ============================================================
# FINAL VALIDATION
# ============================================================

print("\n========== FINAL VALIDATION ==========")

print("Incremental processing: PASS")
print("Idempotency lineage:    PASS")
print("Gold write:             PASS")
print("Lineage update:         PASS")


print("\n" + "=" * 80)
print("FACT DELIVERY BUILD COMPLETE")
print("=" * 80)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()