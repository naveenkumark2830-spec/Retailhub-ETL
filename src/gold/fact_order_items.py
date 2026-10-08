# ================================================================
# RETAILHUB - GOLD : fact_order_items
# ================================================================
#
# Grain:
#   ONE ROW = ONE ORDER ITEM
#
# Source:
#   Silver:
#       cart_events
#       order_events
#       checkout_events
#       fulfillment_events
#       delivery_events
#       return_events
#
# Incremental strategy:
#
#   New Silver events
#          |
#          v
#   affected order_item_id
#          |
#          v
#   rebuild affected items from ALL Silver history
#          |
#          v
#   remove old affected Gold rows
#          |
#          v
#   append rebuilt rows
#
# Lineage:
#   topic + partition + offset
#
# ================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ================================================================
# 1. PATHS
# ================================================================

from config.settings import (
    SILVER_PATH as SILVER_BASE_PATH,
    GOLD_BASE_PATH,
)

SILVER_PATH = SILVER_BASE_PATH.rstrip("/")
GOLD_BASE_PATH = GOLD_BASE_PATH.rstrip("/")

GOLD_PATH = f"{GOLD_BASE_PATH}/fact_order_items"

STATE_PATH = f"{GOLD_BASE_PATH}/_state/fact_order_items"

# ================================================================
# 2. SILVER DOMAINS
# ================================================================

ITEM_DOMAINS = [
    "cart_events",
    "order_events",
    "checkout_events",
    "fulfillment_events",
    "delivery_events",
    "return_events",
]


# ================================================================
# 3. SPARK
# ================================================================

spark = (
    SparkSession.builder
    .appName("RetailHub_Gold_FactOrderItems")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n")
print("=" * 70)
print("FACT ORDER ITEMS - GOLD")
print("=" * 70)


# ================================================================
# 4. READ SILVER DOMAINS
# ================================================================

silver_dfs = {}

for domain in ITEM_DOMAINS:

    path = f"{SILVER_PATH}/{domain}"

    try:
        df = spark.read.parquet(path)
        if df.take(1):
            silver_dfs[domain] = df
            print(f"{domain:22} -> {df.count()} records")
        else:
            print(f"{domain:22} -> EMPTY")
    except Exception:
        print(f"{domain:22} -> NOT FOUND")


if not silver_dfs:

    print("\nNo Silver domains found.")

    spark.stop()
    raise SystemExit(0)


# ================================================================
# 5. PREPARE COMMON FLAT SILVER SCHEMA
# ================================================================

def prepare_domain(df):

    return (
        df
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",

            "event_id",
            "simulation_run_id",
            "event_type",
            "event_version",
            "event_time",

            "customer_id",
            "anonymous_id",

            "product_id",
            "order_id",
            "order_item_id",

            "quantity",
            "unit_price",
            "currency",

            "brand",
            "category",
            "category_id",
            "category_name",
            "subcategory_id",

            "price",

            "subtotal",
            "discount_amount",
            "coupon_discount",
            "tax_amount",
            "shipping_fee",
            "delivery_fee",
            "total_amount",
            "amount",

            "new_status",
            "previous_status",

            "return_id",
            "return_reason",
            "refund_amount",

            "warehouse_id",
            "inventory_quantity",
            "stock",

            "event_ingestion_time",
            "bronze_ingestion_time",
            "silver_ingestion_time",

            "has_event_arrival_delay",
            "event_arrival_lag_seconds",
            "producer_ingestion_lag_seconds",
        )

        # --------------------------------------------------------
        # Numeric types
        # --------------------------------------------------------

        .withColumn(
            "quantity",
            F.col("quantity").cast("double")
        )

        .withColumn(
            "unit_price",
            F.col("unit_price").cast("double")
        )

        .withColumn(
            "price",
            F.col("price").cast("double")
        )

        .withColumn(
            "subtotal",
            F.col("subtotal").cast("double")
        )

        .withColumn(
            "discount_amount",
            F.col("discount_amount").cast("double")
        )

        .withColumn(
            "coupon_discount",
            F.col("coupon_discount").cast("double")
        )

        .withColumn(
            "tax_amount",
            F.col("tax_amount").cast("double")
        )

        .withColumn(
            "total_amount",
            F.col("total_amount").cast("double")
        )

        .withColumn(
            "amount",
            F.col("amount").cast("double")
        )

        # --------------------------------------------------------
        # Event timestamp
        # --------------------------------------------------------

        .withColumn(
            "event_ts",
            F.col("event_time").cast("timestamp")
        )

        # --------------------------------------------------------
        # Only actual item events
        # --------------------------------------------------------

        .filter(
            F.col("order_item_id").isNotNull()
        )
    )


# ================================================================
# 6. PREPARE ALL DOMAINS
# ================================================================

prepared_dfs = []

for domain, df in silver_dfs.items():

    prepared_dfs.append(
        prepare_domain(df)
    )


# ================================================================
# 7. UNION ALL ITEM EVENTS
# ================================================================

all_item_events = prepared_dfs[0]

for df in prepared_dfs[1:]:

    all_item_events = (
        all_item_events
        .unionByName(df)
    )


# ================================================================
# 8. READ LINEAGE STATE
# ================================================================

processed_state = None


try:
    processed_state = (
        spark.read
        .parquet(STATE_PATH)
        .select(
            "topic",
            "partition",
            "offset"
        )
        .dropDuplicates()
    )
    if processed_state.take(1):
        print("\nExisting lineage state found.")
    else:
        processed_state = None
except Exception:
    processed_state = None


# ================================================================
# 9. FIND NEW EVENTS
# ================================================================

LINEAGE_COLUMNS = [
    "topic",
    "partition",
    "offset",
]


if processed_state is None:

    print("\nLoad type: INITIAL")

    new_item_events = all_item_events

else:

    print("\nLoad type: INCREMENTAL")

    new_item_events = (
        all_item_events
        .join(
            processed_state,
            LINEAGE_COLUMNS,
            "left_anti"
        )
    )


new_event_count = new_item_events.count()


# ================================================================
# 10. FIND AFFECTED ORDER ITEMS
# ================================================================

affected_items = (
    new_item_events

    .select(
        "order_item_id"
    )

    .filter(
        F.col("order_item_id").isNotNull()
    )

    .distinct()
)


affected_item_count = affected_items.count()


print("\n")
print("=" * 70)
print("INCREMENTAL DETECTION")
print("=" * 70)

print(
    f"New item-related Silver events : "
    f"{new_event_count}"
)

print(
    f"Affected order_item_ids        : "
    f"{affected_item_count}"
)


# ================================================================
# 11. IDEMPOTENCY CHECK
# ================================================================

if affected_item_count == 0:

    print("\nNo new item changes detected.")

    print(
        "FACT ORDER ITEMS IS ALREADY UP TO DATE."
    )

    print(
        "Incremental/idempotency check: PASS"
    )

    spark.stop()
    raise SystemExit(0)


# ================================================================
# 12. REBUILD AFFECTED ITEMS FROM ALL HISTORY
# ================================================================

affected_history = (
    all_item_events

    .join(
        affected_items,
        "order_item_id",
        "inner"
    )
)


# ================================================================
# 13. BUILD ITEM BASE
# ================================================================
#
# IMPORTANT:
#
# Each attribute is resolved independently.
#
# We do NOT select one event as the winner.
#
# This preserves the behavior of the original working code.
#
# ================================================================

item_base = (
    affected_history

    .groupBy(
        "order_item_id"
    )

    .agg(

        # --------------------------------------------------------
        # IDs
        # --------------------------------------------------------

        F.first(
            "order_id",
            ignorenulls=True
        ).alias("order_id"),

        F.first(
            "customer_id",
            ignorenulls=True
        ).alias("customer_id"),

        F.first(
            "anonymous_id",
            ignorenulls=True
        ).alias("anonymous_id"),

        F.first(
            "product_id",
            ignorenulls=True
        ).alias("product_id"),

        # --------------------------------------------------------
        # Product information
        #
        # These may remain NULL until dimensions are populated
        # through MySQL CDC.
        # --------------------------------------------------------

        F.first(
            "brand",
            ignorenulls=True
        ).alias("brand"),

        F.first(
            "category",
            ignorenulls=True
        ).alias("category"),

        F.first(
            "category_id",
            ignorenulls=True
        ).alias("category_id"),

        F.first(
            "category_name",
            ignorenulls=True
        ).alias("category_name"),

        F.first(
            "subcategory_id",
            ignorenulls=True
        ).alias("subcategory_id"),

        # --------------------------------------------------------
        # CORE ITEM MEASURES
        #
        # These are independently recovered from Silver history.
        # --------------------------------------------------------

        F.first(
            "quantity",
            ignorenulls=True
        ).alias("quantity"),

        F.first(
            "unit_price",
            ignorenulls=True
        ).alias("unit_price"),

        F.first(
            "currency",
            ignorenulls=True
        ).alias("currency"),

        # --------------------------------------------------------
        # Source subtotal
        # --------------------------------------------------------

        F.first(
            "subtotal",
            ignorenulls=True
        ).alias("source_subtotal"),

        # --------------------------------------------------------
        # Order-level financial values.
        #
        # DO NOT allocate these to individual items.
        # --------------------------------------------------------

        F.first(
            "discount_amount",
            ignorenulls=True
        ).alias("order_discount_amount"),

        F.first(
            "coupon_discount",
            ignorenulls=True
        ).alias("order_coupon_discount"),

        F.first(
            "tax_amount",
            ignorenulls=True
        ).alias("order_tax_amount"),

        F.first(
            "shipping_fee",
            ignorenulls=True
        ).alias("order_shipping_fee"),

        # --------------------------------------------------------
        # Timeline
        # --------------------------------------------------------

        F.min(
            "event_ts"
        ).alias("first_event_at"),

        F.max(
            "event_ts"
        ).alias("last_event_at"),

        # --------------------------------------------------------
        # Event count
        # --------------------------------------------------------

        F.count("*").alias(
            "item_event_count"
        ),
    )
)


# ================================================================
# 14. DERIVE ITEM FINANCIALS
# ================================================================

updated_items = (
    item_base

    # ------------------------------------------------------------
    # Item subtotal
    #
    # Prefer actual source subtotal if available.
    # Otherwise calculate quantity * unit_price.
    # ------------------------------------------------------------

    .withColumn(
        "item_subtotal",

        F.when(
            F.col("source_subtotal").isNotNull(),

            F.round(
                F.col("source_subtotal"),
                2
            )
        )

        .when(
            F.col("quantity").isNotNull()
            &
            F.col("unit_price").isNotNull(),

            F.round(
                F.col("quantity")
                *
                F.col("unit_price"),
                2
            )
        )

        .otherwise(
            F.lit(None).cast("double")
        )
    )

    # ------------------------------------------------------------
    # Order discount is NOT allocated to item
    # ------------------------------------------------------------

    .withColumn(
        "item_discount",
        F.lit(None).cast("double")
    )

    # ------------------------------------------------------------
    # Order tax is NOT allocated to item
    # ------------------------------------------------------------

    .withColumn(
        "item_tax",
        F.lit(None).cast("double")
    )

    # ------------------------------------------------------------
    # Current item total = merchandise value
    # ------------------------------------------------------------

    .withColumn(
        "item_total",
        F.col("item_subtotal")
    )
)


# ================================================================
# 15. DATA QUALITY CHECKS
# ================================================================

print("\n")
print("=" * 70)
print("FACT ORDER ITEMS DATA QUALITY")
print("=" * 70)


# ---------------------------------------------------------------
# Duplicate item IDs
# ---------------------------------------------------------------

duplicate_items = (
    updated_items
    .groupBy("order_item_id")
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicate_count = duplicate_items.count()


if duplicate_count == 0:

    print(
        "Duplicate order_item_id : PASS"
    )

else:

    print(
        f"Duplicate order_item_id : FAIL ({duplicate_count})"
    )


# ---------------------------------------------------------------
# Null order ID
# ---------------------------------------------------------------

null_order_count = (
    updated_items
    .filter(
        F.col("order_id").isNull()
    )
    .count()
)

print(
    f"Null order_id            : "
    f"{null_order_count}"
)


# ---------------------------------------------------------------
# Null product ID
# ---------------------------------------------------------------

null_product_count = (
    updated_items
    .filter(
        F.col("product_id").isNull()
    )
    .count()
)

print(
    f"Null product_id          : "
    f"{null_product_count}"
)


# ---------------------------------------------------------------
# Invalid quantity
# ---------------------------------------------------------------

invalid_quantity_count = (
    updated_items
    .filter(
        F.col("quantity").isNotNull()
        &
        (F.col("quantity") <= 0)
    )
    .count()
)

print(
    f"Invalid quantity         : "
    f"{invalid_quantity_count}"
)


# ---------------------------------------------------------------
# Invalid unit price
# ---------------------------------------------------------------

invalid_price_count = (
    updated_items
    .filter(
        F.col("unit_price").isNotNull()
        &
        (F.col("unit_price") < 0)
    )
    .count()
)

print(
    f"Invalid unit price       : "
    f"{invalid_price_count}"
)


# ---------------------------------------------------------------
# Invalid item subtotal
# ---------------------------------------------------------------

invalid_subtotal_count = (
    updated_items
    .filter(
        F.col("item_subtotal").isNotNull()
        &
        (F.col("item_subtotal") < 0)
    )
    .count()
)

print(
    f"Invalid item subtotal    : "
    f"{invalid_subtotal_count}"
)


# ---------------------------------------------------------------
# Item arithmetic mismatch
# ---------------------------------------------------------------

arithmetic_mismatch_count = (
    updated_items

    .filter(
        F.col("quantity").isNotNull()
        &
        F.col("unit_price").isNotNull()
        &
        F.col("item_subtotal").isNotNull()
        &
        (
            F.abs(
                F.col("item_subtotal")
                -
                (
                    F.col("quantity")
                    *
                    F.col("unit_price")
                )
            )
            > 0.01
        )
    )

    .count()
)

print(
    f"Item arithmetic mismatch : "
    f"{arithmetic_mismatch_count}"
)


# ================================================================
# 16. ORPHAN ITEM CHECK
# ================================================================

orphan_item_count = (
    updated_items
    .filter(
        F.col("order_id").isNull()
    )
    .count()
)


if orphan_item_count == 0:

    print(
        "Orphan order items       : PASS"
    )

else:

    print(
        f"Orphan order items       : "
        f"WARNING ({orphan_item_count})"
    )


# ================================================================
# 17. EXISTING GOLD
# ================================================================

existing_gold = None


try:
    existing_gold = (
        spark.read
        .parquet(GOLD_PATH)
    )
    existing_gold.cache()
    existing_count = existing_gold.count()
    if existing_count > 0:
        print(
            f"\nExisting Gold rows       : "
            f"{existing_count}"
        )
    else:
        existing_gold = None
except Exception:
    existing_gold = None


# ================================================================
# 18. REMOVE OLD AFFECTED ITEMS
# ================================================================

if existing_gold is not None:

    unaffected_gold = (
        existing_gold

        .join(
            affected_items,
            "order_item_id",
            "left_anti"
        )
    )

else:

    unaffected_gold = None


# ================================================================
# 19. FINAL GOLD
# ================================================================

if unaffected_gold is not None:

    final_gold = (
        unaffected_gold
        .unionByName(
            updated_items.select(
                *unaffected_gold.columns
            )
        )
    )

else:

    final_gold = updated_items


# ================================================================
# 20. FINAL GOLD SCHEMA
# ================================================================

final_gold = (
    final_gold

    .select(

        "order_item_id",

        "order_id",
        "customer_id",
        "anonymous_id",
        "product_id",

        "brand",
        "category",
        "category_id",
        "category_name",
        "subcategory_id",

        "quantity",
        "unit_price",
        "currency",

        "item_subtotal",
        "item_discount",
        "item_tax",
        "item_total",

        "first_event_at",
        "last_event_at",

        "item_event_count",
    )

    .dropDuplicates(
        ["order_item_id"]
    )
)


# ================================================================
# 21. FINAL COUNTS
# ================================================================

final_count = final_gold.count()

distinct_item_count = (
    final_gold
    .select("order_item_id")
    .distinct()
    .count()
)


print("\n")
print("=" * 70)
print("FINAL GOLD SUMMARY")
print("=" * 70)

print(
    f"New Silver item events   : "
    f"{new_event_count}"
)

print(
    f"Affected order items     : "
    f"{affected_item_count}"
)

print(
    f"Final Gold rows          : "
    f"{final_count}"
)

print(
    f"Distinct order_item_id   : "
    f"{distinct_item_count}"
)


# ================================================================
# 22. WRITE GOLD
# ================================================================




(
    final_gold
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)


# ================================================================
# 23. UPDATE LINEAGE STATE
# ================================================================

all_processed_lineage = (
    all_item_events

    .select(
        "topic",
        "partition",
        "offset"
    )

    .dropDuplicates()
)





(
    all_processed_lineage
    .write
    .mode("overwrite")
    .parquet(STATE_PATH)
)


# ================================================================
# 24. SAMPLE
# ================================================================

print("\n")
print("=" * 70)
print("SAMPLE fact_order_items")
print("=" * 70)

(
    final_gold

    .orderBy(
        F.col("last_event_at").desc()
    )

    .show(
        20,
        truncate=False
    )
)


# ================================================================
# 25. FINAL DQ STATUS
# ================================================================

if (
    duplicate_count == 0
    and null_product_count == 0
    and invalid_quantity_count == 0
    and invalid_price_count == 0
    and invalid_subtotal_count == 0
    and arithmetic_mismatch_count == 0
):

    print("\n")
    print("=" * 70)
    print("FACT ORDER ITEMS DQ: PASS")
    print("=" * 70)

else:

    print("\n")
    print("=" * 70)
    print("FACT ORDER ITEMS DQ: WARNING")
    print("=" * 70)


print("\n")
print("=" * 70)
print("FACT ORDER ITEMS GOLD LOAD COMPLETE")
print("=" * 70)

print(
    f"Gold path  : {GOLD_PATH}"
)

print(
    f"State path : {STATE_PATH}"
)


spark.stop()