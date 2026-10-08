from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DoubleType
import os


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub_Fact_Product_Interactions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. PATH CONFIGURATION
# ============================================================

from config.settings import (
    SILVER_PATH,
    GOLD_BASE_PATH,
)

SILVER_PATH = SILVER_PATH.rstrip("/")
GOLD_BASE_PATH = GOLD_BASE_PATH.rstrip("/")

GOLD_PATH = (
    f"{GOLD_BASE_PATH}/fact_product_interactions"
)

STATE_PATH = (
    f"{GOLD_BASE_PATH}/_state/fact_product_interactions"
)

DISCOVERY_PATH = (
    f"{SILVER_PATH}/discovery_events"
)

CART_PATH = (
    f"{SILVER_PATH}/cart_events"
)


# ============================================================
# 3. HELPER FUNCTIONS
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
    """
    Read a Parquet directory if it exists.
    Otherwise return None.
    """
    try:
        df = spark.read.parquet(path)
        if df.take(1):
            return df
        return None
    except Exception:
        return None


# ============================================================
# 4. LOAD SILVER DATA
# ============================================================

print("\n" + "=" * 80)
print("LOADING SILVER DATA")
print("=" * 80)

raw_discovery = read_parquet_if_exists(
    DISCOVERY_PATH
)

raw_cart = read_parquet_if_exists(
    CART_PATH
)

if raw_discovery is None and raw_cart is None:
    print("\nNo product interaction Silver data found (discovery and cart are missing or empty).")
    spark.stop()
    raise SystemExit(0)

print(
    f"Cart events      : {cart_count}"
)


# ============================================================
# 5. FINAL FACT TABLE COLUMNS
# ============================================================

FACT_COLUMNS = [

    # --------------------------------------------------------
    # Event identity
    # --------------------------------------------------------

    "event_id",
    "event_time",
    "event_type",

    # --------------------------------------------------------
    # User identity
    # --------------------------------------------------------

    "customer_id",
    "anonymous_id",
    "user_type",

    # --------------------------------------------------------
    # Session
    # --------------------------------------------------------

    "session_id",

    # --------------------------------------------------------
    # Product / cart
    # --------------------------------------------------------

    "product_id",
    "cart_id",

    # --------------------------------------------------------
    # Search / discovery
    # --------------------------------------------------------

    "query",
    "search_query",
    "result_count",
    "position",

    # --------------------------------------------------------
    # Product attributes
    # --------------------------------------------------------

    "brand",
    "category",
    "category_id",
    "category_name",
    "subcategory_id",

    # --------------------------------------------------------
    # Commerce values
    # --------------------------------------------------------

    "price",
    "quantity",
    "unit_price",

    # --------------------------------------------------------
    # Kafka lineage
    # --------------------------------------------------------

    "topic",
    "partition",
    "offset",

    # --------------------------------------------------------
    # Data quality / latency
    # --------------------------------------------------------

    "has_event_arrival_delay",
    "event_arrival_lag_seconds",
    "producer_ingestion_lag_seconds",
]


# ============================================================
# 6. PREPARE DISCOVERY EVENTS
# ============================================================

def prepare_discovery(df):

    return (
        df
        .select(
            "event_id",
            "event_time",
            "event_type",

            "customer_id",
            "anonymous_id",
            "user_type",

            "session_id",

            "product_id",
            "cart_id",

            "query",

            # search_query may not exist in every Silver
            # version, so handle it safely later.
            *(
                ["search_query"]
                if "search_query" in df.columns
                else []
            ),

            "result_count",
            "position",

            "brand",
            "category",
            "category_id",
            "category_name",
            "subcategory_id",

            "price",
            "quantity",
            "unit_price",

            "topic",
            "partition",
            "offset",

            "has_event_arrival_delay",
            "event_arrival_lag_seconds",
            "producer_ingestion_lag_seconds",
        )
    )


# ============================================================
# 7. PREPARE CART EVENTS
# ============================================================

def prepare_cart(df):

    return (
        df
        .select(
            "event_id",
            "event_time",
            "event_type",

            "customer_id",
            "anonymous_id",
            "user_type",

            "session_id",

            "product_id",
            "cart_id",

            "query",

            *(
                ["search_query"]
                if "search_query" in df.columns
                else []
            ),

            "result_count",
            "position",

            "brand",
            "category",
            "category_id",
            "category_name",
            "subcategory_id",

            "price",
            "quantity",
            "unit_price",

            "topic",
            "partition",
            "offset",

            "has_event_arrival_delay",
            "event_arrival_lag_seconds",
            "producer_ingestion_lag_seconds",
        )
    )


# ============================================================
# 8. PREPARE BOTH DOMAINS
# ============================================================

if raw_discovery is not None and raw_cart is not None:
    discovery_df = prepare_discovery(raw_discovery)
    cart_df = prepare_cart(raw_cart)
elif raw_discovery is not None:
    discovery_df = prepare_discovery(raw_discovery)
    cart_df = spark.createDataFrame([], discovery_df.schema)
else:
    cart_df = prepare_cart(raw_cart)
    discovery_df = spark.createDataFrame([], cart_df.schema)

discovery_count = discovery_df.count()
cart_count = cart_df.count()

print(f"Discovery events : {discovery_count}")
print(f"Cart events      : {cart_count}")


# ============================================================
# 9. NORMALIZE OPTIONAL COLUMNS
# ============================================================

def normalize_columns(df):

    required_columns = {

        "event_id": "string",

        "event_time": "timestamp",

        "event_type": "string",

        "customer_id": "string",

        "anonymous_id": "string",

        "user_type": "string",

        "session_id": "string",

        "product_id": "string",

        "cart_id": "string",

        "query": "string",

        "search_query": "string",

        "result_count": "int",

        "position": "int",

        "brand": "string",

        "category": "string",

        "category_id": "string",

        "category_name": "string",

        "subcategory_id": "string",

        "price": "double",

        "quantity": "int",

        "unit_price": "double",

        "topic": "string",

        "partition": "int",

        "offset": "long",

        "has_event_arrival_delay": "boolean",

        "event_arrival_lag_seconds": "double",

        "producer_ingestion_lag_seconds": "double",
    }


    for column_name, data_type in required_columns.items():

        if column_name not in df.columns:

            df = df.withColumn(
                column_name,
                F.lit(None).cast(data_type)
            )


    return df.select(
        FACT_COLUMNS
    )


discovery_df = normalize_columns(
    discovery_df
)

cart_df = normalize_columns(
    cart_df
)


# ============================================================
# 10. COMBINE DISCOVERY + CART
# ============================================================

print("\n" + "=" * 80)
print("COMBINING INTERACTION EVENTS")
print("=" * 80)


all_events = (
    discovery_df
    .unionByName(
        cart_df,
        allowMissingColumns=True
    )
)


# ============================================================
# 11. STANDARDIZE TYPES
# ============================================================

all_events = (
    all_events

    .withColumn(
        "event_time",
        F.to_timestamp(
            F.col("event_time")
        )
    )

    .withColumn(
        "result_count",
        F.col("result_count")
        .cast(IntegerType())
    )

    .withColumn(
        "position",
        F.col("position")
        .cast(IntegerType())
    )

    .withColumn(
        "price",
        F.col("price")
        .cast(DoubleType())
    )

    .withColumn(
        "quantity",
        F.col("quantity")
        .cast(IntegerType())
    )

    .withColumn(
        "unit_price",
        F.col("unit_price")
        .cast(DoubleType())
    )

    .withColumn(
        "event_arrival_lag_seconds",
        F.col(
            "event_arrival_lag_seconds"
        )
        .cast(DoubleType())
    )

    .withColumn(
        "producer_ingestion_lag_seconds",
        F.col(
            "producer_ingestion_lag_seconds"
        )
        .cast(DoubleType())
    )
)


# ============================================================
# 12. STANDARDIZE SEARCH QUERY
# ============================================================

all_events = (
    all_events
    .withColumn(
        "search_query",
        F.coalesce(
            F.col("search_query"),
            F.col("query")
        )
    )
)


# ============================================================
# 13. TOTAL AVAILABLE EVENTS
# ============================================================

total_events = all_events.count()

print(
    f"Total interaction events available: "
    f"{total_events}"
)


# ============================================================
# 14. INCREMENTAL PROCESSING
# ============================================================

print("\n" + "=" * 80)
print("INCREMENTAL PROCESSING")
print("=" * 80)


state_df = read_parquet_if_exists(
    STATE_PATH
)


if state_df is None:

    print("Load type: INITIAL")

    new_events = all_events

else:

    print("Load type: INCREMENTAL")

    # --------------------------------------------------------
    # Kafka lineage:
    #
    # topic + partition + offset
    #
    # uniquely identifies the consumed Kafka record.
    # --------------------------------------------------------

    new_events = (
        all_events.alias("new")
        .join(
            state_df.alias("state"),

            (
                (F.col("new.topic") ==
                 F.col("state.topic"))

                &

                (F.col("new.partition") ==
                 F.col("state.partition"))

                &

                (F.col("new.offset") ==
                 F.col("state.offset"))
            ),

            "left_anti"
        )
    )


new_event_count = new_events.count()


print(
    f"New interaction events: "
    f"{new_event_count}"
)


# ============================================================
# 15. NO NEW EVENTS
# ============================================================

if new_event_count == 0:

    print("\nNo new interaction events.")

    print(
        "Gold fact_product_interactions "
        "is already up to date."
    )

    spark.stop()

    raise SystemExit(0)


# ============================================================
# 16. NEW EVENT TYPE DISTRIBUTION
# ============================================================

print("\n" + "=" * 80)
print("NEW EVENT TYPE DISTRIBUTION")
print("=" * 80)


(
    new_events
    .groupBy("event_type")
    .count()
    .orderBy("event_type")
    .show(
        100,
        truncate=False
    )
)


# ============================================================
# 17. DATA QUALITY CHECKS
# ============================================================

print("\n" + "=" * 80)
print("DATA QUALITY CHECKS")
print("=" * 80)


# ============================================================
# DQ 1 — NULL EVENT ID
# ============================================================

null_event_id = (
    new_events
    .filter(
        F.col("event_id").isNull()
    )
    .count()
)


print(
    f"Null event_id                  : "
    f"{null_event_id}"
)


# ============================================================
# DQ 2 — DUPLICATE EVENT ID
# ============================================================

duplicate_event_ids = (
    new_events
    .groupBy("event_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print(
    f"Duplicate event_id groups      : "
    f"{duplicate_event_ids}"
)


# ============================================================
# DQ 3 — NULL EVENT TIME
# ============================================================

null_event_time = (
    new_events
    .filter(
        F.col("event_time").isNull()
    )
    .count()
)


print(
    f"Null event_time                : "
    f"{null_event_time}"
)


# ============================================================
# DQ 4 — INVALID CART QUANTITY
# ============================================================

invalid_quantity = (
    new_events
    .filter(
        F.col("event_type").isin(
            "cart_item_added",
            "cart_item_removed",
            "cart_update"
        )
        &
        F.col("quantity").isNotNull()
        &
        (
            F.col("quantity") <= 0
        )
    )
    .count()
)


print(
    f"Invalid cart quantity          : "
    f"{invalid_quantity}"
)


# ============================================================
# DQ 5 — INVALID UNIT PRICE
# ============================================================

invalid_unit_price = (
    new_events
    .filter(
        F.col("unit_price").isNotNull()
        &
        (
            F.col("unit_price") < 0
        )
    )
    .count()
)


print(
    f"Invalid unit_price             : "
    f"{invalid_unit_price}"
)


# ============================================================
# DQ 6 — INVALID PRODUCT PRICE
# ============================================================

invalid_price = (
    new_events
    .filter(
        F.col("price").isNotNull()
        &
        (
            F.col("price") < 0
        )
    )
    .count()
)


print(
    f"Invalid price                  : "
    f"{invalid_price}"
)


# ============================================================
# DQ 7 — PRODUCT EXPECTATION
# ============================================================

product_expected_events = [

    "product_details_view",
    "product_image_view",
    "product_impression",
    "product_view",

    "wishlist_add",
    "wishlist_remove",

    "cart_item_added",
    "cart_item_removed",
    "cart_update",

    "search_result_clicked",
]


missing_product_for_expected = (
    new_events
    .filter(
        F.col("event_type").isin(
            product_expected_events
        )
        &
        F.col("product_id").isNull()
    )
    .count()
)


print(
    "Expected-product events missing "
    "product_id : "
    f"{missing_product_for_expected}"
)


# ============================================================
# DQ 8 — SEARCH WITHOUT QUERY
# ============================================================

search_without_query = (
    new_events
    .filter(
        (
            F.col("event_type") == "search"
        )
        &
        F.col("search_query").isNull()
    )
    .count()
)


print(
    f"Search events missing query   : "
    f"{search_without_query}"
)


# ============================================================
# 18. DQ STATUS
# ============================================================

fatal_dq_failed = (
    null_event_id > 0
    or
    duplicate_event_ids > 0
    or
    null_event_time > 0
)


warning_dq_failed = (
    invalid_quantity > 0
    or
    invalid_unit_price > 0
    or
    invalid_price > 0
    or
    missing_product_for_expected > 0
    or
    search_without_query > 0
)


if fatal_dq_failed:

    print(
        "\nFATAL DATA QUALITY FAILURE"
    )

    print(
        "Gold table will NOT be written."
    )

    spark.stop()

    raise SystemExit(1)


if warning_dq_failed:

    print(
        "\nDATA QUALITY STATUS: "
        "PASS WITH WARNINGS"
    )

else:

    print(
        "\nDATA QUALITY STATUS: PASS"
    )


# ============================================================
# 19. READ EXISTING GOLD
# ============================================================

existing_gold = read_parquet_if_exists(
    GOLD_PATH
)

if existing_gold is not None:
    existing_gold.cache()


# ============================================================
# 20. DEFENSIVE DEDUPLICATION
# ============================================================

new_events = (
    new_events
    .dropDuplicates(
        ["event_id"]
    )
)


# ============================================================
# 21. BUILD FINAL GOLD DATASET
# ============================================================

if existing_gold is None:

    final_gold = new_events

else:

    final_gold = (
        existing_gold
        .unionByName(
            new_events,
            allowMissingColumns=True
        )
    )


# ============================================================
# 22. FINAL EVENT-ID DEDUPLICATION
# ============================================================

final_gold = (
    final_gold
    .dropDuplicates(
        ["event_id"]
    )
)


# ============================================================
# 23. FINAL COLUMN ORDER
# ============================================================

final_gold = (
    final_gold
    .select(
        FACT_COLUMNS
    )
)


# ============================================================
# 24. FINAL GOLD VALIDATION
# ============================================================

final_row_count = final_gold.count()


final_distinct_events = (
    final_gold
    .select("event_id")
    .distinct()
    .count()
)


print("\n" + "=" * 80)
print("FINAL GOLD VALIDATION")
print("=" * 80)


print(
    f"Final Gold rows                : "
    f"{final_row_count}"
)


print(
    f"Distinct event_id              : "
    f"{final_distinct_events}"
)


if final_row_count == final_distinct_events:

    print(
        "Event grain                    : PASS"
    )

else:

    print(
        "Event grain                    : FAIL"
    )


# ============================================================
# 25. USER TYPE DISTRIBUTION
# ============================================================

print("\n" + "=" * 80)
print("USER TYPE DISTRIBUTION")
print("=" * 80)


(
    final_gold
    .groupBy("user_type")
    .count()
    .orderBy("user_type")
    .show(
        truncate=False
    )
)


# ============================================================
# 26. FINAL EVENT TYPE DISTRIBUTION
# ============================================================

print("\n" + "=" * 80)
print("FINAL EVENT TYPE DISTRIBUTION")
print("=" * 80)


(
    final_gold
    .groupBy("event_type")
    .count()
    .orderBy("event_type")
    .show(
        100,
        truncate=False
    )
)


# ============================================================
# 27. PRODUCT POPULATION
# ============================================================

product_population = (
    final_gold
    .filter(
        F.col("product_id").isNotNull()
    )
    .count()
)


print(
    f"\nRows with product_id          : "
    f"{product_population} / "
    f"{final_row_count}"
)


# ============================================================
# 28. SESSION POPULATION
# ============================================================

session_population = (
    final_gold
    .filter(
        F.col("session_id").isNotNull()
    )
    .count()
)


print(
    f"Rows with session_id          : "
    f"{session_population} / "
    f"{final_row_count}"
)


# ============================================================
# 29. CUSTOMER POPULATION
# ============================================================

customer_population = (
    final_gold
    .filter(
        F.col("customer_id").isNotNull()
    )
    .count()
)


print(
    f"Rows with customer_id         : "
    f"{customer_population} / "
    f"{final_row_count}"
)


# ============================================================
# 30. GUEST POPULATION
# ============================================================

guest_population = (
    final_gold
    .filter(
        F.col("anonymous_id").isNotNull()
    )
    .count()
)


print(
    f"Rows with anonymous_id        : "
    f"{guest_population} / "
    f"{final_row_count}"
)


# ============================================================
# 31. KAFKA LINEAGE POPULATION
# ============================================================

lineage_population = (
    final_gold
    .filter(
        F.col("topic").isNotNull()
        &
        F.col("partition").isNotNull()
        &
        F.col("offset").isNotNull()
    )
    .count()
)


print(
    f"Rows with Kafka lineage       : "
    f"{lineage_population} / "
    f"{final_row_count}"
)


# ============================================================
# 32. WRITE GOLD
# ============================================================

print("\n" + "=" * 80)
print("WRITING GOLD")
print("=" * 80)


(
    final_gold
    .write
    .mode("overwrite")
    .parquet(
        GOLD_PATH
    )
)


print(
    f"Gold written successfully: "
    f"{GOLD_PATH}"
)


# ============================================================
# 33. UPDATE INCREMENTAL STATE
# ============================================================

new_lineage = (
    new_events
    .select(
        "topic",
        "partition",
        "offset"
    )
    .dropDuplicates()
)


if state_df is None:

    updated_state = new_lineage

else:

    updated_state = (
        state_df
        .unionByName(
            new_lineage,
            allowMissingColumns=True
        )
        .dropDuplicates(
            [
                "topic",
                "partition",
                "offset"
            ]
        )
    )


(
    updated_state
    .write
    .mode("overwrite")
    .parquet(
        STATE_PATH
    )
)


print(
    f"Lineage state updated: "
    f"{STATE_PATH}"
)


# ============================================================
# 34. FULL-COLUMN SAMPLE
# ============================================================

print("\n" + "=" * 80)
print("FULL-COLUMN SAMPLE")
print("=" * 80)


(
    final_gold
    .orderBy("event_time")
    .show(
        10,
        truncate=False,
        vertical=True
    )
)


# ============================================================
# 35. FINAL SCHEMA
# ============================================================

print("\n" + "=" * 80)
print("FINAL GOLD SCHEMA")
print("=" * 80)


final_gold.printSchema()


# ============================================================
# 36. FINAL STATUS
# ============================================================

print("\n" + "=" * 80)
print("FACT_PRODUCT_INTERACTIONS BUILD COMPLETE")
print("=" * 80)


print(
    f"New events processed      : "
    f"{new_event_count}"
)


print(
    f"Final Gold rows           : "
    f"{final_row_count}"
)


print(
    f"Distinct event IDs        : "
    f"{final_distinct_events}"
)


print(
    f"Gold path                 : "
    f"{GOLD_PATH}"
)


print(
    f"State path                : "
    f"{STATE_PATH}"
)


print("\nIncremental processing    : PASS")
print("Lineage tracking          : PASS")
print("Event grain               : PASS")
print("Gold write                : PASS")
print("State update              : PASS")

print(
    "\nRetailHub fact_product_interactions "
    "is ready."
)


# ============================================================
# 37. STOP SPARK
# ============================================================

spark.stop()