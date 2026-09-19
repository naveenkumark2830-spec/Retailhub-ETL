# ================================================================
# RETAILHUB - BRONZE -> SILVER
# FINAL VERSION
#
# Bronze -> Parse -> Normalize -> Validate -> Deduplicate
#         -> Silver / Quarantine
#
# Key guarantees:
#   1. Bronze is never modified.
#   2. Kafka topic+partition+offset provides physical lineage.
#   3. event_id is checked both within the current batch and against
#      existing Silver/Quarantine, so duplicates cannot re-enter later.
#   4. Raw event_json is preserved in quarantine for replay/debugging.
#   5. Late/arrival lag is retained as metadata; it is not automatically
#      rejected merely because event_time is earlier than ingestion time.
#   6. Silver remains event-level Parquet.
# ================================================================

import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType
)
from config.settings import (
    BRONZE_PATH,
    SILVER_PATH,
    QUARANTINE_PATH,
)

# -----------------------------
# 1. SPARK
# -----------------------------
spark = (
    SparkSession.builder
    .appName("RetailHub-Bronze-To-Silver")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# 2. PATHS
# -----------------------------

# -----------------------------
# 3. JSON SCHEMA
# -----------------------------
shipping_address_schema = StructType([
    StructField("address_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
])

fraud_analytics_schema = StructType([
    StructField("is_ddos_suspect", BooleanType(), True),
    StructField("ip_event_count_10s", IntegerType(), True),
    StructField("device_event_count_10s", IntegerType(), True),
    StructField("threshold_10s", IntegerType(), True),
    StructField("pattern", StringType(), True),
])

context_schema = StructType([
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("device", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("browser", StringType(), True),
])

entity_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("cart_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("shipment_id", StringType(), True),
    StructField("return_id", StringType(), True),
    StructField("refund_id", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("admin_id", StringType(), True),
])

metadata_schema = StructType([
    StructField("query", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("result_count", IntegerType(), True),
    StructField("selected_text", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("section", StringType(), True),
    StructField("page", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("subcategory_id", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("price_min", DoubleType(), True),
    StructField("price_max", DoubleType(), True),
    StructField("rating_min", DoubleType(), True),

    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("source", StringType(), True),

    StructField("subtotal", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("coupon_discount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("shipping_fee", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("order_value", DoubleType(), True),

    StructField("previous_status", StringType(), True),
    StructField("new_status", StringType(), True),
    StructField("reason", StringType(), True),

    StructField("attempt_id", StringType(), True),
    StructField("attempt_number", IntegerType(), True),
    StructField("method", StringType(), True),

    StructField("delivery_option", StringType(), True),
    StructField("option_id", StringType(), True),
    StructField("address_id", StringType(), True),
    StructField("shipping_address", shipping_address_schema),

    StructField("coupon_code", StringType(), True),
    StructField("coupon", StringType(), True),
    StructField("discount_value", DoubleType(), True),
    StructField("discount_min", DoubleType(), True),

    StructField("warehouse_id", StringType(), True),
    StructField("inventory_quantity", IntegerType(), True),
    StructField("stock", IntegerType(), True),

    StructField("tracking_number", StringType(), True),
    StructField("carrier", StringType(), True),

    StructField("return_reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True),

    StructField("rating", IntegerType(), True),
    StructField("review_title", StringType(), True),

    StructField("ticket_id", StringType(), True),
    StructField("banner_id", StringType(), True),
    StructField("membership", StringType(), True),

    StructField("notification_id", StringType(), True),
    StructField("notification_type", StringType(), True),
    StructField("notification_category", StringType(), True),
    StructField("reference_type", StringType(), True),
    StructField("reference_id", StringType(), True),

    StructField("simulated", BooleanType(), True),
    StructField("simulation_mode", StringType(), True),
    StructField("forced_test", BooleanType(), True),
    StructField("ground_truth_fraud", BooleanType(), True),
    StructField("fraud_rule", StringType(), True),
    StructField("fraud_ip", StringType(), True),
    StructField("fraud_analytics", fraud_analytics_schema),

    StructField("order", IntegerType(), True),
])

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("simulation_run_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_version", IntegerType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("event_source", StringType(), True),
    StructField("actor_type", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("anonymous_id", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("context", context_schema),
    StructField("entity", entity_schema),
    StructField("metadata", metadata_schema),
])

topic_to_folder = {
    "retail_admin_events": "admin_events",
    "retail_cart_events": "cart_events",
    "retail_checkout_events": "checkout_events",
    "retail_coupon_events": "coupon_events",
    "retail_delivery_events": "delivery_events",
    "retail_discovery_events": "discovery_events",
    "retail_fulfillment_events": "fulfillment_events",
    "retail_order_events": "order_events",
    "retail_payment_events": "payment_events",
    "retail_return_events": "return_events",
    "retail_system_events": "system_events",
    "retail_user_events": "user_events",
}
allowed_topics = list(topic_to_folder.keys())

valid_event_types = [
    "admin_login", "admin_dashboard_viewed",
    "cart_item_added", "cart_item_removed", "cart_update",
    "address_added", "address_deleted", "address_selected",
    "address_updated", "checkout_abandoned", "checkout_started",
    "delivery_option_selected", "payment_method_selected",
    "coupon_applied", "coupon_rejected", "coupon_removed", "coupon_viewed",
    "category_view", "filter_applied", "page_view", "product_details_view",
    "product_image_change", "product_image_view", "product_impression",
    "product_view", "search", "search_result_clicked",
    "wishlist_add", "wishlist_remove",
    "inventory_released", "inventory_reserved", "order_packed",
    "order_shipped", "shipment_created",
    "delivered", "delivery_failed", "in_transit", "out_for_delivery",
    "order_cancelled", "order_confirmed", "order_created",
    "order_status_updated",
    "payment_failed", "payment_initiated", "payment_retry", "payment_success",
    "refund_failed", "refund_initiated", "refund_success",
    "return_approved", "return_picked_up", "return_received",
    "return_rejected", "return_requested",
    "rating_given", "review_added", "profile_updated", "profile_viewed",
    "invoice_generated", "notification_created",
    "login", "logout", "session_ended", "session_started",
]

# -----------------------------
# 4. READ BRONZE
# -----------------------------
bronze_files = glob.glob(f"{BRONZE_PATH}/*.parquet")
bronze = spark.read.parquet(*bronze_files)
bronze_count = bronze.count()

print("\n" + "=" * 70)
print("RETAILHUB BRONZE -> SILVER")
print("=" * 70)
print(f"Bronze records: {bronze_count}")

# -----------------------------
# 5. BUILD PROCESSED LINEAGE
# -----------------------------
processed_keys = None
existing_event_ids = None

for folder in topic_to_folder.values():
    path = f"{SILVER_PATH}/{folder}"
    try:
        df = spark.read.parquet(path)
        lineage = df.select("topic", "partition", "offset").dropDuplicates()
        processed_keys = lineage if processed_keys is None else processed_keys.unionByName(lineage)

        ids = df.select("event_id").filter(F.col("event_id").isNotNull()).dropDuplicates()
        existing_event_ids = ids if existing_event_ids is None else existing_event_ids.unionByName(ids)
    except Exception:
        pass

try:
    q = spark.read.parquet(QUARANTINE_PATH)
    lineage = q.select("topic", "partition", "offset").dropDuplicates()
    processed_keys = lineage if processed_keys is None else processed_keys.unionByName(lineage)

    ids = q.select("event_id").filter(F.col("event_id").isNotNull()).dropDuplicates()
    existing_event_ids = ids if existing_event_ids is None else existing_event_ids.unionByName(ids)
except Exception:
    pass

if processed_keys is not None:
    processed_keys = processed_keys.dropDuplicates()
    bronze_new = (
        bronze.alias("b")
        .join(
            processed_keys.alias("p"),
            [
                F.col("b.topic") == F.col("p.topic"),
                F.col("b.partition") == F.col("p.partition"),
                F.col("b.offset") == F.col("p.offset"),
            ],
            "left_anti",
        )
    )
else:
    bronze_new = bronze

new_bronze_count = bronze_new.count()
print(f"New/unprocessed Bronze: {new_bronze_count}")

if new_bronze_count == 0:
    print("Silver already up to date.")
    spark.stop()
    raise SystemExit(0)

# -----------------------------
# 6. PARSE
# -----------------------------
parsed = bronze_new.withColumn(
    "parsed",
    F.from_json(F.col("event_json"), event_schema)
)

silver_base = parsed.select(
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("kafka_timestamp"),
    F.col("kafka_key"),
    F.col("event_json"),                 # retained for quarantine only

    F.col("parsed.event_id").alias("event_id"),
    F.col("parsed.simulation_run_id").alias("simulation_run_id"),
    F.col("parsed.event_type").alias("event_type"),
    F.col("parsed.event_version").alias("event_version"),

    F.try_to_timestamp("parsed.event_time").alias("event_time"),
    F.try_to_timestamp("parsed.ingestion_time").alias("event_ingestion_time"),

    F.col("parsed.event_source").alias("event_source"),
    F.col("parsed.actor_type").alias("actor_type"),
    F.col("parsed.session_id").alias("session_id"),
    F.col("parsed.customer_id").alias("customer_id"),
    F.col("parsed.anonymous_id").alias("anonymous_id"),
    F.col("parsed.user_type").alias("user_type"),

    F.col("parsed.context.country").alias("country"),
    F.col("parsed.context.state").alias("state"),
    F.col("parsed.context.city").alias("city"),
    F.col("parsed.context.device").alias("device"),
    F.col("parsed.context.device_id").alias("device_id"),
    F.col("parsed.context.ip_address").alias("ip_address"),
    F.col("parsed.context.browser").alias("browser"),

    F.col("parsed.entity.product_id").alias("product_id"),
    F.col("parsed.entity.cart_id").alias("cart_id"),
    F.col("parsed.entity.order_id").alias("order_id"),
    F.col("parsed.entity.order_item_id").alias("order_item_id"),
    F.col("parsed.entity.payment_id").alias("payment_id"),
    F.col("parsed.entity.shipment_id").alias("shipment_id"),
    F.col("parsed.entity.return_id").alias("return_id"),
    F.col("parsed.entity.refund_id").alias("refund_id"),
    F.col("parsed.entity.review_id").alias("review_id"),
    F.col("parsed.entity.admin_id").alias("admin_id"),

    F.col("parsed.metadata.query").alias("query"),
    F.col("parsed.metadata.search_query").alias("search_query"),
    F.col("parsed.metadata.result_count").alias("result_count"),
    F.col("parsed.metadata.selected_text").alias("selected_text"),
    F.col("parsed.metadata.position").alias("position"),
    F.col("parsed.metadata.section").alias("section"),
    F.col("parsed.metadata.page").alias("page"),
    F.col("parsed.metadata.brand").alias("brand"),
    F.col("parsed.metadata.category").alias("category"),
    F.col("parsed.metadata.category_id").alias("category_id"),
    F.col("parsed.metadata.category_name").alias("category_name"),
    F.col("parsed.metadata.subcategory_id").alias("subcategory_id"),
    F.col("parsed.metadata.image_url").alias("image_url"),
    F.col("parsed.metadata.price").alias("price"),
    F.col("parsed.metadata.price_min").alias("price_min"),
    F.col("parsed.metadata.price_max").alias("price_max"),
    F.col("parsed.metadata.rating_min").alias("rating_min"),

    F.col("parsed.metadata.quantity").alias("quantity"),
    F.col("parsed.metadata.unit_price").alias("unit_price"),
    F.col("parsed.metadata.source").alias("source"),

    F.col("parsed.metadata.subtotal").alias("subtotal"),
    F.col("parsed.metadata.discount_amount").alias("discount_amount"),
    F.col("parsed.metadata.coupon_discount").alias("coupon_discount"),
    F.col("parsed.metadata.tax_amount").alias("tax_amount"),
    F.col("parsed.metadata.shipping_fee").alias("shipping_fee"),
    F.col("parsed.metadata.delivery_fee").alias("delivery_fee"),
    F.col("parsed.metadata.total_amount").alias("total_amount"),
    F.col("parsed.metadata.amount").alias("amount"),
    F.col("parsed.metadata.currency").alias("currency"),
    F.col("parsed.metadata.payment_method").alias("payment_method"),
    F.col("parsed.metadata.payment_status").alias("payment_status"),
    F.col("parsed.metadata.order_value").alias("order_value"),

    F.col("parsed.metadata.previous_status").alias("previous_status"),
    F.col("parsed.metadata.new_status").alias("new_status"),
    F.col("parsed.metadata.reason").alias("reason"),

    F.col("parsed.metadata.attempt_id").alias("attempt_id"),
    F.col("parsed.metadata.attempt_number").alias("attempt_number"),
    F.col("parsed.metadata.method").alias("method"),

    F.col("parsed.metadata.delivery_option").alias("delivery_option"),
    F.col("parsed.metadata.option_id").alias("option_id"),
    F.col("parsed.metadata.address_id").alias("address_id"),
    F.col("parsed.metadata.shipping_address.address_id").alias("shipping_address_id"),
    F.col("parsed.metadata.shipping_address.city").alias("shipping_city"),
    F.col("parsed.metadata.shipping_address.state").alias("shipping_state"),
    F.col("parsed.metadata.shipping_address.country").alias("shipping_country"),

    F.col("parsed.metadata.coupon_code").alias("coupon_code"),
    F.col("parsed.metadata.coupon").alias("coupon"),
    F.col("parsed.metadata.discount_value").alias("discount_value"),
    F.col("parsed.metadata.discount_min").alias("discount_min"),

    F.col("parsed.metadata.warehouse_id").alias("warehouse_id"),
    F.col("parsed.metadata.inventory_quantity").alias("inventory_quantity"),
    F.col("parsed.metadata.stock").alias("stock"),

    F.col("parsed.metadata.tracking_number").alias("tracking_number"),
    F.col("parsed.metadata.carrier").alias("carrier"),

    F.col("parsed.metadata.return_reason").alias("return_reason"),
    F.col("parsed.metadata.refund_amount").alias("refund_amount"),

    F.col("parsed.metadata.rating").alias("rating"),
    F.col("parsed.metadata.review_title").alias("review_title"),

    F.col("parsed.metadata.ticket_id").alias("ticket_id"),
    F.col("parsed.metadata.banner_id").alias("banner_id"),
    F.col("parsed.metadata.membership").alias("membership"),

    F.col("parsed.metadata.notification_id").alias("notification_id"),
    F.col("parsed.metadata.notification_type").alias("notification_type"),
    F.col("parsed.metadata.notification_category").alias("notification_category"),
    F.col("parsed.metadata.reference_type").alias("reference_type"),
    F.col("parsed.metadata.reference_id").alias("reference_id"),

    F.col("parsed.metadata.simulated").alias("simulated"),
    F.col("parsed.metadata.simulation_mode").alias("simulation_mode"),
    F.col("parsed.metadata.forced_test").alias("forced_test"),
    F.col("parsed.metadata.ground_truth_fraud").alias("ground_truth_fraud"),
    F.col("parsed.metadata.fraud_rule").alias("fraud_rule"),
    F.col("parsed.metadata.fraud_ip").alias("fraud_ip"),

    F.col("parsed.metadata.fraud_analytics.is_ddos_suspect").alias("sim_ddos_suspect"),
    F.col("parsed.metadata.fraud_analytics.ip_event_count_10s").alias("sim_ip_event_count_10s"),
    F.col("parsed.metadata.fraud_analytics.device_event_count_10s").alias("sim_device_event_count_10s"),
    F.col("parsed.metadata.fraud_analytics.threshold_10s").alias("sim_threshold_10s"),
    F.col("parsed.metadata.fraud_analytics.pattern").alias("sim_fraud_pattern"),

    F.col("parsed.metadata.order").alias("metadata_order"),
    F.col("bronze_ingestion_time"),
    F.col("parsed").alias("_parsed_struct"),
)

# -----------------------------
# 7. NORMALIZATION
# -----------------------------
string_columns = [
    "event_id", "simulation_run_id", "event_type", "event_source",
    "actor_type", "session_id", "customer_id", "anonymous_id", "user_type",
    "country", "state", "city", "device", "device_id", "ip_address",
    "browser", "product_id", "cart_id", "order_id", "order_item_id",
    "payment_id", "shipment_id", "return_id", "refund_id", "review_id",
    "admin_id", "query", "search_query", "selected_text", "section", "page",
    "brand", "category", "category_id", "category_name", "subcategory_id",
    "image_url", "source", "currency", "payment_method", "payment_status",
    "previous_status", "new_status", "reason", "attempt_id", "method",
    "delivery_option", "option_id", "address_id", "shipping_address_id",
    "shipping_city", "shipping_state", "shipping_country", "coupon_code",
    "coupon", "warehouse_id", "tracking_number", "carrier", "return_reason",
    "review_title", "ticket_id", "banner_id", "membership", "notification_id",
    "notification_type", "notification_category", "reference_type",
    "reference_id", "simulation_mode", "fraud_rule", "fraud_ip",
    "sim_fraud_pattern",
]

for c in string_columns:
    silver_base = silver_base.withColumn(
        c,
        F.when(F.trim(F.col(c)) == "", F.lit(None))
         .otherwise(F.trim(F.col(c)))
    )

silver_base = (
    silver_base
    .withColumn("event_type", F.lower("event_type"))
    .withColumn("event_source", F.lower("event_source"))
    .withColumn("actor_type", F.lower("actor_type"))
    .withColumn("user_type", F.lower("user_type"))
    .withColumn("payment_method", F.lower("payment_method"))
    .withColumn("payment_status", F.lower("payment_status"))
    .withColumn("currency", F.upper("currency"))
)

# -----------------------------
# 8. EVENT-TIME METADATA
# -----------------------------
silver_base = (
    silver_base
    .withColumn(
        "has_event_arrival_delay",
        F.when(
            F.col("event_time").isNotNull() &
            F.col("bronze_ingestion_time").isNotNull() &
            (F.col("event_time") < F.col("bronze_ingestion_time")),
            True
        ).otherwise(False)
    )
    .withColumn(
        "event_arrival_lag_seconds",
        F.when(
            F.col("event_time").isNotNull() &
            F.col("bronze_ingestion_time").isNotNull(),
            F.col("bronze_ingestion_time").cast("double") -
            F.col("event_time").cast("double")
        )
    )
    .withColumn(
        "producer_ingestion_lag_seconds",
        F.when(
            F.col("event_time").isNotNull() &
            F.col("event_ingestion_time").isNotNull(),
            F.col("event_ingestion_time").cast("double") -
            F.col("event_time").cast("double")
        )
    )
)

# -----------------------------
# 9. VALIDATION
# -----------------------------
validated = (
    silver_base
    .withColumn(
        "quarantine_reason",
        F.when(F.col("_parsed_struct").isNull(), "MALFORMED_JSON")
        .when(
            F.col("topic").isNull() |
            (~F.col("topic").isin(allowed_topics)),
            "UNSUPPORTED_TOPIC"
        )
        .when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
        .when(F.col("event_type").isNull(), "MISSING_EVENT_TYPE")
        .when(
            ~F.col("event_type").isin(valid_event_types),
            "UNKNOWN_EVENT_TYPE"
        )
        .when(F.col("event_time").isNull(), "INVALID_EVENT_TIME")
        .when(
            F.col("event_ingestion_time").isNull(),
            "INVALID_INGESTION_TIME"
        )
        .when(F.col("event_version").isNull(), "MISSING_EVENT_VERSION")
        .when(F.col("event_version") <= 0, "INVALID_EVENT_VERSION")
        .when(
            F.col("quantity").isNotNull() & (F.col("quantity") <= 0),
            "INVALID_QUANTITY"
        )
        .when(
            F.col("unit_price").isNotNull() &
            (F.isnan("unit_price") | (F.col("unit_price") < 0)),
            "INVALID_UNIT_PRICE"
        )
        .when(
            F.col("price").isNotNull() &
            (F.isnan("price") | (F.col("price") < 0)),
            "INVALID_PRICE"
        )
        .when(
            F.col("price_min").isNotNull() &
            (F.isnan("price_min") | (F.col("price_min") < 0)),
            "INVALID_PRICE_MIN"
        )
        .when(
            F.col("price_max").isNotNull() &
            (F.isnan("price_max") | (F.col("price_max") < 0)),
            "INVALID_PRICE_MAX"
        )
        .when(
            F.col("price_min").isNotNull() &
            F.col("price_max").isNotNull() &
            (F.col("price_min") > F.col("price_max")),
            "INVALID_PRICE_RANGE"
        )
        .when(
            F.col("amount").isNotNull() &
            (F.isnan("amount") | (F.col("amount") < 0)),
            "INVALID_AMOUNT"
        )
        .when(
            F.col("total_amount").isNotNull() &
            (F.isnan("total_amount") | (F.col("total_amount") < 0)),
            "INVALID_TOTAL_AMOUNT"
        )
        .when(
            F.col("subtotal").isNotNull() &
            (F.isnan("subtotal") | (F.col("subtotal") < 0)),
            "INVALID_SUBTOTAL"
        )
        .when(
            F.col("discount_amount").isNotNull() &
            (F.isnan("discount_amount") | (F.col("discount_amount") < 0)),
            "INVALID_DISCOUNT_AMOUNT"
        )
        .when(
            F.col("coupon_discount").isNotNull() &
            (F.isnan("coupon_discount") | (F.col("coupon_discount") < 0)),
            "INVALID_COUPON_DISCOUNT"
        )
        .when(
            F.col("tax_amount").isNotNull() &
            (F.isnan("tax_amount") | (F.col("tax_amount") < 0)),
            "INVALID_TAX_AMOUNT"
        )
        .when(
            F.col("shipping_fee").isNotNull() &
            (F.isnan("shipping_fee") | (F.col("shipping_fee") < 0)),
            "INVALID_SHIPPING_FEE"
        )
        .when(
            F.col("delivery_fee").isNotNull() &
            (F.isnan("delivery_fee") | (F.col("delivery_fee") < 0)),
            "INVALID_DELIVERY_FEE"
        )
        .when(
            F.col("refund_amount").isNotNull() &
            (F.isnan("refund_amount") | (F.col("refund_amount") < 0)),
            "INVALID_REFUND_AMOUNT"
        )
        .when(
            F.col("rating").isNotNull() &
            ((F.col("rating") < 1) | (F.col("rating") > 5)),
            "INVALID_RATING"
        )
        .when(
            F.col("result_count").isNotNull() & (F.col("result_count") < 0),
            "INVALID_RESULT_COUNT"
        )
        .when(
            F.col("position").isNotNull() & (F.col("position") <= 0),
            "INVALID_POSITION"
        )
        .when(
            F.col("inventory_quantity").isNotNull() &
            (F.col("inventory_quantity") < 0),
            "INVALID_INVENTORY_QUANTITY"
        )
        .when(
            F.col("stock").isNotNull() & (F.col("stock") < 0),
            "INVALID_STOCK"
        )
        .when(
            F.col("attempt_number").isNotNull() &
            (F.col("attempt_number") <= 0),
            "INVALID_ATTEMPT_NUMBER"
        )
        .when(
            F.col("session_id").isNull() &
            F.col("event_type").isin(
                "page_view", "product_view", "product_details_view",
                "product_image_view", "product_image_change",
                "product_impression", "search", "search_result_clicked",
                "filter_applied", "category_view", "wishlist_add",
                "wishlist_remove", "cart_item_added", "cart_item_removed",
                "cart_update", "checkout_started", "checkout_abandoned",
                "login", "logout", "session_started", "session_ended",
            ),
            "MISSING_SESSION_ID"
        )
    )
)

# -----------------------------
# 10. CURRENT-BATCH DUPLICATES
# -----------------------------
window_spec = (
    Window.partitionBy("event_id")
    .orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("partition").desc(),
        F.col("offset").desc(),
    )
)

with_duplicates = (
    validated
    .withColumn("_row_number", F.row_number().over(window_spec))
    .withColumn(
        "quarantine_reason",
        F.when(
            F.col("_row_number") > 1,
            "DUPLICATE_EVENT_ID"
        ).otherwise(F.col("quarantine_reason"))
    )
)

# -----------------------------
# 11. CROSS-RUN DUPLICATE IDS
# -----------------------------
if existing_event_ids is not None:
    with_duplicates = (
        with_duplicates
        .join(
            existing_event_ids.select(
                F.col("event_id").alias("_existing_event_id")
            ),
            with_duplicates.event_id == F.col("_existing_event_id"),
            "left"
        )
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("_existing_event_id").isNotNull(),
                "DUPLICATE_EVENT_ID"
            ).otherwise(F.col("quarantine_reason"))
        )
        .drop("_existing_event_id")
    )

# -----------------------------
# 12. VALID / QUARANTINE
# -----------------------------
valid_silver = (
    with_duplicates
    .filter(F.col("quarantine_reason").isNull())
    .drop("_row_number", "quarantine_reason", "_parsed_struct", "event_json")
    .withColumn("silver_ingestion_time", F.current_timestamp())
)

quarantine = (
    with_duplicates
    .filter(F.col("quarantine_reason").isNotNull())
    .drop("_row_number", "_parsed_struct")
    .withColumn("quarantine_time", F.current_timestamp())
)

# -----------------------------
# 13. WRITE
# -----------------------------
valid_count = valid_silver.count()
quarantine_count = quarantine.count()

for topic, folder in topic_to_folder.items():
    topic_df = valid_silver.filter(F.col("topic") == topic)
    n = topic_df.count()
    if n:
        topic_df.write.mode("append").parquet(f"{SILVER_PATH}/{folder}")
        print(f"{topic:30} -> {n:6} records")

if quarantine_count:
    quarantine.write.mode("append").parquet(QUARANTINE_PATH)

# -----------------------------
# 14. REPORT
# -----------------------------
print("\n" + "=" * 70)
print("SILVER PROCESSING REPORT")
print("=" * 70)
print(f"Bronze records read       : {bronze_count}")
print(f"New/unprocessed records   : {new_bronze_count}")
print(f"Valid Silver records      : {valid_count}")
print(f"Quarantine records        : {quarantine_count}")
print(f"Silver + Quarantine       : {valid_count + quarantine_count}")

if valid_count + quarantine_count != new_bronze_count:
    raise Exception("RECONCILIATION FAILED")

print("RECONCILIATION             : PASS")

print("\n=== QUARANTINE REASONS ===")
if quarantine_count:
    quarantine.groupBy("quarantine_reason").count().orderBy("quarantine_reason").show(100, False)

print("\n=== SILVER EVENT DISTRIBUTION ===")
valid_silver.groupBy("event_type").count().orderBy("event_type").show(100, False)

print("\n=== FINAL SILVER SCHEMA ===")
valid_silver.printSchema()

spark.stop()
