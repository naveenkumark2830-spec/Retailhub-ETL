from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

from config.settings import (
    KAFKA_BROKER,
    FRAUD_TOPIC_PATTERN,
    FRAUD_CHECKPOINT_PATH,
)


# ============================================================
# KAFKA / CHECKPOINT
# ============================================================

KAFKA_BOOTSTRAP = KAFKA_BROKER
TOPIC_PATTERN = FRAUD_TOPIC_PATTERN
CHECKPOINT = FRAUD_CHECKPOINT_PATH


# ============================================================
# FRAUD RULE THRESHOLDS
# ============================================================

# 10-second traffic protection
DDOS_EVENT_THRESHOLD = 30

# 10-minute velocity
BOT_EVENT_THRESHOLD = 20
BOT_SESSION_THRESHOLD = 4

# Authentication
FAILED_LOGIN_THRESHOLD = 5
MULTI_IP_LOGIN_THRESHOLD = 3

# Payment
PAYMENT_FAILURE_THRESHOLD = 4
PAYMENT_METHOD_THRESHOLD = 3

# Orders
HIGH_VALUE_ORDER_THRESHOLD = 50000.0
HIGH_VALUE_ORDER_COUNT = 3

# Multi-account abuse
MULTI_ACCOUNT_THRESHOLD = 5

# Coupon
COUPON_CUSTOMER_THRESHOLD = 3

# Refund
REFUND_24H_THRESHOLD = 3

# Checkout/order velocity
CHECKOUT_VELOCITY_THRESHOLD = 5


# ============================================================
# EVENT TYPES
# ============================================================

FAILED_LOGIN_EVENTS = [
    "LOGIN_FAILED",
    "LOGIN_FAILURE",
]

PAYMENT_FAILURE_EVENTS = [
    "PAYMENT_FAILED",
    "PAYMENT_FAILURE",
]

ORDER_EVENTS = [
    "ORDER_CREATED",
    "ORDER_PLACED",
]

CHECKOUT_EVENTS = [
    "CHECKOUT_STARTED",
    "ORDER_CREATED",
]

REFUND_EVENTS = [
    "RETURN_REQUESTED",
    "REFUND_INITIATED",
    "REFUND_COMPLETED",
]

SENSITIVE_CHANGE_EVENTS = [
    "PASSWORD_CHANGED",
    "EMAIL_CHANGED",
    "PHONE_CHANGED",
    "PAYMENT_METHOD_CHANGED",
]

LOGIN_EVENTS = [
    "LOGIN",
    "LOGIN_SUCCESS",
]


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub-FraudGuard")
    .config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 80)
print("RETAILHUB FRAUDGUARD - DETERMINISTIC FRAUD ENGINE")
print("=" * 80)

print(f"Kafka                 : {KAFKA_BOOTSTRAP}")
print(f"Topic pattern         : {TOPIC_PATTERN}")
print(f"Checkpoint            : {CHECKPOINT}")

print()
print("Rules enabled:")
print("  - Brute-force login")
print("  - Multi-IP login attack")
print("  - Payment failure velocity")
print("  - Multiple payment methods")
print("  - High-value transaction")
print("  - High-value order velocity")
print("  - New device + sensitive change")
print("  - Account takeover sequence")
print("  - Multi-account device")
print("  - Multi-account IP")
print("  - Coupon abuse")
print("  - Refund abuse")
print("  - Bot / scraper")
print("  - DDoS / IP flood")
print("  - Checkout velocity")

print("=" * 80)


# ============================================================
# EVENT SCHEMA
# ============================================================

event_schema = StructType([

    StructField(
        "event_id",
        StringType(),
        True,
    ),

    StructField(
        "event_time",
        StringType(),
        True,
    ),

    StructField(
        "session_id",
        StringType(),
        True,
    ),

    StructField(
        "customer_id",
        StringType(),
        True,
    ),

    StructField(
        "event_type",
        StringType(),
        True,
    ),

    # --------------------------------------------------------
    # Context
    # --------------------------------------------------------

    StructField(
        "context",
        StructType([

            StructField(
                "ip_address",
                StringType(),
                True,
            ),

            StructField(
                "device_id",
                StringType(),
                True,
            ),

            StructField(
                "browser",
                StringType(),
                True,
            ),

            StructField(
                "user_agent",
                StringType(),
                True,
            ),

            StructField(
                "country",
                StringType(),
                True,
            ),

            StructField(
                "state",
                StringType(),
                True,
            ),

            StructField(
                "city",
                StringType(),
                True,
            ),
        ]),
        True,
    ),

    # --------------------------------------------------------
    # Entity
    # --------------------------------------------------------

    StructField(
        "entity",
        StructType([

            StructField(
                "order_id",
                StringType(),
                True,
            ),

            StructField(
                "product_id",
                StringType(),
                True,
            ),

            StructField(
                "payment_id",
                StringType(),
                True,
            ),

            StructField(
                "coupon_id",
                StringType(),
                True,
            ),
        ]),
        True,
    ),

    # --------------------------------------------------------
    # Payment
    # --------------------------------------------------------

    StructField(
        "payment",
        StructType([

            StructField(
                "payment_method_id",
                StringType(),
                True,
            ),

            StructField(
                "payment_method_type",
                StringType(),
                True,
            ),
        ]),
        True,
    ),

    # --------------------------------------------------------
    # Transaction
    # --------------------------------------------------------

    StructField(
        "amount",
        DoubleType(),
        True,
    ),

    StructField(
        "currency",
        StringType(),
        True,
    ),
])


# ============================================================
# KAFKA SOURCE
# ============================================================

raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP,
    )
    .option(
        "subscribePattern",
        TOPIC_PATTERN,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .option(
        "failOnDataLoss",
        "false",
    )
    .load()
)


# ============================================================
# PARSE JSON
# ============================================================

parsed = raw.select(
    "topic",
    "partition",
    "offset",
    "timestamp",

    F.from_json(
        F.col("value").cast("string"),
        event_schema,
    ).alias("event"),
)


# ============================================================
# NORMALIZE EVENT
# ============================================================

events = parsed.select(

    "topic",
    "partition",
    "offset",
    "timestamp",

    F.col("event.event_id").alias(
        "event_id"
    ),

    F.expr(
        "try_to_timestamp(event.event_time)"
    ).alias(
        "event_time"
    ),

    F.col("event.session_id").alias(
        "session_id"
    ),

    F.col("event.customer_id").alias(
        "customer_id"
    ),

    F.col("event.event_type").alias(
        "event_type"
    ),

    # Context
    F.col(
        "event.context.ip_address"
    ).alias(
        "ip_address"
    ),

    F.col(
        "event.context.device_id"
    ).alias(
        "device_id"
    ),

    F.col(
        "event.context.browser"
    ).alias(
        "browser"
    ),

    F.col(
        "event.context.user_agent"
    ).alias(
        "user_agent"
    ),

    F.col(
        "event.context.country"
    ).alias(
        "country"
    ),

    F.col(
        "event.context.state"
    ).alias(
        "state"
    ),

    F.col(
        "event.context.city"
    ).alias(
        "city"
    ),

    # Entity
    F.col(
        "event.entity.order_id"
    ).alias(
        "order_id"
    ),

    F.col(
        "event.entity.product_id"
    ).alias(
        "product_id"
    ),

    F.col(
        "event.entity.payment_id"
    ).alias(
        "payment_id"
    ),

    F.col(
        "event.entity.coupon_id"
    ).alias(
        "coupon_id"
    ),

    # Payment
    F.col(
        "event.payment.payment_method_id"
    ).alias(
        "payment_method_id"
    ),

    F.col(
        "event.payment.payment_method_type"
    ).alias(
        "payment_method_type"
    ),

    # Transaction
    F.col(
        "event.amount"
    ).alias(
        "amount"
    ),

    F.col(
        "event.currency"
    ).alias(
        "currency"
    ),
)


# ============================================================
# BASIC VALIDATION
# ============================================================

valid_events = (
    events
    .filter(
        F.col("event_id").isNotNull()
    )
    .filter(
        F.col("event_time").isNotNull()
    )
)


# ============================================================
# 1. IP VELOCITY / DDOS / BOT
#
# Key:
#     IP
#
# Window:
#     10 seconds for DDoS
#     10 minutes for bot behaviour
# ============================================================

ip_window = (
    valid_events

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "10 seconds",
            "5 seconds",
        ),

        F.col("ip_address"),
    )

    .agg(

        F.count("*").alias(
            "event_count"
        ),

        F.countDistinct(
            "session_id"
        ).alias(
            "session_count"
        ),

        F.countDistinct(
            "customer_id"
        ).alias(
            "customer_count"
        ),

        F.countDistinct(
            "device_id"
        ).alias(
            "device_count"
        ),
    )
)


ip_signals = (
    ip_window

    .withColumn(
        "ddos",
        F.when(
            F.col("event_count")
            > DDOS_EVENT_THRESHOLD,
            F.lit("DDOS"),
        ),
    )

    .withColumn(
        "scraper",
        F.when(
            (
                F.col("event_count")
                >= BOT_EVENT_THRESHOLD
            )
            &
            (
                F.col("session_count")
                >= BOT_SESSION_THRESHOLD
            ),
            F.lit("BOT_OR_SCRAPER"),
        ),
    )

    .withColumn(
        "multi_account_ip",
        F.when(
            F.col("customer_count")
            >= MULTI_ACCOUNT_THRESHOLD,
            F.lit("MULTI_ACCOUNT_IP"),
        ),
    )
)


# ============================================================
# 2. CUSTOMER AUTHENTICATION RULES
#
# Key:
#     customer_id
#
# Window:
#     5 minutes
# ============================================================

customer_auth = (
    valid_events

    .filter(
        F.col("customer_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "5 minutes",
            "1 minute",
        ),

        F.col("customer_id"),
    )

    .agg(

        F.sum(
            F.when(
                F.col("event_type").isin(
                    FAILED_LOGIN_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "failed_logins"
        ),

        F.countDistinct(
            F.when(
                F.col("event_type").isin(
                    FAILED_LOGIN_EVENTS
                ),
                F.col("ip_address"),
            )
        ).alias(
            "failed_login_ips"
        ),

        F.countDistinct(
            F.col("device_id")
        ).alias(
            "device_count"
        ),

        F.sum(
            F.when(
                F.col("event_type").isin(
                    SENSITIVE_CHANGE_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "sensitive_changes"
        ),

        F.countDistinct(
            F.when(
                F.col("event_type").isin(
                    LOGIN_EVENTS
                ),
                F.col("device_id"),
            )
        ).alias(
            "login_devices"
        ),
    )
)


customer_auth_signals = (
    customer_auth

    .withColumn(
        "brute_force",
        F.when(
            F.col("failed_logins")
            >= FAILED_LOGIN_THRESHOLD,
            F.lit("BRUTE_FORCE_LOGIN"),
        ),
    )

    .withColumn(
        "multi_ip_login",
        F.when(
            F.col("failed_login_ips")
            >= MULTI_IP_LOGIN_THRESHOLD,
            F.lit("MULTI_IP_LOGIN_ATTACK"),
        ),
    )
)


# ============================================================
# 3. PAYMENT RULES
#
# Key:
#     customer_id
#
# Window:
#     10 minutes
# ============================================================

payment_window = (
    valid_events

    .filter(
        F.col("customer_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "10 minutes",
            "1 minute",
        ),

        F.col("customer_id"),
    )

    .agg(

        F.sum(
            F.when(
                F.col("event_type").isin(
                    PAYMENT_FAILURE_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "payment_failures"
        ),

        F.countDistinct(
            F.col("payment_method_id")
        ).alias(
            "payment_methods"
        ),
    )
)


payment_signals = (
    payment_window

    .withColumn(
        "payment_failure_velocity",
        F.when(
            F.col("payment_failures")
            >= PAYMENT_FAILURE_THRESHOLD,
            F.lit("PAYMENT_FAILURE_VELOCITY"),
        ),
    )

    .withColumn(
        "multiple_payment_methods",
        F.when(
            (
                F.col("payment_methods")
                >= PAYMENT_METHOD_THRESHOLD
            )
            &
            (
                F.col("payment_failures")
                >= 1
            ),
            F.lit("MULTIPLE_PAYMENT_METHODS"),
        ),
    )
)


# ============================================================
# 4. ORDER / HIGH VALUE RULES
#
# Key:
#     customer_id
#
# Window:
#     10 minutes
# ============================================================

order_window = (
    valid_events

    .filter(
        F.col("customer_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "10 minutes",
            "1 minute",
        ),

        F.col("customer_id"),
    )

    .agg(

        F.sum(
            F.when(
                F.col("amount")
                > HIGH_VALUE_ORDER_THRESHOLD,
                1,
            ).otherwise(0)
        ).alias(
            "high_value_orders"
        ),

        F.sum(
            F.when(
                F.col("event_type").isin(
                    CHECKOUT_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "checkout_count"
        ),

        F.sum(
            F.when(
                F.col("event_type").isin(
                    ORDER_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "order_count"
        ),

        F.max(
            F.when(
                F.col("event_type").isin(
                    ORDER_EVENTS
                ),
                F.col("amount"),
            )
        ).alias(
            "max_order_amount"
        ),
    )
)


order_signals = (
    order_window

    .withColumn(
        "high_value",
        F.when(
            F.col("max_order_amount")
            > HIGH_VALUE_ORDER_THRESHOLD,
            F.lit("HIGH_VALUE_TRANSACTION"),
        ),
    )

    .withColumn(
        "high_value_velocity",
        F.when(
            F.col("high_value_orders")
            >= HIGH_VALUE_ORDER_COUNT,
            F.lit("HIGH_VALUE_ORDER_VELOCITY"),
        ),
    )

    .withColumn(
        "checkout_velocity",
        F.when(
            F.col("checkout_count")
            >= CHECKOUT_VELOCITY_THRESHOLD,
            F.lit("CHECKOUT_VELOCITY"),
        ),
    )
)


# ============================================================
# 5. DEVICE / MULTI-ACCOUNT RULE
#
# Key:
#     device_id
#
# Window:
#     10 minutes
# ============================================================

device_window = (
    valid_events

    .filter(
        F.col("device_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "10 minutes",
            "1 minute",
        ),

        F.col("device_id"),
    )

    .agg(

        F.countDistinct(
            "customer_id"
        ).alias(
            "customer_count"
        ),

        F.countDistinct(
            "payment_method_id"
        ).alias(
            "payment_method_count"
        ),

        F.count(
            F.when(
                F.col("event_type").isin(
                    ORDER_EVENTS
                ),
                True,
            )
        ).alias(
            "order_count"
        ),
    )
)


device_signals = (
    device_window

    .withColumn(
        "multi_account",
        F.when(
            F.col("customer_count")
            >= MULTI_ACCOUNT_THRESHOLD,
            F.lit("MULTI_ACCOUNT_DEVICE"),
        ),
    )

    .withColumn(
        "device_payment_abuse",
        F.when(
            (
                F.col("customer_count")
                >= MULTI_ACCOUNT_THRESHOLD
            )
            &
            (
                F.col("payment_method_count")
                >= PAYMENT_METHOD_THRESHOLD
            )
            &
            (
                F.col("order_count")
                >= 1
            ),
            F.lit(
                "MULTI_ACCOUNT_PAYMENT_ABUSE"
            ),
        ),
    )
)


# ============================================================
# 6. COUPON ABUSE
#
# Key:
#     coupon_id
#
# Window:
#     10 minutes
# ============================================================

coupon_window = (
    valid_events

    .filter(
        F.col("coupon_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "10 minutes",
            "1 minute",
        ),

        F.col("coupon_id"),
    )

    .agg(

        F.countDistinct(
            "customer_id"
        ).alias(
            "customer_count"
        ),

        F.countDistinct(
            "device_id"
        ).alias(
            "device_count"
        ),

        F.count("*").alias(
            "coupon_usage_count"
        ),
    )
)


coupon_signals = (
    coupon_window

    .withColumn(
        "coupon_abuse",
        F.when(
            (
                F.col("customer_count")
                >= COUPON_CUSTOMER_THRESHOLD
            )
            &
            (
                F.col("device_count")
                <= 2
            ),
            F.lit("COUPON_ABUSE"),
        ),
    )
)


# ============================================================
# 7. REFUND ABUSE
#
# Key:
#     customer_id
#
# Window:
#     24 hours
# ============================================================

refund_window = (
    valid_events

    .filter(
        F.col("customer_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "24 hours",
            "1 hour",
        ),

        F.col("customer_id"),
    )

    .agg(

        F.sum(
            F.when(
                F.col("event_type").isin(
                    REFUND_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "refund_count"
        ),
    )
)


refund_signals = (
    refund_window

    .withColumn(
        "refund_abuse",
        F.when(
            F.col("refund_count")
            >= REFUND_24H_THRESHOLD,
            F.lit("REFUND_ABUSE"),
        ),
    )
)


# ============================================================
# 8. ACCOUNT TAKEOVER SIGNAL
#
# Deterministic evidence combination:
#
# failed login
#      +
# new device / multiple devices
#      +
# sensitive account change
#      +
# order
#
# This is a signal, not an AI decision.
# ============================================================

ato_window = (
    valid_events

    .filter(
        F.col("customer_id").isNotNull()
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )

    .groupBy(
        F.window(
            "event_time",
            "10 minutes",
            "1 minute",
        ),

        F.col("customer_id"),
    )

    .agg(

        F.sum(
            F.when(
                F.col("event_type").isin(
                    FAILED_LOGIN_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "failed_logins"
        ),

        F.countDistinct(
            F.when(
                F.col("event_type").isin(
                    LOGIN_EVENTS
                ),
                F.col("device_id"),
            )
        ).alias(
            "login_devices"
        ),

        F.sum(
            F.when(
                F.col("event_type").isin(
                    SENSITIVE_CHANGE_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "sensitive_changes"
        ),

        F.sum(
            F.when(
                F.col("event_type").isin(
                    ORDER_EVENTS
                ),
                1,
            ).otherwise(0)
        ).alias(
            "orders"
        ),
    )
)


ato_signals = (
    ato_window

    .withColumn(
        "account_takeover",
        F.when(
            (
                F.col("failed_logins")
                >= 1
            )
            &
            (
                F.col("login_devices")
                >= 2
            )
            &
            (
                F.col("sensitive_changes")
                >= 1
            )
            &
            (
                F.col("orders")
                >= 1
            ),
            F.lit(
                "ACCOUNT_TAKEOVER_SEQUENCE"
            ),
        ),
    )
)


# ============================================================
# CREATE A COMMON ALERT SCHEMA
# ============================================================

def signal_output(
    dataframe,
    signal_column,
    entity_column,
    rule_source,
):
    return (
        dataframe
        .filter(
            F.col(signal_column).isNotNull()
        )
        .select(
            F.col(entity_column).alias(
                "entity_key"
            ),

            F.col("window").alias(
                "detection_window"
            ),

            F.col(signal_column).alias(
                "fraud_type"
            ),

            F.lit(rule_source).alias(
                "rule_source"
            ),

            F.current_timestamp().alias(
                "alert_time"
            ),
        )
    )


# ============================================================
# CONVERT ALL SIGNALS TO COMMON FORMAT
# ============================================================

all_signals = [

    signal_output(
        ip_signals,
        "ddos",
        "ip_address",
        "IP_VELOCITY",
    ),

    signal_output(
        ip_signals,
        "scraper",
        "ip_address",
        "BOT_DETECTION",
    ),

    signal_output(
        ip_signals,
        "multi_account_ip",
        "ip_address",
        "MULTI_ACCOUNT_IP",
    ),

    signal_output(
        customer_auth_signals,
        "brute_force",
        "customer_id",
        "AUTHENTICATION",
    ),

    signal_output(
        customer_auth_signals,
        "multi_ip_login",
        "customer_id",
        "AUTHENTICATION",
    ),

    signal_output(
        payment_signals,
        "payment_failure_velocity",
        "customer_id",
        "PAYMENT",
    ),

    signal_output(
        payment_signals,
        "multiple_payment_methods",
        "customer_id",
        "PAYMENT",
    ),

    signal_output(
        order_signals,
        "high_value",
        "customer_id",
        "TRANSACTION",
    ),

    signal_output(
        order_signals,
        "high_value_velocity",
        "customer_id",
        "TRANSACTION",
    ),

    signal_output(
        order_signals,
        "checkout_velocity",
        "customer_id",
        "VELOCITY",
    ),

    signal_output(
        device_signals,
        "multi_account",
        "device_id",
        "DEVICE",
    ),

    signal_output(
        device_signals,
        "device_payment_abuse",
        "device_id",
        "DEVICE",
    ),

    signal_output(
        coupon_signals,
        "coupon_abuse",
        "coupon_id",
        "COUPON",
    ),

    signal_output(
        refund_signals,
        "refund_abuse",
        "customer_id",
        "REFUND",
    ),

    signal_output(
        ato_signals,
        "account_takeover",
        "customer_id",
        "ACCOUNT_TAKEOVER",
    ),
]


# ============================================================
# UNION ALL SIGNALS
# ============================================================

fraud_signals = all_signals[0]

for signal_df in all_signals[1:]:

    fraud_signals = fraud_signals.unionByName(
        signal_df
    )


# ============================================================
# OUTPUT
# ============================================================

query = (
    fraud_signals
    .writeStream
    .format("console")
    .outputMode("update")
    .option(
        "truncate",
        "false",
    )
    .option(
        "numRows",
        100,
    )
    .option(
        "checkpointLocation",
        CHECKPOINT,
    )
    .start()
)


# ============================================================
# KEEP STREAM ALIVE
# ============================================================

try:

    query.awaitTermination()

except KeyboardInterrupt:

    print(
        "\nStopping FraudGuard..."
    )

    query.stop()
    spark.stop()