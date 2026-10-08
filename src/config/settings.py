import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Kafka
# -----------------------------

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:9092"
)

KAFKA_TOPIC_PATTERN = os.getenv(
    "KAFKA_TOPIC_PATTERN",
    "retail_.*"
)

FRAUD_TOPIC_PATTERN = os.getenv(
    "FRAUD_TOPIC_PATTERN",
    "retail_.*_events"
)

# -----------------------------
# Storage Paths
# -----------------------------

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "./data/bronze"
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "./data/silver"
)

QUARANTINE_PATH = os.getenv(
    "QUARANTINE_PATH",
    "./data/quarantine/events"
)

GOLD_BASE_PATH = os.getenv(
    "GOLD_PATH",
    "./data/gold"
)

# -----------------------------
# Streaming Checkpoints
# -----------------------------

BRONZE_CHECKPOINT_PATH = os.getenv(
    "BRONZE_CHECKPOINT_PATH",
    "./checkpoints/kafka_bronze"
)

FRAUD_CHECKPOINT_PATH = os.getenv(
    "FRAUD_CHECKPOINT_PATH",
    "./checkpoints/fraud_detection_v4"
)


# -----------------------------
# FraudGuard
# -----------------------------


#FRAUDGUARD_CONSUMER_GROUP = os.getenv(
#    "FRAUDGUARD_CONSUMER_GROUP",
#    "fraudguard-v1"
#)


# -----------------------------
# FraudGuard Storage Paths
# -----------------------------

FRAUDGUARD_DATA_PATH = os.getenv(
    "FRAUDGUARD_DATA_PATH",
    "./data/fraudguard"
)

FRAUDGUARD_SIGNAL_PATH = os.getenv(
    "FRAUDGUARD_SIGNAL_PATH",
    "./data/fraudguard/signals"
)

FRAUDGUARD_INCIDENT_PATH = os.getenv(
    "FRAUDGUARD_INCIDENT_PATH",
    "./data/fraudguard/incidents"
)

FRAUDGUARD_STATE_PATH = os.getenv(
    "FRAUDGUARD_STATE_PATH",
    "./data/fraudguard/state"
)

FRAUDGUARD_AUDIT_PATH = os.getenv(
    "FRAUDGUARD_AUDIT_PATH",
    "./data/fraudguard/audit"
)

FRAUDGUARD_EVIDENCE_PATH = os.getenv(
    "FRAUDGUARD_EVIDENCE_PATH",
    "./data/fraudguard/evidence"
)

FRAUDGUARD_LOG_PATH = os.getenv(
    "FRAUDGUARD_LOG_PATH",
    "./logs/fraudguard"
)

FRAUDGUARD_DECISION_TOPIC = os.getenv(
    "FRAUDGUARD_DECISION_TOPIC",
    "retail_fraudguard_events",
)

# -----------------------------
# FraudGuard Website Integration
# -----------------------------

WEBSITE_API_BASE_URL = os.getenv(
    "WEBSITE_API_BASE_URL",
    "http://localhost:8000"
)

FRAUDGUARD_API_HOST = os.getenv(
    "FRAUDGUARD_API_HOST",
    "0.0.0.0"
)

FRAUDGUARD_API_PORT = int(
    os.getenv(
        "FRAUDGUARD_API_PORT",
        "8001"
    )
)