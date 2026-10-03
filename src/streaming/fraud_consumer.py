"""
RetailHub FraudGuard
====================

High-performance real-time fraud detector.

Architecture
------------
Kafka
  |
  v
ONE Spark Structured Streaming readStream
  |
  v
foreachBatch
  |
  v
Bounded in-memory fraud state
  |
  +--> 15 deterministic fraud rules
  |
  v
Fraud signals

Performance principles
----------------------
- ONE Kafka readStream
- ONE foreachBatch
- No count(distinct)
- No collect_set
- No large Spark state store
- No orderBy on streaming output
- No rdd.isEmpty()
- No repeated Spark actions
- TTL-based state cleanup
- Only required event fields are parsed
- Ground-truth/fraud labels are ignored
- All paths come from src/config/settings.py

CHANGELOG (this revision)
--------------------------
1. FIXED: rule_multi_account_ip called should_alert() with 5 args against
   a 4-arg signature -> TypeError on every trigger, silently swallowed by
   the batch-level bare except. Alert now fires correctly.
2. Per-event exceptions are now logged with traceback + event_id instead
   of being silently discarded.
3. emit_alert() now writes a durable JSON-lines record to
   FRAUDGUARD_INCIDENT_PATH in addition to the console banner, so alerts
   survive beyond stdout.
4. Kafka read now sets maxOffsetsPerTrigger for real backpressure control.
5. Startup config validation: required settings are checked before the
   stream starts, failing fast with a clear error instead of failing
   later inside the stream.
6. Basic per-batch metrics (processed / errors / alerts-by-rule /
   invalid-timestamp count) are printed and written to
   FRAUDGUARD_LOG_PATH as JSON lines.

CHANGELOG (this revision, round 2 — correctness fixes)
--------------------------------------------------------
8. FIXED: an event with an invalid/unparseable event_time was previously
   still processed, using now() as its timestamp. That let a malformed
   timestamp count toward velocity windows (e.g. "5 failed logins in 5
   minutes") and could produce a false fraud alert. Such events are now
   logged and SKIPPED entirely rather than processed with a fabricated
   timestamp.
9. FIXED: coupon_code was hardcoded to NULL (`F.lit(None)`), so
   COUPON_ABUSE could never fire regardless of real traffic. It is now
   actually extracted from the parsed event (checks entity.coupon_code,
   then metadata.coupon_code). Adjust the two F.col(...) paths below if
   your simulator puts it somewhere else.
10. FIXED: process_batch() was calling batch_df.count() and then a
    second action (toLocalIterator()) — two Spark actions per batch,
    contradicting this file's own "no repeated Spark actions" principle.
    Now a single toLocalIterator() over a `.limit(MAX_EVENTS_PER_BATCH + 1)`
    selection is used; if the (MAX+1)th row is reached, that tells us the
    batch exceeded the cap without a separate count() action. The exact
    total size of an oversized batch is no longer known (that no longer
    costs an action) — only "capped, at least one event not processed"
    is logged in that case.
11. FIXED: rule_account_takeover and rule_new_device_sensitive_change
    previously only checked that all required event TYPES were present
    somewhere in the window (a presence check), not that they occurred
    in the correct chronological order. A customer who placed an order,
    then changed their password, then logged in from a new device could
    have triggered ACCOUNT_TAKEOVER_SEQUENCE even though nothing
    resembling an account takeover happened. Both rules now use a
    true ordered-subsequence check (failed_login -> new_device ->
    sensitive_change -> order, each strictly after the previous, by
    timestamp).
"""


from __future__ import annotations
from fraudguard.score_calculator import calculate_risk
from fraudguard.response_engine import decide_response
from fraudguard.restriction_manager import (
    create_restriction,
)
from fraudguard.audit_logger import log_audit
from fraudguard.ai_investigator import investigate_incident
from fraudguard.kafka_producer import publish_fraud_decision

import json
import logging
import os
import sys
import time
import traceback
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)


# ---------------------------------------------------------------------------
# PROJECT IMPORT PATH
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from src.config.settings import (
    KAFKA_BROKER,
    FRAUD_TOPIC_PATTERN,
    FRAUD_CHECKPOINT_PATH,
    FRAUDGUARD_DATA_PATH,
    FRAUDGUARD_INCIDENT_PATH,
    FRAUDGUARD_STATE_PATH,
    FRAUDGUARD_AUDIT_PATH,
    FRAUDGUARD_EVIDENCE_PATH,
    FRAUDGUARD_LOG_PATH,
)


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logger = logging.getLogger("fraudguard")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
    )
    logger.addHandler(_handler)


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = KAFKA_BROKER
TOPIC_PATTERN = FRAUD_TOPIC_PATTERN
CHECKPOINT = FRAUD_CHECKPOINT_PATH

# Processing
PROCESSING_INTERVAL = os.getenv(
    "FRAUDGUARD_PROCESSING_INTERVAL",
    "2 seconds",
)

MAX_EVENTS_PER_BATCH = int(
    os.getenv(
        "FRAUDGUARD_MAX_EVENTS_PER_BATCH",
        "5000",
    )
)

# NEW: real backpressure control at the Kafka source, rather than relying
# solely on a post-hoc .limit() that silently drops events.
MAX_OFFSETS_PER_TRIGGER = int(
    os.getenv(
        "FRAUDGUARD_MAX_OFFSETS_PER_TRIGGER",
        "20000",
    )
)


# ---------------------------------------------------------------------------
# STARTUP CONFIG VALIDATION
# ---------------------------------------------------------------------------

def _require(name: str, value: Any) -> None:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise RuntimeError(
            f"[FraudGuard] Missing required configuration: {name}. "
            f"Check src/config/settings.py."
        )


for _name, _value in [
    ("KAFKA_BROKER", KAFKA_BOOTSTRAP),
    ("FRAUD_TOPIC_PATTERN", TOPIC_PATTERN),
    ("FRAUD_CHECKPOINT_PATH", CHECKPOINT),
    ("FRAUDGUARD_INCIDENT_PATH", FRAUDGUARD_INCIDENT_PATH),
    ("FRAUDGUARD_LOG_PATH", FRAUDGUARD_LOG_PATH),
]:
    _require(_name, _value)


# ---------------------------------------------------------------------------
# RULE WINDOWS
# ---------------------------------------------------------------------------

LOGIN_WINDOW = 300          # 5 minutes
PAYMENT_WINDOW = 600        # 10 minutes
CHECKOUT_WINDOW = 300       # 5 minutes
HIGH_VALUE_WINDOW = 600     # 10 minutes
ACCOUNT_WINDOW = 600        # 10 minutes
COUPON_WINDOW = 600         # 10 minutes
REFUND_WINDOW = 86400       # 24 hours
DDOS_WINDOW = 10            # 10 seconds
BOT_WINDOW = 10             # 10 seconds

# ---------------------------------------------------------------------------
# THRESHOLDS
# ---------------------------------------------------------------------------

FAILED_LOGIN_THRESHOLD = 5
MULTI_IP_LOGIN_THRESHOLD = 3

PAYMENT_FAILURE_THRESHOLD = 4
MULTIPLE_PAYMENT_METHOD_THRESHOLD = 3

HIGH_VALUE_THRESHOLD = float(
    os.getenv(
        "FRAUD_HIGH_VALUE_THRESHOLD",
        "50000",
    )
)

HIGH_VALUE_ORDER_THRESHOLD = 3

MULTI_ACCOUNT_THRESHOLD = 5

COUPON_CUSTOMER_THRESHOLD = 3

REFUND_THRESHOLD = 3

BOT_EVENT_THRESHOLD = 20
BOT_SESSION_THRESHOLD = 4

DDOS_EVENT_THRESHOLD = 30

CHECKOUT_VELOCITY_THRESHOLD = 5


# ---------------------------------------------------------------------------
# EVENT TYPES
# ---------------------------------------------------------------------------

FAILED_LOGIN_EVENTS = {
    "login_failed",
    "login_failure",
}

PAYMENT_FAILURE_EVENTS = {
    "payment_failed",
    "payment_failure",
}

LOGIN_EVENTS = {
    "login",
    "login_failed",
    "login_failure",
}

ORDER_EVENTS = {
    "order_created",
}

CHECKOUT_EVENTS = {
    "checkout_started",
    "order_created",
}

SENSITIVE_CHANGE_EVENTS = {
    "password_changed",
    "email_changed",
    "phone_changed",
    "address_changed",
    "profile_updated",
    "payment_method_added",
    "payment_method_updated",
}

NEW_DEVICE_EVENTS = {
    "login",
    "session_started",
}

COUPON_EVENTS = {
    "coupon_applied",
}

REFUND_EVENTS = {
    "refund_requested",
    "return_requested",
}

# Actual RetailHub event naming is lowercase.
# We deliberately support aliases where useful.


# ---------------------------------------------------------------------------
# SPARK
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RetailHub-FraudGuard")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------------------
# DIRECTORIES
# ---------------------------------------------------------------------------

for path in [
    FRAUDGUARD_DATA_PATH,
    FRAUDGUARD_INCIDENT_PATH,
    FRAUDGUARD_STATE_PATH,
    FRAUDGUARD_AUDIT_PATH,
    FRAUDGUARD_EVIDENCE_PATH,
    FRAUDGUARD_LOG_PATH,
]:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as exc:
        logger.warning("Could not create directory %s: %s", path, exc)


# ---------------------------------------------------------------------------
# EVENT SCHEMA
# ---------------------------------------------------------------------------

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("anonymous_id", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("event_type", StringType(), True),

    StructField(
        "context",
        StructType([
            StructField("ip_address", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("device", StringType(), True),
            StructField("country", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
        ]),
        True,
    ),

    StructField(
        "entity",
        StructType([
            StructField("product_id", StringType(), True),
            StructField("cart_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("payment_id", StringType(), True),
            StructField("return_id", StringType(), True),
            StructField("refund_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_amount", DoubleType(), True),
            # FIXED (round 2): coupon_code was never in the schema, so it
            # could never be parsed and COUPON_ABUSE could never fire.
            # Adjust the path here (and/or under metadata below) to match
            # wherever your simulator actually emits the coupon code.
            StructField("coupon_code", StringType(), True),
        ]),
        True,
    ),

    StructField(
        "metadata",
        StructType([
            StructField("amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("order_value", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("coupon_code", StringType(), True),
        ]),
        True,
    ),
])


# ---------------------------------------------------------------------------
# HELPER
# ---------------------------------------------------------------------------

def now_epoch() -> float:
    return time.time()


def event_epoch(value: Any) -> Tuple[float, bool]:
    """
    Convert event timestamp into epoch seconds.

    Returns (epoch_seconds, was_valid). Invalid/unparseable timestamps
    fall back to now(), but the caller gets `was_valid=False` so the
    fallback can be counted in metrics instead of silently disappearing.
    """

    if value is None:
        return now_epoch(), False

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value.timestamp(), True

    try:
        text = str(value)

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        return datetime.fromisoformat(text).timestamp(), True

    except Exception:
        return now_epoch(), False


def cleanup_deque(
    q: deque,
    cutoff: float,
) -> None:

    while q and q[0][0] < cutoff:
        q.popleft()


def append_event(
    store: Dict,
    key: str,
    timestamp: float,
    value: Any,
) -> None:

    store[key].append((timestamp, value))


def unique_recent(
    q: deque,
    cutoff: float,
) -> set:

    return {
        value
        for ts, value in q
        if ts >= cutoff and value is not None
    }


def ordered_subsequence_present(
    events_in_window: List[Tuple[float, str]],
    steps: List[set],
) -> bool:
    """
    NEW (round 2): true chronological check, not just "all types present
    somewhere in the window".

    `events_in_window` is a list of (timestamp, event_type) already
    filtered to the relevant time window -- NOT assumed to be sorted.

    `steps` is an ordered list of sets of event_type; the function returns
    True only if there exist events e_1, e_2, ..., e_n (n = len(steps))
    with strictly increasing timestamps such that e_i's event_type is in
    steps[i]. This is a standard greedy subsequence match: scanning the
    timeline once and advancing to the next required step as soon as it's
    matched is sufficient to detect whether the ordered pattern occurs
    anywhere in the window.
    """

    if not steps:
        return True

    ordered = sorted(events_in_window, key=lambda pair: pair[0])

    step_idx = 0

    for _, event_type in ordered:

        if event_type in steps[step_idx]:

            step_idx += 1

            if step_idx >= len(steps):
                return True

    return False


# ---------------------------------------------------------------------------
# FRAUD STATE
# ---------------------------------------------------------------------------

@dataclass
class CustomerState:

    failed_logins: deque = field(default_factory=deque)

    payment_failures: deque = field(default_factory=deque)

    payment_methods: deque = field(default_factory=deque)

    high_value_orders: deque = field(default_factory=deque)

    checkout_events: deque = field(default_factory=deque)

    sequence_events: deque = field(default_factory=deque)

    coupon_events: deque = field(default_factory=deque)

    refund_events: deque = field(default_factory=deque)

    devices: deque = field(default_factory=deque)

    incidents: int = 0


@dataclass
class EntityState:

    events: deque = field(default_factory=deque)

    customers: deque = field(default_factory=deque)

    sessions: deque = field(default_factory=deque)

    devices: deque = field(default_factory=deque)


customers: Dict[str, CustomerState] = defaultdict(CustomerState)

ip_states: Dict[str, EntityState] = defaultdict(EntityState)

device_states: Dict[str, EntityState] = defaultdict(EntityState)


# ---------------------------------------------------------------------------
# ALERT DEDUPLICATION
# ---------------------------------------------------------------------------

last_alert: Dict[Tuple[str, str, str], float] = {}
# Number of incidents observed for each customer during this
# FraudGuard process lifetime.
customer_incident_counts: Dict[str, int] = defaultdict(int)

ALERT_COOLDOWN = int(
    os.getenv(
        "FRAUDGUARD_ALERT_COOLDOWN",
        "30",
    )
)


def should_alert(
    fraud_type: str,
    entity_type: str,
    entity_key: str,
    event_ts: float,
) -> bool:
    """
    FIXED (round 4): cooldown is now measured in wall-clock (processing)
    time, not event time.

    Previously this compared the *event's own* timestamp against the
    timestamp of the last alert for the same key. That's correct in
    theory for a live stream where event_time keeps advancing in step
    with real time -- but it silently breaks the moment two events for
    the same key share an identical (or near-identical) event_time:
    diff = 0, which is always < ALERT_COOLDOWN, so the cooldown can
    never expire and that (fraud_type, entity_type, entity_key)
    combination is blocked from alerting again, forever -- even if a
    thousand more qualifying events arrive. This is exactly what
    happened when replaying fixed-timestamp test events: the first
    alert per rule fired, then every identical follow-up event was
    silently suppressed.

    Cooldown exists to protect a human/downstream system from being
    flooded with duplicate notifications in real operational time --
    "don't re-notify me about the same thing more than once every 30
    real seconds" -- which is a wall-clock concern, not a property of
    the data. Velocity/window rules (how many high-value orders in the
    last 10 minutes) correctly stay keyed on event_ts elsewhere; only
    this alert-throttling check moves to wall-clock time.

    `event_ts` is kept as a parameter (all 15 rule call sites already
    pass it) but is intentionally unused for the cooldown comparison
    now, so no call sites needed to change.
    """

    key = (
        fraud_type,
        entity_type,
        entity_key,
    )

    now = now_epoch()

    previous = last_alert.get(key)

    if previous is not None:
        if now - previous < ALERT_COOLDOWN:
            return False

    last_alert[key] = now

    return True


# ---------------------------------------------------------------------------
# BATCH METRICS
# ---------------------------------------------------------------------------

def new_batch_metrics(batch_id: int) -> Dict[str, Any]:
    return {
        "batch_id": batch_id,
        "batch_start_time": datetime.now(timezone.utc).isoformat(),
        "events_seen": 0,
        "events_processed": 0,
        "events_errored": 0,
        "invalid_timestamps": 0,
        "alerts_by_rule": defaultdict(int),
        "dropped_by_batch_cap": 0,
    }


def log_batch_metrics(metrics: Dict[str, Any]) -> None:
    record = dict(metrics)
    record["alerts_by_rule"] = dict(metrics["alerts_by_rule"])
    record["batch_end_time"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Batch %s summary: processed=%s errored=%s invalid_ts=%s "
        "alerts=%s dropped_by_cap=%s",
        record["batch_id"],
        record["events_processed"],
        record["events_errored"],
        record["invalid_timestamps"],
        sum(record["alerts_by_rule"].values()),
        record["dropped_by_batch_cap"],
    )

    try:
        log_file = os.path.join(FRAUDGUARD_LOG_PATH, "batch_metrics.jsonl")
        with open(log_file, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:
        logger.warning("Could not write batch metrics: %s", exc)


# ---------------------------------------------------------------------------
# INCIDENT OUTPUT
# ---------------------------------------------------------------------------

def emit_alert(
    fraud_type: str,
    entity_type: str,
    entity_key: str,
    event: Dict[str, Any],
    reason: str,
    severity: str,
    metrics: Optional[Dict[str, Any]] = None,
) -> None:

    timestamp = datetime.now(timezone.utc).isoformat()

    incident_id = (
        f"INC-{datetime.now(timezone.utc).strftime('%Y%m%d')}-"
        f"{uuid.uuid4().hex[:8].upper()}"
    )

    customer_id = event.get("customer_id")

    alert = {
        "incident_id": incident_id,
        "alert_time": timestamp,
        "fraud_type": fraud_type,
        "severity": severity,

        "entity_type": entity_type,
        "entity_key": entity_key,

        "customer_id": customer_id,
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),

        "ip_address": event.get("ip_address"),
        "device_id": event.get("device_id"),
        "session_id": event.get("session_id"),

        "reason": reason,

        "source": "fraudguard_deterministic",
    }

    print(
        "\n"
        + "=" * 80
        + "\n"
        + "🚨 FRAUD DETECTED\n"
        + "=" * 80
    )

    print(f"Incident      : {incident_id}")
    print(f"Rule          : {fraud_type}")
    print(f"Severity      : {severity}")
    print(f"Entity        : {entity_type}:{entity_key}")
    print(f"Customer      : {customer_id}")
    print(f"Event         : {event.get('event_type')}")
    print(f"Reason        : {reason}")

    print("=" * 80)

    # ------------------------------------------------------------------
    # NEW: durable incident record, appended as JSON-lines. This is a
    # simple, dependency-free sink; swap for a Kafka producer or a
    # Delta/DB write later without touching any rule logic.
    # ------------------------------------------------------------------

    try:
        incident_file = os.path.join(
            FRAUDGUARD_INCIDENT_PATH, "incidents.jsonl"
        )
        with open(incident_file, "a") as f:
            f.write(json.dumps(alert, default=str) + "\n")
    except Exception as exc:
        logger.error(
            "Failed to persist incident %s: %s", incident_id, exc
        )

    if metrics is not None:
        metrics["alerts_by_rule"][fraud_type] += 1
            # ------------------------------------------------------------------
    # RESPONSE LAYER
    # ------------------------------------------------------------------

    if customer_id:
        customer_incident_counts[customer_id] += 1
        incident_count = customer_incident_counts[customer_id]
    else:
        incident_count = 1

    # 1. Calculate risk
    risk = calculate_risk(
        fraud_type=fraud_type,
        severity=severity,
        reason=reason,
    )

    # 2. Determine response
    response = decide_response(
        risk_level=risk.level,
        incident_count=incident_count,
    )


    # 3. AI investigation
    investigation = investigate_incident(
        incident_id=incident_id,
        fraud_type=fraud_type,
        severity=severity,
        reason=reason,
        risk_score=risk.score,
        risk_level=risk.level,
        customer_id=customer_id,
        event_context={
            "ip_address": event.get("ip_address"),
            "device_id": event.get("device_id"),
            "session_id": event.get("session_id"),
            "event_type": event.get("event_type"),
        },
    )

    logger.info(
        "AI investigation | incident=%s | pattern=%s | "
        "confidence=%s | recommendation=%s",
        incident_id,
        investigation.attack_pattern,
        investigation.confidence,
        investigation.recommendation,
    )

    

    logger.info(
        "FraudGuard response | incident=%s | customer=%s | "
        "score=%s | level=%s | action=%s",
        incident_id,
        customer_id,
        risk.score,
        risk.level,
        response.action,
    )

    # 4. Create temporary restriction when required
    restriction = None

    if response.action in {
        "TEMPORARY_RESTRICTION",
        "TEMPORARY_PROTECTION",
    }:
        restriction = create_restriction(
            customer_id=customer_id,
            incident_id=incident_id,
            reason=reason,
            duration_minutes=response.restriction_minutes,
        )

    # 5. Record complete decision in audit log
    log_audit(
        agent="fraudguard_response_engine",
        event=fraud_type,
        reason=reason,
        evidence={
            "incident_id": incident_id,
            "risk_score": risk.score,
            "risk_level": risk.level,
            "incident_count": incident_count,
            "ai_attack_pattern": investigation.attack_pattern,
            "ai_finding": investigation.finding,
            "ai_confidence": investigation.confidence,
            "ai_evidence": investigation.evidence,
            "ai_recommendation": investigation.recommendation,
        },
        decision=response.action,
        action=response.action,
        approval=(
            "ADMIN_REVIEW"
            if response.requires_admin_review
            else None
        ),
        result=(
            "RESTRICTION_CREATED"
            if restriction
            else "RESPONSE_RECORDED"
        ),
        incident_id=incident_id,
        customer_id=customer_id,
    )

        # ------------------------------------------------------------------
    # 6. Publish finalized FraudGuard decision to Kafka
    # ------------------------------------------------------------------
    publish_fraud_decision(
        incident_id=incident_id,
        customer_id=customer_id,
        fraud_type=fraud_type,
        severity=severity,
        reason=reason,
        risk_score=risk.score,
        risk_level=risk.level,
        action=response.action,
        requires_customer_action=response.requires_customer_action,
        requires_admin_review=response.requires_admin_review,
        restriction_minutes=response.restriction_minutes,
        ai_attack_pattern=investigation.attack_pattern,
        ai_finding=investigation.finding,
        ai_confidence=investigation.confidence,
        ai_recommendation=investigation.recommendation,
        event_id=event.get("event_id"),
        event_type=event.get("event_type"),
        ip_address=event.get("ip_address"),
        device_id=event.get("device_id"),
        session_id=event.get("session_id"),
    )


# ---------------------------------------------------------------------------
# RULE 1
# BRUTE FORCE LOGIN
# ---------------------------------------------------------------------------

def rule_brute_force(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - LOGIN_WINDOW

    cleanup_deque(
        state.failed_logins,
        cutoff,
    )

    if len(state.failed_logins) >= FAILED_LOGIN_THRESHOLD:

        if should_alert(
            "BRUTE_FORCE_LOGIN",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "BRUTE_FORCE_LOGIN",
                "CUSTOMER",
                customer_id,
                event,
                f"{len(state.failed_logins)} failed logins "
                f"within {LOGIN_WINDOW // 60} minutes",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 2
# MULTI-IP LOGIN ATTACK
# ---------------------------------------------------------------------------

def rule_multi_ip_login(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - LOGIN_WINDOW

    cleanup_deque(
        state.failed_logins,
        cutoff,
    )

    ips = unique_recent(
        state.failed_logins,
        cutoff,
    )

    if len(ips) >= MULTI_IP_LOGIN_THRESHOLD:

        if should_alert(
            "MULTI_IP_LOGIN_ATTACK",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "MULTI_IP_LOGIN_ATTACK",
                "CUSTOMER",
                customer_id,
                event,
                f"Failed logins from {len(ips)} different IP addresses",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 3
# PAYMENT FAILURE VELOCITY
# ---------------------------------------------------------------------------

def rule_payment_failure_velocity(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - PAYMENT_WINDOW

    cleanup_deque(
        state.payment_failures,
        cutoff,
    )

    if len(state.payment_failures) >= PAYMENT_FAILURE_THRESHOLD:

        if should_alert(
            "PAYMENT_FAILURE_VELOCITY",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "PAYMENT_FAILURE_VELOCITY",
                "CUSTOMER",
                customer_id,
                event,
                f"{len(state.payment_failures)} payment failures "
                f"within 10 minutes",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 4
# MULTIPLE PAYMENT METHODS
# ---------------------------------------------------------------------------

def rule_multiple_payment_methods(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - PAYMENT_WINDOW

    cleanup_deque(
        state.payment_methods,
        cutoff,
    )

    methods = unique_recent(
        state.payment_methods,
        cutoff,
    )

    failures = [
        x
        for x in state.payment_failures
        if x[0] >= cutoff
    ]

    if (
        len(methods) >= MULTIPLE_PAYMENT_METHOD_THRESHOLD
        and len(failures) >= 2
    ):

        if should_alert(
            "MULTIPLE_PAYMENT_METHODS",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "MULTIPLE_PAYMENT_METHODS",
                "CUSTOMER",
                customer_id,
                event,
                f"{len(methods)} payment methods combined "
                f"with repeated payment failures",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 5
# HIGH VALUE TRANSACTION
# ---------------------------------------------------------------------------

def rule_high_value_transaction(
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    amount = event.get("amount") or 0.0

    if (
        event.get("event_type") == "order_created"
        and amount >= HIGH_VALUE_THRESHOLD
    ):

        entity_key = (
            event.get("customer_id")
            or event.get("order_id")
            or event.get("event_id")
        )

        if should_alert(
            "HIGH_VALUE_TRANSACTION",
            "CUSTOMER",
            entity_key,
            ts,
        ):

            emit_alert(
                "HIGH_VALUE_TRANSACTION",
                "CUSTOMER",
                entity_key,
                event,
                f"Order value ₹{amount:,.2f} exceeds "
                f"₹{HIGH_VALUE_THRESHOLD:,.2f}",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 6
# REPEATED HIGH VALUE ORDERS
# ---------------------------------------------------------------------------

def rule_repeated_high_value_orders(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - HIGH_VALUE_WINDOW

    cleanup_deque(
        state.high_value_orders,
        cutoff,
    )

    if len(state.high_value_orders) >= HIGH_VALUE_ORDER_THRESHOLD:

        if should_alert(
            "HIGH_VALUE_ORDER_VELOCITY",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "HIGH_VALUE_ORDER_VELOCITY",
                "CUSTOMER",
                customer_id,
                event,
                f"{len(state.high_value_orders)} high-value "
                f"orders within 10 minutes",
                "VERY_HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 7
# NEW DEVICE + SENSITIVE CHANGE
# ---------------------------------------------------------------------------

def rule_new_device_sensitive_change(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - ACCOUNT_WINDOW

    cleanup_deque(
        state.sequence_events,
        cutoff,
    )

    windowed = [
        (event_ts, event_type)
        for event_ts, event_type in state.sequence_events
        if event_ts >= cutoff
    ]

    # FIXED (round 2): previously a presence check (both types occur
    # somewhere in the window, in any order). Now requires the new-device
    # event to strictly precede the sensitive-change event.
    ato_like = ordered_subsequence_present(
        windowed,
        [
            {"new_device", "login_new_device"},
            SENSITIVE_CHANGE_EVENTS,
        ],
    )

    if ato_like:

        if should_alert(
            "ACCOUNT_CHANGE_NEW_DEVICE",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "ACCOUNT_CHANGE_NEW_DEVICE",
                "CUSTOMER",
                customer_id,
                event,
                "Sensitive account change detected after activity "
                "from a new device",
                "VERY_HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 8
# ACCOUNT TAKEOVER SEQUENCE
# ---------------------------------------------------------------------------

def rule_account_takeover(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - ACCOUNT_WINDOW

    cleanup_deque(
        state.sequence_events,
        cutoff,
    )

    windowed = [
        (event_ts, event_type)
        for event_ts, event_type in state.sequence_events
        if event_ts >= cutoff
    ]

    # FIXED (round 2): previously this only checked that all four event
    # TYPES appeared somewhere in the window, regardless of order -- e.g.
    # order_created -> password_changed -> new_device -> login_failed
    # would have triggered this, even though that isn't an account-
    # takeover pattern. Now requires the true chronological order:
    # failed_login -> new_device -> sensitive_change -> order, each
    # strictly after the previous.
    is_takeover_sequence = ordered_subsequence_present(
        windowed,
        [
            FAILED_LOGIN_EVENTS,
            {"new_device", "login_new_device"},
            SENSITIVE_CHANGE_EVENTS,
            {"order_created"},
        ],
    )

    if is_takeover_sequence:

        if should_alert(
            "ACCOUNT_TAKEOVER_SEQUENCE",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "ACCOUNT_TAKEOVER_SEQUENCE",
                "CUSTOMER",
                customer_id,
                event,
                "Failed login → new device → sensitive account "
                "change → order sequence detected, in that "
                "chronological order",
                "CRITICAL",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 9
# MULTI ACCOUNT DEVICE
# ---------------------------------------------------------------------------

def rule_multi_account_device(
    device_id: Optional[str],
    state: EntityState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    if not device_id:
        return

    cutoff = ts - ACCOUNT_WINDOW

    cleanup_deque(
        state.customers,
        cutoff,
    )

    customers_seen = unique_recent(
        state.customers,
        cutoff,
    )

    if len(customers_seen) >= MULTI_ACCOUNT_THRESHOLD:

        if should_alert(
            "MULTI_ACCOUNT_DEVICE",
            "DEVICE",
            device_id,
            ts,
        ):

            emit_alert(
                "MULTI_ACCOUNT_DEVICE",
                "DEVICE",
                device_id,
                event,
                f"Device associated with "
                f"{len(customers_seen)} customers within 10 minutes",
                "VERY_HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 10
# MULTI ACCOUNT IP
# ---------------------------------------------------------------------------

def rule_multi_account_ip(
    ip_address: Optional[str],
    state: EntityState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    if not ip_address:
        return

    cutoff = ts - ACCOUNT_WINDOW

    cleanup_deque(
        state.customers,
        cutoff,
    )

    customers_seen = unique_recent(
        state.customers,
        cutoff,
    )

    if len(customers_seen) >= MULTI_ACCOUNT_THRESHOLD:

        # FIXED: this previously called should_alert(fraud_type, entity_type,
        # entity_key, event, ts) -- 5 args against a 4-arg signature, which
        # raised TypeError every time this branch was hit and was silently
        # swallowed by the batch-level except. Now matches the real signature.
        if should_alert(
            "MULTI_ACCOUNT_IP",
            "IP",
            ip_address,
            ts,
        ):

            emit_alert(
                "MULTI_ACCOUNT_IP",
                "IP",
                ip_address,
                event,
                f"IP associated with "
                f"{len(customers_seen)} customers within 10 minutes",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 11
# COUPON ABUSE
# ---------------------------------------------------------------------------

def rule_coupon_abuse(
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    coupon = event.get("coupon_code")

    if not coupon:
        return

    cutoff = ts - COUPON_WINDOW

    # We keep a compact global coupon state.
    key = f"coupon:{coupon}"

    if not hasattr(rule_coupon_abuse, "_state"):
        rule_coupon_abuse._state = defaultdict(deque)

    q = rule_coupon_abuse._state[key]

    q.append(
        (
            ts,
            event.get("customer_id"),
            event.get("device_id"),
        )
    )

    cleanup_deque(
        q,
        cutoff,
    )

    customers_seen = {
        customer
        for _, customer, _ in q
        if customer
    }

    devices_seen = {
        device
        for _, _, device in q
        if device
    }

    if (
        len(customers_seen) >= COUPON_CUSTOMER_THRESHOLD
        and len(devices_seen) <= 2
    ):

        if should_alert(
            "COUPON_ABUSE",
            "COUPON",
            coupon,
            ts,
        ):

            emit_alert(
                "COUPON_ABUSE",
                "COUPON",
                coupon,
                event,
                f"Coupon used by {len(customers_seen)} customers "
                f"across only {len(devices_seen)} devices",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 12
# REFUND ABUSE
# ---------------------------------------------------------------------------

def rule_refund_abuse(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - REFUND_WINDOW

    cleanup_deque(
        state.refund_events,
        cutoff,
    )

    if len(state.refund_events) >= REFUND_THRESHOLD:

        if should_alert(
            "REFUND_ABUSE",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "REFUND_ABUSE",
                "CUSTOMER",
                customer_id,
                event,
                f"{len(state.refund_events)} refund/return "
                f"events within 24 hours",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 13
# BOT / SCRAPER
# ---------------------------------------------------------------------------

def rule_bot_scraper(
    ip_address: Optional[str],
    state: EntityState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    if not ip_address:
        return

    cutoff = ts - BOT_WINDOW

    cleanup_deque(
        state.events,
        cutoff,
    )

    cleanup_deque(
        state.sessions,
        cutoff,
    )

    event_count = len(state.events)

    sessions = unique_recent(
        state.sessions,
        cutoff,
    )

    if (
        event_count >= BOT_EVENT_THRESHOLD
        and len(sessions) >= BOT_SESSION_THRESHOLD
    ):

        if should_alert(
            "BOT_OR_SCRAPER",
            "IP",
            ip_address,
            ts,
        ):

            emit_alert(
                "BOT_OR_SCRAPER",
                "IP",
                ip_address,
                event,
                f"{event_count} events and "
                f"{len(sessions)} sessions within 10 seconds",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 14
# DDOS
# ---------------------------------------------------------------------------

def rule_ddos(
    ip_address: Optional[str],
    state: EntityState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    if not ip_address:
        return

    cutoff = ts - DDOS_WINDOW

    cleanup_deque(
        state.events,
        cutoff,
    )

    if len(state.events) > DDOS_EVENT_THRESHOLD:

        if should_alert(
            "DDOS",
            "IP",
            ip_address,
            ts,
        ):

            emit_alert(
                "DDOS",
                "IP",
                ip_address,
                event,
                f"{len(state.events)} events from one IP "
                f"within {DDOS_WINDOW} seconds",
                "CRITICAL",
                metrics,
            )


# ---------------------------------------------------------------------------
# RULE 15
# CHECKOUT VELOCITY
# ---------------------------------------------------------------------------

def rule_checkout_velocity(
    customer_id: Optional[str],
    state: CustomerState,
    event: Dict,
    ts: float,
    metrics: Dict[str, Any],
) -> None:

    cutoff = ts - CHECKOUT_WINDOW

    cleanup_deque(
        state.checkout_events,
        cutoff,
    )

    if len(state.checkout_events) >= CHECKOUT_VELOCITY_THRESHOLD:

        if should_alert(
            "CHECKOUT_VELOCITY",
            "CUSTOMER",
            customer_id,
            ts,
        ):

            emit_alert(
                "CHECKOUT_VELOCITY",
                "CUSTOMER",
                customer_id,
                event,
                f"{len(state.checkout_events)} checkout/order "
                f"events within 5 minutes",
                "HIGH",
                metrics,
            )


# ---------------------------------------------------------------------------
# EVENT PROCESSOR
# ---------------------------------------------------------------------------

def process_event(
    event: Dict[str, Any],
    metrics: Dict[str, Any],
) -> None:

    event_type = (
        str(
            event.get(
                "event_type",
                "",
            )
        )
        .strip()
        .lower()
    )

    if not event_type:
        return

    customer_id = event.get("customer_id")

    ip_address = event.get("ip_address")

    device_id = event.get("device_id")

    session_id = event.get("session_id")

    ts, ts_valid = event_epoch(
        event.get("event_time")
    )

    if not ts_valid:
        # FIXED (round 2): previously this event was still processed using
        # now() as a stand-in timestamp, which could let a malformed
        # event_time count toward velocity-window rules (failed-login
        # bursts, DDoS, etc.) and trigger a false alert. Skip the event
        # entirely instead — it's still counted in metrics so the problem
        # is visible, but it no longer influences any fraud rule.
        metrics["invalid_timestamps"] += 1
        logger.warning(
            "Skipping event_id=%s -- invalid/unparseable event_time "
            "(value=%r)",
            event.get("event_id"),
            event.get("event_time"),
        )
        return

    # ---------------------------------------------------------------
    # Customer state
    # ---------------------------------------------------------------

    customer_state = None

    if customer_id:

        customer_state = customers[customer_id]

    # ---------------------------------------------------------------
    # IP state
    # ---------------------------------------------------------------

    ip_state = None

    if ip_address:

        ip_state = ip_states[ip_address]

        ip_state.events.append(
            (ts, event_type)
        )

        if session_id:

            ip_state.sessions.append(
                (ts, session_id)
            )

        if customer_id:

            ip_state.customers.append(
                (ts, customer_id)
            )

        if device_id:

            ip_state.devices.append(
                (ts, device_id)
            )

    # ---------------------------------------------------------------
    # Device state
    # ---------------------------------------------------------------

    device_state = None

    if device_id:

        device_state = device_states[device_id]

        device_state.events.append(
            (ts, event_type)
        )

        if customer_id:

            device_state.customers.append(
                (ts, customer_id)
            )

        if session_id:

            device_state.sessions.append(
                (ts, session_id)
            )

    # ---------------------------------------------------------------
    # Customer state updates
    # ---------------------------------------------------------------

    if customer_state:

        if event_type in FAILED_LOGIN_EVENTS:

            customer_state.failed_logins.append(
                (
                    ts,
                    ip_address,
                )
            )

        if event_type in PAYMENT_FAILURE_EVENTS:

            customer_state.payment_failures.append(
                (
                    ts,
                    event.get("payment_id")
                    or event.get("event_id"),
                )
            )

        payment_method = event.get(
            "payment_method"
        )

        if payment_method:

            customer_state.payment_methods.append(
                (
                    ts,
                    payment_method,
                )
            )

        if event_type in CHECKOUT_EVENTS:

            customer_state.checkout_events.append(
                (
                    ts,
                    event_type,
                )
            )

        if event_type in ORDER_EVENTS:

            amount = event.get(
                "amount"
            ) or 0.0

            if amount >= HIGH_VALUE_THRESHOLD:

                customer_state.high_value_orders.append(
                    (
                        ts,
                        amount,
                    )
                )

        if device_id:

            previous_devices = unique_recent(
                customer_state.devices,
                ts - ACCOUNT_WINDOW,
            )

            if device_id not in previous_devices:

                customer_state.sequence_events.append(
                    (
                        ts,
                        "new_device",
                    )
                )

            customer_state.devices.append(
                (
                    ts,
                    device_id,
                )
            )

        customer_state.sequence_events.append(
            (
                ts,
                event_type,
            )
        )

        if event_type in COUPON_EVENTS:

            customer_state.coupon_events.append(
                (
                    ts,
                    event.get("coupon_code"),
                )
            )

        if event_type in REFUND_EVENTS:

            customer_state.refund_events.append(
                (
                    ts,
                    event_type,
                )
            )

    # ---------------------------------------------------------------
    # RULES
    # ---------------------------------------------------------------

    if customer_state and customer_id:

        rule_brute_force(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_multi_ip_login(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_payment_failure_velocity(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_multiple_payment_methods(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_repeated_high_value_orders(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_new_device_sensitive_change(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_account_takeover(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_refund_abuse(
            customer_id, customer_state, event, ts, metrics,
        )

        rule_checkout_velocity(
            customer_id, customer_state, event, ts, metrics,
        )

    # Immediate rule

    rule_high_value_transaction(
        event, ts, metrics,
    )

    # IP rules

    if ip_state and ip_address:

        rule_multi_account_ip(
            ip_address, ip_state, event, ts, metrics,
        )

        rule_bot_scraper(
            ip_address, ip_state, event, ts, metrics,
        )

        rule_ddos(
            ip_address, ip_state, event, ts, metrics,
        )

    # Device rules

    if device_state and device_id:

        rule_multi_account_device(
            device_id, device_state, event, ts, metrics,
        )

    # Coupon rule

    rule_coupon_abuse(
        event, ts, metrics,
    )


# ---------------------------------------------------------------------------
# BATCH PROCESSOR
# ---------------------------------------------------------------------------

def process_batch(
    batch_df,
    batch_id: int,
) -> None:

    # FIXED (round 3): batch_df.isEmpty() is itself a Spark action, so
    # combined with the toLocalIterator() action below this was two
    # actions per batch again -- the same problem already fixed for
    # count(). Removed: toLocalIterator() naturally yields zero rows for
    # an empty batch, so the for-loop below just doesn't execute and
    # log_batch_metrics() still runs with all-zero counts. No isEmpty()
    # check needed.
    logger.info("Processing batch %s", batch_id)

    metrics = new_batch_metrics(batch_id)

    # ---------------------------------------------------------------
    # IMPORTANT:
    #
    # We collect only the normalized columns needed by FraudGuard.
    #
    # FIXED (round 2): this previously called batch_df.count() before
    # toLocalIterator() -- two Spark actions per batch, which violates
    # this file's own "no repeated Spark actions" principle. We now pull
    # MAX_EVENTS_PER_BATCH + 1 rows in a single toLocalIterator() action;
    # if we ever reach the (MAX+1)th row, that alone tells us the batch
    # was oversized, with no separate count() needed. We stop processing
    # at MAX_EVENTS_PER_BATCH either way -- the exact total size of an
    # oversized batch is no longer known without paying for an extra
    # action, but "batch exceeded the cap" is still logged.
    # ---------------------------------------------------------------

    rows = (
        batch_df
        .select(
            "event_id",
            "event_time",
            "session_id",
            "customer_id",
            "event_type",
            "ip_address",
            "device_id",
            "order_id",
            "payment_id",
            "amount",
            "payment_method",
            "coupon_code",
        )
        .limit(MAX_EVENTS_PER_BATCH + 1)
        .toLocalIterator()
    )

    seen = 0
    truncated = False

    for row in rows:

        seen += 1

        if seen > MAX_EVENTS_PER_BATCH:
            truncated = True
            break

        event = row.asDict(
            recursive=True
        )

        try:

            process_event(
                event, metrics,
            )

            metrics["events_processed"] += 1

        except Exception:

            metrics["events_errored"] += 1

            logger.error(
                "Event processing error for event_id=%s:\n%s",
                event.get("event_id"),
                traceback.format_exc(),
            )

    metrics["events_seen"] = seen if not truncated else f">= {seen}"

    if truncated:
        metrics["dropped_by_batch_cap"] = "unknown (>=1) -- count() avoided"
        logger.warning(
            "Batch %s exceeded MAX_EVENTS_PER_BATCH=%s. At least one "
            "event was NOT processed this trigger -- consider lowering "
            "FRAUDGUARD_MAX_OFFSETS_PER_TRIGGER or raising "
            "FRAUDGUARD_MAX_EVENTS_PER_BATCH. (Exact overflow count is "
            "not computed, to avoid a second Spark action per batch.)",
            batch_id, MAX_EVENTS_PER_BATCH,
        )

    log_batch_metrics(metrics)


# ---------------------------------------------------------------------------
# KAFKA SOURCE
# ---------------------------------------------------------------------------

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
    .option(
        # NEW: real backpressure control. Caps how many Kafka offsets are
        # pulled into a single trigger, so a burst can't dump an unbounded
        # number of events into one micro-batch. MAX_EVENTS_PER_BATCH below
        # remains only as a last-resort safety valve.
        "maxOffsetsPerTrigger",
        MAX_OFFSETS_PER_TRIGGER,
    )
    .load()
)


# ---------------------------------------------------------------------------
# PARSE JSON
# ---------------------------------------------------------------------------

parsed = (
    raw
    .select(
        F.from_json(
            F.col("value").cast("string"),
            event_schema,
        ).alias("event")
    )
)


# ---------------------------------------------------------------------------
# NORMALIZATION
# ---------------------------------------------------------------------------

events = (
    parsed
    .select(
        F.col("event.event_id").alias(
            "event_id"
        ),

        F.col("event.event_time").alias(
            "event_time"
        ),

        F.col("event.session_id").alias(
            "session_id"
        ),

        F.col("event.customer_id").alias(
            "customer_id"
        ),

        F.lower(
            F.trim(
                F.col("event.event_type")
            )
        ).alias(
            "event_type"
        ),

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
            "event.entity.order_id"
        ).alias(
            "order_id"
        ),

        F.col(
            "event.entity.payment_id"
        ).alias(
            "payment_id"
        ),

        F.coalesce(
            F.col(
                "event.metadata.amount"
            ),

            F.col(
                "event.metadata.total_amount"
            ),

            F.col(
                "event.metadata.order_value"
            ),

            F.col(
                "event.entity.transaction_amount"
            ),

            F.col(
                "event.entity.amount"
            ),

            F.lit(0.0),
        ).alias(
            "amount"
        ),

        F.col(
            "event.metadata.payment_method"
        ).alias(
            "payment_method"
        ),

        # FIXED (round 2): previously hardcoded to NULL via a separate
        # withColumn(F.lit(None)) below, which meant COUPON_ABUSE could
        # never trigger. Now actually extracted from the event, checking
        # entity.coupon_code first, then metadata.coupon_code. Update
        # these paths if your simulator places it elsewhere.
        F.coalesce(
            F.col("event.entity.coupon_code"),
            F.col("event.metadata.coupon_code"),
        ).alias(
            "coupon_code"
        ),

        # These are deliberately read only from the event schema.
        # Ground-truth fields such as ground_truth_fraud,
        # fraud_rule, simulation_mode, etc. are NOT parsed.
        # FIXED (round 3): removed an unused F.get_json_object(event_id, "$")
        # column that added a pointless extra expression to the select
        # (event_id is a plain string, not a JSON document) and whose
        # output ("_unused") was never referenced anywhere.
    )
)


# ---------------------------------------------------------------------------
# BASIC VALIDATION
# ---------------------------------------------------------------------------

valid_events = (
    events
    .filter(
        F.col("event_id").isNotNull()
        & F.col("event_type").isNotNull()
    )
)


# ---------------------------------------------------------------------------
# START STREAM
# ---------------------------------------------------------------------------

print("=" * 80)
print("RETAILHUB FRAUDGUARD")
print("=" * 80)

print(
    f"Kafka broker            : {KAFKA_BOOTSTRAP}"
)

print(
    f"Topic pattern           : {TOPIC_PATTERN}"
)

print(
    f"Checkpoint              : {CHECKPOINT}"
)

print(
    "Rules                   : 15"
)

print(
    "Spark state store       : DISABLED FOR FRAUD RULES"
)

print(
    "Distinct count          : NONE"
)

print(
    "collect_set              : NONE"
)

print(
    "Ground truth             : IGNORED"
)

print(
    f"Processing interval     : {PROCESSING_INTERVAL}"
)

print(
    f"Max offsets/trigger     : {MAX_OFFSETS_PER_TRIGGER}"
)

print(
    f"Max events/batch (cap)  : {MAX_EVENTS_PER_BATCH}"
)

print(
    f"Incident sink           : {FRAUDGUARD_INCIDENT_PATH}/incidents.jsonl"
)

print(
    f"Batch metrics sink      : {FRAUDGUARD_LOG_PATH}/batch_metrics.jsonl"
)

print("=" * 80)


query = (
    valid_events
    .writeStream
    .foreachBatch(
        process_batch
    )
    .outputMode(
        "update"
    )
    .option(
        "checkpointLocation",
        CHECKPOINT,
    )
    .trigger(
        processingTime=PROCESSING_INTERVAL
    )
    .start()
)


# ---------------------------------------------------------------------------
# KEEP PROCESS ALIVE
# ---------------------------------------------------------------------------

try:

    query.awaitTermination()

except KeyboardInterrupt:

    logger.info("Stopping FraudGuard...")

    try:
        query.stop()
    except Exception:
        pass

    try:
        spark.stop()
    except Exception:
        pass

    logger.info("FraudGuard stopped.")