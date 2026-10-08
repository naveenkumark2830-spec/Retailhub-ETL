"""
RetailHub FraudGuard
Kafka producer for finalized fraud decisions.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from kafka import KafkaProducer

# Import centralized RetailHub configuration
from src.config.settings import (
    KAFKA_BROKER,
    FRAUDGUARD_DECISION_TOPIC,
)

logger = logging.getLogger("fraudguard.kafka_producer")

_producer: Optional[KafkaProducer] = None


def get_producer() -> KafkaProducer:
    """
    Create the Kafka producer once and reuse it.
    """

    global _producer

    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda value: json.dumps(
                value,
                default=str,
            ).encode("utf-8"),
            key_serializer=lambda key: (
                key.encode("utf-8")
                if key is not None
                else None
            ),
            acks="all",
            retries=3,
        )

    return _producer


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

def publish_fraud_decision(
    *,
    incident_id: str,
    customer_id: Optional[str],
    fraud_type: str,
    severity: str,
    reason: str,
    risk_score: int,
    risk_level: str,
    action: str,
    requires_customer_action: bool,
    requires_admin_review: bool,
    restriction_minutes: Optional[int],
    ai_attack_pattern: Optional[str] = None,
    ai_finding: Optional[str] = None,
    ai_confidence: Optional[float] = None,
    ai_recommendation: Optional[str] = None,
    event_id: Optional[str] = None,
    event_type: Optional[str] = None,
    ip_address: Optional[str] = None,
    device_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Publish the finalized FraudGuard decision to Kafka.

    Kafka publishing failure must NOT crash FraudGuard.
    """

    message = {
        "event_type": "FRAUD_DECISION",
        "timestamp": datetime.now(timezone.utc).isoformat(),

        "incident_id": incident_id,
        "customer_id": customer_id,

        "fraud_type": fraud_type,
        "severity": severity,
        "reason": reason,

        "risk_score": risk_score,
        "risk_level": risk_level,

        "action": action,
        "requires_customer_action": requires_customer_action,
        "requires_admin_review": requires_admin_review,
        "restriction_minutes": restriction_minutes,

        "ai_attack_pattern": ai_attack_pattern,
        "ai_finding": ai_finding,
        "ai_confidence": ai_confidence,
        "ai_recommendation": ai_recommendation,

        "source_event_id": event_id,
        "source_event_type": event_type,

        "ip_address": ip_address,
        "device_id": device_id,
        "session_id": session_id,

        "source": "fraudguard",
    }

    try:
        producer = get_producer()

        future = producer.send(
            FRAUDGUARD_DECISION_TOPIC,
            key=customer_id or incident_id,
            value=message,
        )

        metadata = future.get(timeout=10)

        logger.info(
            "Fraud decision published | incident=%s | "
            "customer=%s | action=%s | topic=%s | partition=%s | offset=%s",
            incident_id,
            customer_id,
            action,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    except Exception as exc:
        logger.warning(
            "Kafka publish failed | incident=%s | "
            "fallback=local_audit | reason=%s",
            incident_id,
            str(exc).splitlines()[0],
        )

    return message