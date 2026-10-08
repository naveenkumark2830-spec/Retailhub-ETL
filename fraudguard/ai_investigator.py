"""
RetailHub FraudGuard
AI Investigation Layer.

Deterministic rules detect suspicious activity.
This module investigates incidents using Gemini when enabled.

AI does NOT directly enforce restrictions or bans.

Architecture:

Fraud Rule
    ↓
Risk Assessment
    ↓
AI Investigation
    ↓
InvestigationResult
    ↓
Response Engine
    ↓
Enforcement

If Gemini is unavailable, the deterministic investigation
fallback is used automatically.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List

from dotenv import load_dotenv
from google import genai
from google.genai import types


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv()


AI_ENABLED = os.getenv(
    "FRAUDGUARD_AI_ENABLED",
    "false",
).lower() == "true"

AI_PROVIDER = os.getenv(
    "FRAUDGUARD_AI_PROVIDER",
    "gemini",
).lower()

AI_MODEL = os.getenv(
    "FRAUDGUARD_AI_MODEL",
    "gemini-3-flash-preview",
)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


# ---------------------------------------------------------------------------
# Result contract
# ---------------------------------------------------------------------------

@dataclass
class InvestigationResult:
    incident_id: str
    finding: str
    attack_pattern: str
    evidence: List[str] = field(default_factory=list)
    confidence: float = 0.0
    recommendation: str = "MONITOR"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "finding": self.finding,
            "attack_pattern": self.attack_pattern,
            "evidence": self.evidence,
            "confidence": self.confidence,
            "recommendation": self.recommendation,
        }


# ---------------------------------------------------------------------------
# Deterministic fallback mapping
# ---------------------------------------------------------------------------

PATTERN_MAP = {
    "BRUTE_FORCE_LOGIN": (
        "Credential attack",
        "Repeated authentication failures indicate possible "
        "credential guessing or automated login attempts.",
    ),
    "MULTI_IP_LOGIN_ATTACK": (
        "Distributed credential attack",
        "Authentication failures originated from multiple IP "
        "addresses against the same customer.",
    ),
    "PAYMENT_FAILURE_VELOCITY": (
        "Payment attack",
        "Repeated payment failures indicate possible payment "
        "testing or suspicious transaction activity.",
    ),
    "MULTIPLE_PAYMENT_METHODS": (
        "Payment method abuse",
        "Multiple payment methods combined with payment failures "
        "indicate suspicious payment activity.",
    ),
    "HIGH_VALUE_TRANSACTION": (
        "High-value transaction anomaly",
        "A transaction exceeded the configured high-value threshold.",
    ),
    "HIGH_VALUE_ORDER_VELOCITY": (
        "High-value transaction velocity",
        "Multiple high-value orders occurred within a short window.",
    ),
    "ACCOUNT_CHANGE_NEW_DEVICE": (
        "Possible account takeover",
        "Sensitive account changes followed activity from a new device.",
    ),
    "ACCOUNT_TAKEOVER_SEQUENCE": (
        "Account takeover sequence",
        "The detected sequence matches failed login → new device → "
        "sensitive change → order.",
    ),
    "MULTI_ACCOUNT_DEVICE": (
        "Shared-device anomaly",
        "One device was associated with multiple customer accounts.",
    ),
    "MULTI_ACCOUNT_IP": (
        "Shared-IP anomaly",
        "One IP address was associated with multiple customer accounts.",
    ),
    "COUPON_ABUSE": (
        "Coupon abuse",
        "A coupon was used across multiple customers with limited "
        "device diversity.",
    ),
    "REFUND_ABUSE": (
        "Refund abuse",
        "Multiple refund or return events occurred within the "
        "configured monitoring window.",
    ),
    "BOT_OR_SCRAPER": (
        "Automated traffic",
        "High event volume combined with multiple sessions suggests "
        "possible bot or scraper activity.",
    ),
    "DDOS": (
        "Traffic flood",
        "A single IP generated an unusually high event volume "
        "within a short time window.",
    ),
    "CHECKOUT_VELOCITY": (
        "Checkout velocity anomaly",
        "Multiple checkout or order events occurred within a "
        "short period.",
    ),
}


# ---------------------------------------------------------------------------
# Deterministic recommendation
# ---------------------------------------------------------------------------

def _deterministic_recommendation(
    risk_level: str,
) -> str:

    if risk_level == "CRITICAL":
        return "TEMPORARY_PROTECTION"

    if risk_level == "VERY_HIGH":
        return "TEMPORARY_RESTRICTION"

    if risk_level == "HIGH":
        return "STEP_UP_AUTHENTICATION"

    return "MONITOR"


# ---------------------------------------------------------------------------
# Evidence builder
# ---------------------------------------------------------------------------

def _build_evidence(
    fraud_type: str,
    severity: str,
    risk_score: int,
    reason: str,
    customer_id: str | None,
    context: Dict[str, Any],
) -> List[str]:

    evidence = [
        f"Fraud rule triggered: {fraud_type}",
        f"Severity classified as {severity}",
        f"Risk score is {risk_score}/100",
        f"Detection reason: {reason}",
    ]

    if customer_id:
        evidence.append(
            f"Customer associated with incident: {customer_id}"
        )

    if context.get("ip_address"):
        evidence.append(
            f"Source IP: {context['ip_address']}"
        )

    if context.get("device_id"):
        evidence.append(
            f"Device: {context['device_id']}"
        )

    if context.get("session_id"):
        evidence.append(
            f"Session: {context['session_id']}"
        )

    if context.get("event_type"):
        evidence.append(
            f"Event type: {context['event_type']}"
        )

    return evidence


# ---------------------------------------------------------------------------
# Deterministic fallback investigation
# ---------------------------------------------------------------------------

def _deterministic_investigation(
    incident_id: str,
    fraud_type: str,
    severity: str,
    reason: str,
    risk_score: int,
    risk_level: str,
    customer_id: str | None,
    context: Dict[str, Any],
) -> InvestigationResult:

    attack_pattern, finding = PATTERN_MAP.get(
        fraud_type,
        (
            "Unknown suspicious activity",
            "A deterministic FraudGuard rule identified suspicious activity.",
        ),
    )

    evidence = _build_evidence(
        fraud_type=fraud_type,
        severity=severity,
        risk_score=risk_score,
        reason=reason,
        customer_id=customer_id,
        context=context,
    )

    confidence = min(
        0.99,
        max(
            0.50,
            0.50 + (risk_score / 200.0),
        ),
    )

    return InvestigationResult(
        incident_id=incident_id,
        finding=finding,
        attack_pattern=attack_pattern,
        evidence=evidence,
        confidence=round(confidence, 2),
        recommendation=_deterministic_recommendation(
            risk_level
        ),
    )


# ---------------------------------------------------------------------------
# Gemini client
# ---------------------------------------------------------------------------

_gemini_client = None


def _get_gemini_client():
    global _gemini_client

    if _gemini_client is not None:
        return _gemini_client

    if not GEMINI_API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY is not configured."
        )

    _gemini_client = genai.Client(
    api_key=GEMINI_API_KEY,
    http_options=types.HttpOptions(
        timeout=10000
    ),
)

    return _gemini_client


# ---------------------------------------------------------------------------
# Gemini investigation
# ---------------------------------------------------------------------------

def _gemini_investigation(
    incident_id: str,
    fraud_type: str,
    severity: str,
    reason: str,
    risk_score: int,
    risk_level: str,
    customer_id: str | None,
    context: Dict[str, Any],
    deterministic_result: InvestigationResult,
) -> InvestigationResult:

    client = _get_gemini_client()

    investigation_schema = {
        "type": "object",
        "properties": {
            "attack_pattern": {
                "type": "string",
            },
            "finding": {
                "type": "string",
            },
            "evidence": {
                "type": "array",
                "items": {
                    "type": "string",
                },
            },
            "confidence": {
                "type": "number",
            },
            "recommendation": {
                "type": "string",
                "enum": [
                    "MONITOR",
                    "STEP_UP_AUTHENTICATION",
                    "TEMPORARY_RESTRICTION",
                    "TEMPORARY_PROTECTION",
                ],
            },
        },
        "required": [
            "attack_pattern",
            "finding",
            "evidence",
            "confidence",
            "recommendation",
        ],
    }

    investigation_input = {
        "incident_id": incident_id,
        "fraud_type": fraud_type,
        "severity": severity,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "reason": reason,
        "customer_id": customer_id,
        "event_context": context,
        "deterministic_attack_pattern": (
            deterministic_result.attack_pattern
        ),
        "deterministic_finding": (
            deterministic_result.finding
        ),
    }

    prompt = f"""
You are the AI investigation layer of RetailHub FraudGuard.

Your job is to investigate a fraud incident detected by
deterministic security rules.

IMPORTANT:
- Do NOT invent evidence.
- Use only the supplied incident information.
- Explain why the activity is suspicious.
- Identify the most likely attack pattern.
- Provide a confidence between 0.0 and 1.0.
- Provide a recommendation from the allowed values.
- Your recommendation is advisory only.
- You do NOT enforce restrictions, bans, deletions, OTPs,
  account closures, or transaction decisions.
- The deterministic FraudGuard Response Engine remains
  authoritative for enforcement.

Incident data:

{json.dumps(investigation_input, indent=2, default=str)}

Return only the requested structured investigation.
"""

    response = client.models.generate_content(
        model=AI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
            disable=True
        ),
        ),
    )

    if not response.text:
        raise RuntimeError(
            "Gemini returned an empty response."
        )

    data = json.loads(response.text)

    attack_pattern = str(
        data.get(
            "attack_pattern",
            deterministic_result.attack_pattern,
        )
    )

    finding = str(
        data.get(
            "finding",
            deterministic_result.finding,
        )
    )

    evidence = data.get(
        "evidence",
        deterministic_result.evidence,
    )

    if not isinstance(evidence, list):
        evidence = deterministic_result.evidence

    evidence = [
        str(item)
        for item in evidence
    ]

    try:
        confidence = float(
            data.get(
                "confidence",
                deterministic_result.confidence,
            )
        )
    except (TypeError, ValueError):
        confidence = deterministic_result.confidence

    confidence = min(
        1.0,
        max(
            0.0,
            confidence,
        ),
    )

    recommendation = str(
        data.get(
            "recommendation",
            deterministic_result.recommendation,
        )
    ).upper()

    allowed_recommendations = {
        "MONITOR",
        "STEP_UP_AUTHENTICATION",
        "TEMPORARY_RESTRICTION",
        "TEMPORARY_PROTECTION",
    }

    if recommendation not in allowed_recommendations:
        recommendation = deterministic_result.recommendation

    return InvestigationResult(
        incident_id=incident_id,
        finding=finding,
        attack_pattern=attack_pattern,
        evidence=evidence,
        confidence=round(confidence, 2),
        recommendation=recommendation,
    )


# ---------------------------------------------------------------------------
# Public investigation interface
# ---------------------------------------------------------------------------

def investigate_incident(
    incident_id: str,
    fraud_type: str,
    severity: str,
    reason: str,
    risk_score: int,
    risk_level: str,
    customer_id: str | None = None,
    event_context: Dict[str, Any] | None = None,
) -> InvestigationResult:

    fraud_type = str(
        fraud_type or ""
    ).upper()

    severity = str(
        severity or ""
    ).upper()

    risk_level = str(
        risk_level or ""
    ).upper()

    context = event_context or {}

    # Always build deterministic investigation first.
    deterministic_result = _deterministic_investigation(
        incident_id=incident_id,
        fraud_type=fraud_type,
        severity=severity,
        reason=reason,
        risk_score=risk_score,
        risk_level=risk_level,
        customer_id=customer_id,
        context=context,
    )

    # AI disabled → deterministic mode.
    if not AI_ENABLED:
        logger.debug(
            "FraudGuard AI disabled | incident=%s",
            incident_id,
        )

        return deterministic_result

    # Current implementation supports Gemini.
    if AI_PROVIDER != "gemini":
        logger.warning(
            "Unsupported AI provider '%s' | "
            "using deterministic fallback | incident=%s",
            AI_PROVIDER,
            incident_id,
        )

        return deterministic_result

    # Gemini failure must NEVER stop FraudGuard.
    try:
        result = _gemini_investigation(
            incident_id=incident_id,
            fraud_type=fraud_type,
            severity=severity,
            reason=reason,
            risk_score=risk_score,
            risk_level=risk_level,
            customer_id=customer_id,
            context=context,
            deterministic_result=deterministic_result,
        )

        logger.info(
            "Gemini investigation successful | "
            "incident=%s | pattern=%s | confidence=%s",
            incident_id,
            result.attack_pattern,
            result.confidence,
        )

        return result

    except Exception as exc:
        logger.warning(
            "Gemini unavailable | incident=%s | "
            "fallback=deterministic | reason=%s",
            incident_id,
            str(exc).splitlines()[0],
        )

        return deterministic_result