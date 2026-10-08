"""
RetailHub FraudGuard
Risk scoring layer.

Converts deterministic fraud signals into a normalized
0-100 risk score and risk level.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# RISK LEVELS
# ---------------------------------------------------------------------------

LOW = "LOW"
MEDIUM = "MEDIUM"
HIGH = "HIGH"
VERY_HIGH = "VERY_HIGH"
CRITICAL = "CRITICAL"


@dataclass
class RiskSignal:
    rule_id: str
    severity: str
    score: int
    reason: str
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskAssessment:
    score: int
    level: str
    signals: List[RiskSignal] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "risk_score": self.score,
            "risk_level": self.level,
            "signals": [
                {
                    "rule_id": signal.rule_id,
                    "severity": signal.severity,
                    "score": signal.score,
                    "reason": signal.reason,
                    "evidence": signal.evidence,
                }
                for signal in self.signals
            ],
        }


# ---------------------------------------------------------------------------
# BASE SCORES
# ---------------------------------------------------------------------------

RULE_SCORES = {
    "BRUTE_FORCE_LOGIN": 55,
    "MULTI_IP_LOGIN_ATTACK": 60,
    "PAYMENT_FAILURE_VELOCITY": 60,
    "MULTIPLE_PAYMENT_METHODS": 65,
    "HIGH_VALUE_TRANSACTION": 55,
    "HIGH_VALUE_ORDER_VELOCITY": 75,
    "ACCOUNT_CHANGE_NEW_DEVICE": 75,
    "ACCOUNT_TAKEOVER_SEQUENCE": 95,
    "MULTI_ACCOUNT_DEVICE": 80,
    "MULTI_ACCOUNT_IP": 65,
    "COUPON_ABUSE": 60,
    "REFUND_ABUSE": 60,
    "BOT_OR_SCRAPER": 60,
    "DDOS": 90,
    "CHECKOUT_VELOCITY": 55,
}


# ---------------------------------------------------------------------------
# LEVEL CONVERSION
# ---------------------------------------------------------------------------

def score_to_level(score: int) -> str:
    score = max(0, min(100, int(score)))

    if score >= 85:
        return CRITICAL

    if score >= 70:
        return VERY_HIGH

    if score >= 50:
        return HIGH

    if score >= 30:
        return MEDIUM

    return LOW


# ---------------------------------------------------------------------------
# SINGLE INCIDENT
# ---------------------------------------------------------------------------

def calculate_risk(
    fraud_type: str,
    severity: str,
    reason: str = "",
    evidence: Dict[str, Any] | None = None,
) -> RiskAssessment:
    """
    Calculate risk for one deterministic fraud signal.
    """

    fraud_type = str(fraud_type or "").upper()
    severity = str(severity or "").upper()

    base_score = RULE_SCORES.get(fraud_type)

    if base_score is None:
        severity_defaults = {
            LOW: 20,
            MEDIUM: 40,
            HIGH: 60,
            VERY_HIGH: 75,
            CRITICAL: 90,
        }

        base_score = severity_defaults.get(severity, 50)

    signal = RiskSignal(
        rule_id=fraud_type,
        severity=severity,
        score=base_score,
        reason=reason,
        evidence=evidence or {},
    )

    return RiskAssessment(
        score=base_score,
        level=score_to_level(base_score),
        signals=[signal],
    )


# ---------------------------------------------------------------------------
# MULTIPLE SIGNALS
# ---------------------------------------------------------------------------

def combine_risk_signals(
    assessments: List[RiskAssessment],
) -> RiskAssessment:
    """
    Combine multiple fraud signals for the same investigation.

    We intentionally use diminishing contribution rather than simply
    summing all scores, so three signals do not automatically produce
    an unrealistic 180+ score.
    """

    if not assessments:
        return RiskAssessment(
            score=0,
            level=LOW,
            signals=[],
        )

    signals: List[RiskSignal] = []

    for assessment in assessments:
        signals.extend(assessment.signals)

    scores = sorted(
        [signal.score for signal in signals],
        reverse=True,
    )

    total = 0.0

    # Strongest signal contributes fully.
    if scores:
        total += scores[0]

    # Additional signals contribute progressively less.
    multipliers = [0.50, 0.30, 0.20, 0.15, 0.10]

    for index, score in enumerate(scores[1:]):
        multiplier = multipliers[min(index, len(multipliers) - 1)]
        total += score * multiplier

    # Multiple independent signals are stronger evidence.
    unique_rules = {
        signal.rule_id
        for signal in signals
    }

    if len(unique_rules) >= 3:
        total += 10
    elif len(unique_rules) == 2:
        total += 5

    final_score = min(100, round(total))

    return RiskAssessment(
        score=final_score,
        level=score_to_level(final_score),
        signals=signals,
    )