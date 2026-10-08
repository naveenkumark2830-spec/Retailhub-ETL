"""
RetailHub FraudGuard
Response decision engine.

Detection tells us WHAT happened.
Risk scoring tells us HOW serious it is.
This module decides WHAT ACTION is permitted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


# ---------------------------------------------------------------------------
# ACTIONS
# ---------------------------------------------------------------------------

MONITOR = "MONITOR"

STEP_UP_AUTHENTICATION = "STEP_UP_AUTHENTICATION"

TEMPORARY_RESTRICTION = "TEMPORARY_RESTRICTION"

TEMPORARY_PROTECTION = "TEMPORARY_PROTECTION"

ADMIN_REVIEW = "ADMIN_REVIEW"


@dataclass
class ResponseDecision:
    action: str
    reason: str
    requires_customer_action: bool = False
    requires_admin_review: bool = False
    restriction_minutes: int | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "reason": self.reason,
            "requires_customer_action": self.requires_customer_action,
            "requires_admin_review": self.requires_admin_review,
            "restriction_minutes": self.restriction_minutes,
        }


# ---------------------------------------------------------------------------
# DEFAULT RESTRICTION PERIODS
# ---------------------------------------------------------------------------

DEFAULT_RESTRICTION_MINUTES = 24 * 60


# ---------------------------------------------------------------------------
# DECISION ENGINE
# ---------------------------------------------------------------------------

def decide_response(
    risk_level: str,
    incident_count: int = 1,
) -> ResponseDecision:

    level = str(risk_level or "LOW").upper()

    # ---------------------------------------------------------------
    # LOW
    # ---------------------------------------------------------------

    if level == "LOW":
        return ResponseDecision(
            action=MONITOR,
            reason="Low-risk activity; continue monitoring.",
        )

    # ---------------------------------------------------------------
    # MEDIUM
    # ---------------------------------------------------------------

    if level == "MEDIUM":
        return ResponseDecision(
            action=MONITOR,
            reason="Medium-risk activity; continue monitoring for additional signals.",
        )

    # ---------------------------------------------------------------
    # HIGH
    # ---------------------------------------------------------------

    if level == "HIGH":
        return ResponseDecision(
            action=STEP_UP_AUTHENTICATION,
            reason="High-risk activity requires customer verification.",
            requires_customer_action=True,
        )

    # ---------------------------------------------------------------
    # VERY HIGH
    # ---------------------------------------------------------------

    if level == "VERY_HIGH":
        return ResponseDecision(
            action=TEMPORARY_RESTRICTION,
            reason="Very-high-risk activity requires temporary protection.",
            restriction_minutes=DEFAULT_RESTRICTION_MINUTES,
        )

    # ---------------------------------------------------------------
    # CRITICAL
    # ---------------------------------------------------------------

    if level == "CRITICAL":

        # Repeated critical incidents require human review.
        if incident_count >= 3:
            return ResponseDecision(
                action=ADMIN_REVIEW,
                reason=(
                    "Repeated critical-risk activity detected; "
                    "temporary protection and administrator review required."
                ),
                requires_admin_review=True,
                restriction_minutes=DEFAULT_RESTRICTION_MINUTES,
            )

        return ResponseDecision(
            action=TEMPORARY_PROTECTION,
            reason=(
                "Critical-risk activity detected; temporary protection "
                "is required while the incident is investigated."
            ),
            requires_admin_review=True,
            restriction_minutes=DEFAULT_RESTRICTION_MINUTES,
        )

    # ---------------------------------------------------------------
    # UNKNOWN
    # ---------------------------------------------------------------

    return ResponseDecision(
        action=MONITOR,
        reason="Unknown risk level; defaulting to monitoring.",
    )