"""
RetailHub FraudGuard
Security audit logger.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.config.settings import FRAUDGUARD_AUDIT_PATH


_LOCK = threading.Lock()

AUDIT_FILE = os.path.join(
    FRAUDGUARD_AUDIT_PATH,
    "fraudguard_audit.jsonl",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_audit(
    *,
    agent: str,
    event: str,
    reason: str,
    evidence: Optional[Dict[str, Any]] = None,
    decision: Optional[str] = None,
    confidence: Optional[float] = None,
    action: Optional[str] = None,
    approval: Optional[str] = None,
    result: Optional[str] = None,
    incident_id: Optional[str] = None,
    customer_id: Optional[str] = None,
) -> Dict[str, Any]:

    record = {
        "timestamp": _now(),
        "agent": agent,
        "event": event,
        "reason": reason,
        "evidence": evidence or {},
        "decision": decision,
        "confidence": confidence,
        "action": action,
        "approval": approval,
        "result": result,
        "incident_id": incident_id,
        "customer_id": customer_id,
    }

    os.makedirs(
        FRAUDGUARD_AUDIT_PATH,
        exist_ok=True,
    )

    with _LOCK:
        with open(AUDIT_FILE, "a") as f:
            f.write(
                json.dumps(
                    record,
                    default=str,
                )
                + "\n"
            )

    return record