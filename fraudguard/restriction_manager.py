"""
RetailHub FraudGuard
Temporary restriction manager.

No permanent bans.
No account deletion.
All restrictions have an expiry time.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from src.config.settings import FRAUDGUARD_STATE_PATH


_LOCK = threading.Lock()

RESTRICTION_FILE = os.path.join(
    FRAUDGUARD_STATE_PATH,
    "restrictions.jsonl",
)


def _ensure_directory() -> None:
    os.makedirs(
        FRAUDGUARD_STATE_PATH,
        exist_ok=True,
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _write_record(record: Dict[str, Any]) -> None:
    _ensure_directory()

    with _LOCK:
        with open(RESTRICTION_FILE, "a") as f:
            f.write(
                json.dumps(
                    record,
                    default=str,
                )
                + "\n"
            )


# ---------------------------------------------------------------------------
# CREATE
# ---------------------------------------------------------------------------

def create_restriction(
    customer_id: str,
    incident_id: str,
    reason: str,
    restriction_type: str = "CHECKOUT_BLOCK",
    duration_minutes: int = 1440,
) -> Dict[str, Any]:

    created_at = _now()

    expires_at = created_at + timedelta(
        minutes=duration_minutes
    )

    restriction_id = (
        f"RES-{created_at.strftime('%Y%m%d')}-"
        f"{uuid.uuid4().hex[:8].upper()}"
    )

    record = {
        "restriction_id": restriction_id,
        "customer_id": customer_id,
        "incident_id": incident_id,
        "status": "RESTRICTED",
        "restriction_type": restriction_type,
        "reason": reason,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }

    _write_record(record)

    return record


# ---------------------------------------------------------------------------
# RELEASE
# ---------------------------------------------------------------------------

def release_restriction(
    restriction_id: str,
    customer_id: str,
    reason: str,
) -> Dict[str, Any]:

    record = {
        "restriction_id": restriction_id,
        "customer_id": customer_id,
        "status": "RELEASED",
        "reason": reason,
        "released_at": _now().isoformat(),
    }

    _write_record(record)

    return record


# ---------------------------------------------------------------------------
# EXPIRY CHECK
# ---------------------------------------------------------------------------

def is_expired(
    expires_at: str,
) -> bool:

    try:
        expiry = datetime.fromisoformat(
            expires_at.replace("Z", "+00:00")
        )

        return _now() >= expiry

    except Exception:
        return False


# ---------------------------------------------------------------------------
# STATUS
# ---------------------------------------------------------------------------

def restriction_status(
    restriction: Dict[str, Any],
) -> str:

    status = restriction.get("status")

    if status != "RESTRICTED":
        return status or "UNKNOWN"

    expires_at = restriction.get("expires_at")

    if expires_at and is_expired(expires_at):
        return "EXPIRED"

    return "RESTRICTED"