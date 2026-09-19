"""Kosmic Tarang trade lifecycle state machine (valid transitions only)."""
from __future__ import annotations

from typing import Dict, FrozenSet, Optional, Set

# Candidate-facing statuses (screener) and trade statuses share one vocabulary
# so events can reference either side of the lifecycle.

SCREENING = "SCREENING"
QUALIFIED = "QUALIFIED"
EXPIRED = "EXPIRED"
DISMISSED = "DISMISSED"

ENTRY_PENDING = "ENTRY_PENDING"
IN_TRADE = "IN_TRADE"
EXIT_PENDING = "EXIT_PENDING"
CLOSED = "CLOSED"
REPORTED = "REPORTED"

# Paper stubs for partial-fill / reject recovery (Phase 3: stub only)
PARTIAL_FILL = "PARTIAL_FILL"
REJECTED = "REJECTED"
UNWINDING = "UNWINDING"

OPEN_STATUSES: FrozenSet[str] = frozenset({ENTRY_PENDING, IN_TRADE, EXIT_PENDING, PARTIAL_FILL, UNWINDING})
CLOSED_LIKE: FrozenSet[str] = frozenset({CLOSED, REPORTED, EXPIRED, DISMISSED, REJECTED})

# from -> allowed to
_TRANSITIONS: Dict[str, Set[str]] = {
    SCREENING: {QUALIFIED, EXPIRED, DISMISSED},
    QUALIFIED: {ENTRY_PENDING, EXPIRED, DISMISSED},
    ENTRY_PENDING: {IN_TRADE, PARTIAL_FILL, REJECTED, DISMISSED},
    PARTIAL_FILL: {IN_TRADE, UNWINDING, REJECTED, EXIT_PENDING},
    REJECTED: {UNWINDING, DISMISSED, CLOSED},
    UNWINDING: {CLOSED, REJECTED},
    IN_TRADE: {EXIT_PENDING, CLOSED},
    EXIT_PENDING: {CLOSED, IN_TRADE},  # IN_TRADE = cancel exit / recovery stub
    CLOSED: {REPORTED},
    REPORTED: set(),
    EXPIRED: set(),
    DISMISSED: set(),
}

EXIT_REASONS = frozenset(
    {
        "PROFIT_TARGET",
        "CREDIT_STOP",
        "DELTA_STOP",
        "BUDGET_STOP",
        "IV_STOP",
        "HARD_EXIT",
        "TIME_STOP",
        "EVENT_STOP",
        "MANUAL",
        "PARTIAL_UNWIND",
        "REJECT_RECOVERY",
        "EXPIRED",
        "DISMISSED",
    }
)

ACTORS = frozenset({"USER", "AUTO", "SYSTEM"})


class InvalidTransition(ValueError):
    pass


def normalize_status(status: Optional[str]) -> str:
    return str(status or "").strip().upper().replace(" ", "_")


def can_transition(from_status: Optional[str], to_status: str) -> bool:
    fr = normalize_status(from_status)
    to = normalize_status(to_status)
    if not fr:
        # First status assignment (insert)
        return to in _TRANSITIONS or to in CLOSED_LIKE
    allowed = _TRANSITIONS.get(fr)
    if allowed is None:
        return False
    return to in allowed


def assert_transition(from_status: Optional[str], to_status: str) -> None:
    if not can_transition(from_status, to_status):
        raise InvalidTransition(
            f"invalid transition {normalize_status(from_status)!r} -> {normalize_status(to_status)!r}"
        )


def allowed_targets(from_status: Optional[str]) -> Set[str]:
    fr = normalize_status(from_status)
    return set(_TRANSITIONS.get(fr) or set())
