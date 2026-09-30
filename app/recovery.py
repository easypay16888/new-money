from __future__ import annotations

from enum import StrEnum


class HaltClass(StrEnum):
    TRANSIENT_INFRA = "TRANSIENT_INFRA"
    SAFETY_OR_MANUAL = "SAFETY_OR_MANUAL"


AUTO_RECOVERABLE_REASONS = frozenset({
    "reconciliation failed",
    "WebSocket disconnected or stale",
    "Redis unavailable",
    "dead man switch unavailable",
})


def classify_halt_reason(reason: str) -> HaltClass:
    if reason in AUTO_RECOVERABLE_REASONS:
        return HaltClass.TRANSIENT_INFRA
    return HaltClass.SAFETY_OR_MANUAL
