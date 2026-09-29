from __future__ import annotations

import math
from collections import deque
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from app.models import DerivativeObservation


class DerivativeHistory:
    """Reconstruct derivative features from observations available at a decision time."""

    def __init__(self, observations: Sequence[DerivativeObservation] = ()) -> None:
        self.observations = sorted(observations, key=lambda item: item.timestamp)
        self.cursor = 0
        self.last_time: datetime | None = None
        self.values: dict[str, Decimal] = {}
        self.funding_history: deque[Decimal] = deque(maxlen=100)

    def at(self, timestamp: datetime) -> dict[str, float]:
        if self.last_time is not None and timestamp < self.last_time:
            self.cursor = 0
            self.values = {}
            self.funding_history.clear()
        while (
            self.cursor < len(self.observations)
            and self.observations[self.cursor].timestamp <= timestamp
        ):
            observation = self.observations[self.cursor]
            self._apply(observation)
            self.cursor += 1
        self.last_time = timestamp
        return {key: float(value) for key, value in self.values.items()}

    def _apply(self, observation: DerivativeObservation) -> None:
        kind, value = observation.kind, observation.value
        previous = self.values.get(kind)
        self.values[kind] = value
        if kind == "oi" and previous is not None and previous > 0:
            self.values["oi_change"] = (value - previous) / previous
        if kind == "funding":
            if not self.funding_history or self.funding_history[-1] != value:
                self.funding_history.append(value)
            if len(self.funding_history) >= 10:
                mean = sum(self.funding_history, Decimal(0)) / len(self.funding_history)
                variance = sum(
                    (float(item - mean) ** 2 for item in self.funding_history), 0.0
                ) / len(self.funding_history)
                self.values["funding_zscore"] = Decimal(
                    str(float(value - mean) / math.sqrt(variance) if variance else 0)
                )
        index = self.values.get("index")
        mark = self.values.get("mark")
        if index is not None and index > 0 and mark is not None:
            self.values["premium"] = (mark - index) / index
