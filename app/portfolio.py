from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.models import Instrument


def summarize_positions(
    rows: list[dict[str, Any]], instruments: dict[str, Instrument], equity: Decimal
) -> dict[str, Decimal | None]:
    notional = signed_notional = unrealized = realized = correlated = Decimal(0)
    liquidation_distances: list[Decimal] = []
    for row in rows:
        quantity = Decimal(row.get("pos") or "0")
        if quantity == 0:
            continue
        symbol = row["instId"]
        instrument = instruments.get(symbol)
        mark = Decimal(row.get("markPx") or "0")
        if row.get("notionalUsd"):
            value = abs(Decimal(row["notionalUsd"]))
        elif instrument and mark > 0:
            value = abs(quantity * instrument.contract_value * mark)
        else:
            raise ValueError("position notional cannot be determined")
        notional += value
        signed_notional += value if quantity > 0 else -value
        if symbol.startswith(("BTC-", "ETH-")):
            correlated += value
        unrealized += Decimal(row.get("upl") or "0")
        realized += Decimal(row.get("realizedPnl") or "0")
        liquidation = Decimal(row.get("liqPx") or "0")
        if liquidation > 0 and mark > 0:
            liquidation_distances.append(abs(mark - liquidation) / mark)
    return {
        "position_notional": notional,
        "effective_leverage": notional / equity if equity else Decimal(0),
        "unrealized_pnl": unrealized,
        "realized_pnl": realized,
        "directional_exposure": signed_notional / equity if equity else Decimal(0),
        "correlation_exposure": correlated / equity if equity else Decimal(0),
        "liquidation_distance": min(liquidation_distances) if liquidation_distances else None,
    }
