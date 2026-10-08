"""Exchange fill identity and immutable evidence comparison."""
from decimal import Decimal
from typing import Any

FillKey = tuple[str, str]


def fill_key(inst_id: object, trade_id: object) -> FillKey:
    # Do not coerce or trim exchange identifiers into a different identity.
    if (not isinstance(inst_id, str) or not isinstance(trade_id, str)
            or not inst_id or not trade_id or inst_id != inst_id.strip() or trade_id != trade_id.strip()):
        raise ValueError("invalid fill identity")
    return inst_id, trade_id


def equivalent_fill(local: dict[str, Any], remote: dict[str, Any]) -> bool:
    for key in ("instId", "tradeId", "ordId", "side", "fillTime"):
        if str(local.get(key) or "") != str(remote.get(key) or ""):
            return False
    for key in ("clOrdId", "posSide", "feeCcy"):
        if local.get(key) and remote.get(key) and local[key] != remote[key]:
            return False
    for key in ("fillSz", "fillPx", "fee", "fillPnl"):
        a = (local.get("fillFee") or local.get("fee")) if key == "fee" else local.get(key)
        b = (remote.get("fillFee") or remote.get("fee")) if key == "fee" else remote.get(key)
        if a in (None, "") and b in (None, ""):
            continue
        if a in (None, "") or b in (None, ""):
            return False
        try:
            left, right = Decimal(str(a)), Decimal(str(b))
            if not left.is_finite() or not right.is_finite() or left != right:
                return False
        except (ArithmeticError, TypeError, ValueError):
            return False
    return True
