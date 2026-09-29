import json
import logging
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
            "symbol": None,
            "strategy": None,
            "signal_id": None,
            "order_id": None,
            "clOrdId": None,
            "risk_state": None,
            "error_code": None,
        }
        for key in (
            "symbol",
            "strategy",
            "signal_id",
            "order_id",
            "clOrdId",
            "risk_state",
            "error_code",
        ):
            value = getattr(record, key, None)
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
