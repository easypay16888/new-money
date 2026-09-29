from app.api import app
from app.logging import configure_logging

configure_logging()

__all__ = ["app"]
