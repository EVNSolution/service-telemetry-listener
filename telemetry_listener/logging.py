from __future__ import annotations

import logging


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _normalize_level(level: str) -> int:
    candidate = level.strip().upper()
    resolved = logging.getLevelName(candidate)
    if isinstance(resolved, int):
        return resolved
    return logging.INFO


def configure_logging(level: str = "INFO") -> logging.Logger:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root_logger.addHandler(handler)

    root_logger.setLevel(_normalize_level(level))
    return logging.getLogger("telemetry_listener")
