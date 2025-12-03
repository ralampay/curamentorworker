"""Logging helpers that mirror output to console and to rotating log files."""

import logging
from pathlib import Path
from typing import Optional

from .config import Settings


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def get_logger(name: str = "curamentorworker", settings: Optional[Settings] = None) -> logging.Logger:
    """Return a configured logger that writes to stdout and log/<env>.log."""

    settings = settings or Settings()
    log_level = logging.DEBUG if settings.app_env != "production" else logging.INFO
    log_path = Path("log") / f"{settings.app_env}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT)

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        logger.addHandler(console_handler)

    if not any(isinstance(h, logging.FileHandler) and Path(h.baseFilename).resolve() == log_path.resolve() for h in logger.handlers):
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

    return logger
