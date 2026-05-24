"""
Shared logging helper. Use `get_logger(__name__)` in any module to get
a consistently-formatted logger that writes to stdout.
"""

from __future__ import annotations

import logging
import sys


_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:                                  # already configured
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, _DATE_FORMAT))
    logger.addHandler(handler)
    logger.propagate = False
    return logger
