"""Logging helper: JSON-line when `BDA_LOG_JSON=1`, plain text otherwise."""

from __future__ import annotations

import json
import logging
import os
import sys

_STD_RECORD_FIELDS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message",
})


class _JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _STD_RECORD_FIELDS:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def setup_logger(
    name: str, *, log_file: str | None = None, level: int = logging.INFO
) -> logging.Logger:
    """Idempotently configure the root logger with file and stderr handlers."""
    formatter = _select_formatter()
    root = logging.getLogger()
    root.setLevel(level)

    if log_file:
        _attach_file_handler(root, log_file, formatter, level)
    _attach_console_handler(root, formatter, level)

    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    return logging.getLogger(name)


def _select_formatter() -> logging.Formatter:
    if os.environ.get("BDA_LOG_JSON", "").strip() == "1":
        return _JsonFormatter()
    return logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def _attach_file_handler(
    root: logging.Logger, log_file: str,
    formatter: logging.Formatter, level: int,
) -> None:
    target = os.path.abspath(log_file)
    if any(isinstance(h, logging.FileHandler)
           and getattr(h, "baseFilename", "") == target
           for h in root.handlers):
        return
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    root.addHandler(handler)


def _attach_console_handler(
    root: logging.Logger, formatter: logging.Formatter, level: int
) -> None:
    if any(isinstance(h, logging.StreamHandler)
           and getattr(h, "stream", None) is sys.stderr
           for h in root.handlers):
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    root.addHandler(handler)
