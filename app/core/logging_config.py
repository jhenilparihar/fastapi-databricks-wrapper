import logging
import os
import json
from logging.handlers import RotatingFileHandler
from datetime import datetime

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }

        for key, value in record.__dict__.items():
            if key not in log_record and key not in (
                "args",
                "msg",
                "name",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            ):
                log_record[key] = value

        return json.dumps(log_record)


def _create_handler(file_path, level=logging.DEBUG):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    handler = RotatingFileHandler(
        file_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(JSONFormatter())
    return handler


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    service_dir = os.path.join(LOG_DIR, name)

    # Combined service log
    logger.addHandler(_create_handler(os.path.join(service_dir, f"{name}.log")))

    # Separate files per level
    logger.addHandler(
        _create_handler(os.path.join(service_dir, "debug.log"), level=logging.DEBUG)
    )
    logger.addHandler(
        _create_handler(os.path.join(service_dir, "info.log"), level=logging.INFO)
    )
    logger.addHandler(
        _create_handler(os.path.join(service_dir, "warning.log"), level=logging.WARNING)
    )
    logger.addHandler(
        _create_handler(os.path.join(service_dir, "error.log"), level=logging.ERROR)
    )
    logger.addHandler(
        _create_handler(
            os.path.join(service_dir, "critical.log"), level=logging.CRITICAL
        )
    )

    # Global all.log
    logger.addHandler(_create_handler(os.path.join(LOG_DIR, "all.log")))

    logger.propagate = False
    return logger
