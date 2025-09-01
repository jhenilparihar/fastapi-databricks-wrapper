from time import perf_counter
from contextlib import contextmanager


@contextmanager
def timed_op(logger, event: str, extra: dict | None = None):
    """
    Context manager to time an operation and log success/failure with duration.
    Usage:
        with timed_op(logger, "create_schema", {"schema": schema_name, "catalog": catalog_name}):
            dbx.create_schema(schema_name, catalog_name)
    """
    extra = extra or {}
    start = perf_counter()
    try:
        yield
        duration_ms = int((perf_counter() - start) * 1000)
        logger.info("ok", extra={"event": event, "duration_ms": duration_ms, **extra})
    except Exception as e:
        duration_ms = int((perf_counter() - start) * 1000)
        logger.error(
            "fail",
            extra={
                "event": event,
                "duration_ms": duration_ms,
                "error": str(e),
                **extra,
            },
        )
        raise
