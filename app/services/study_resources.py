from time import perf_counter
from contextlib import contextmanager
from app.models.study_payload import StudyPayload
import app.databricks_api as dbx
from app.core.config import ACCESS_MAP
from app.core.logging_config import get_logger

logger = get_logger("study_resources")


@contextmanager
def timed_op(event: str, extra: dict | None = None):
    """
    Context manager to time an operation and log success/failure with duration.
    Usage:
        with timed_op("create_schema", {"schema": schema_name, "catalog": catalog_name}):
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


def process_payload(payload: StudyPayload):
    logger.debug(
        f"Received payload",
        extra={"event": "payload_received", "payload": payload.dict()},
    )
    logger.info("Starting process_payload", extra={"event": "process_payload_start"})

    start_total = perf_counter()

    catalog_name = payload.business_metadata.product_name.lower()
    study = payload.business_metadata.study

    # 1. Check if catalog exists
    with timed_op("list_catalogs"):
        catalogs = dbx.list_catalogs().get("catalogs", [])

    if catalog_name not in [c["name"] for c in catalogs]:
        logger.error(
            "Catalog not found",
            extra={"event": "catalog_missing", "catalog": catalog_name},
        )
        raise dbx.DatabricksAPIError(404, f"Catalog {catalog_name} not found")

    # 2. Create schemas
    for schema in payload.storage_setup.data_schemas:
        schema_name = f"{study}_{schema}"
        with timed_op(
            "create_schema", {"schema": schema_name, "catalog": catalog_name}
        ):
            dbx.create_schema(schema_name, catalog_name)

    # 3. Create volumes under studyname_volumes schema
    volume_schema = f"{study}_volumes"
    for schema in payload.storage_setup.data_schemas:
        if schema == "volumes":
            continue
        volume_name = f"vol_{schema}"
        with timed_op(
            "create_volume",
            {"volume": volume_name, "schema": volume_schema, "catalog": catalog_name},
        ):
            dbx.create_volume(volume_name, volume_schema, catalog_name)

    # 4. Create directories inside volumes
    # If volume_directories is a dict, use .items(); if it's a list of tuples, the below loop works as-is.
    for volume_type, dirs in payload.storage_setup.volume_directories:
        volume_name = f"vol_{volume_type}"
        for directory_name in dirs:
            with timed_op(
                "create_directory",
                {
                    "directory": directory_name,
                    "volume": volume_name,
                    "schema": volume_schema,
                    "catalog": catalog_name,
                },
            ):
                dbx.create_directory(
                    directory_name, volume_name, volume_schema, catalog_name
                )

    # 5. Apply access controls
    access_controls = payload.access_controls
    if access_controls:
        for schema_name, control in access_controls.items():
            changes = []

            for group in control.groups or []:
                access = ACCESS_MAP.get(group.access, [])
                if not access:
                    logger.warning(
                        "Invalid access level",
                        extra={
                            "event": "invalid_access_level",
                            "group": group.group,
                            "access": group.access,
                        },
                    )
                    continue

                changes.append({"add": access, "principal": f"{group.group}"})

            access_payload = {"changes": changes}
            logger.debug(
                "Prepared access payload",
                extra={
                    "event": "access_payload_prepared",
                    "schema": schema_name,
                    "changes_count": len(changes),
                },
            )

            full_name = f"{catalog_name}.{study}_{schema_name}"
            with timed_op(
                "grant_permissions",
                {
                    "object_type": "SCHEMA",
                    "full_name": full_name,
                    "changes_count": len(changes),
                },
            ):
                dbx.grant_permissions("SCHEMA", full_name, access_payload)

    total_duration_ms = int((perf_counter() - start_total) * 1000)
    logger.info(
        "Completed process_payload successfully",
        extra={"event": "process_payload_completed", "duration_ms": total_duration_ms},
    )

    return {"status": "success", "message": "All resources created successfully"}
