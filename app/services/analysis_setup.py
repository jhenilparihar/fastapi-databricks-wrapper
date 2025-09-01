from time import perf_counter
from app.models.analysis_payload import AnalysisPayload
import app.databricks_api as dbx
from app.core.config import ACCESS_MAP
from app.core.logging_config import get_logger
from app.utils.time_logging import timed_op

logger = get_logger("analysis_setup")


def process_analysis_payload(payload: AnalysisPayload):
    logger.debug(
        f"Received payload",
        extra={"event": "payload_received", "payload": payload.dict()},
    )
    logger.info("Starting process_payload for analysis setup", extra={"event": "process_payload_start"})

    start_total = perf_counter()

    catalog_name = payload.business_metadata.product_name.lower()
    study = payload.business_metadata.study

    # 1. Check if catalog exists
    with timed_op(logger=logger, event="list_catalogs"):
        catalogs = dbx.list_catalogs().get("catalogs", [])

    if catalog_name not in [c["name"] for c in catalogs]:
        logger.error(
            "Catalog not found",
            extra={"event": "catalog_missing", "catalog": catalog_name},
        )
        raise dbx.DatabricksAPIError(404, f"Catalog {catalog_name} not found")

    # 2. Create schemas
    for schema in payload.storage_setup.data_layer_schemas:
        schema_name = f"{study}_{payload.business_metadata.analysis_type}_{schema}"
        with timed_op(
            logger=logger,
            event="create_schema",
            extra={"schema": schema_name, "catalog": catalog_name},
        ):
            dbx.create_schema(schema_name, catalog_name)

    # 3. volumes already under studyname_volumes schema
    volume_schema = f"{study}_volumes"

    volume_name = f"{payload.business_metadata.analysis_type}_vol"
    with timed_op(
        logger=logger,
        event="create_volume",
        extra={
            "volume": volume_name,
            "schema": volume_schema,
            "catalog": catalog_name,
        },
    ):
        dbx.create_volume(volume_name, volume_schema, catalog_name)

    # 4. Create directories inside volumes
    for directory in payload.storage_setup.volume_directories:
        with timed_op(
            logger=logger,
            event="create_directory",
            extra={
                "directory": directory,
                "volume": volume_name,
                "schema": volume_schema,
                "catalog": catalog_name,
            },
        ):
            dbx.create_directory(directory, volume_name, volume_schema, catalog_name)

    # 5. Apply access controls

    """
    Apply table action to analysis schema
    Apply volume action to volume schema
    """
    access_controls = payload.access_controls
    if access_controls:
        for _, group in access_controls.items():
            table_changes = []
            volume_changes = []

            table_access = ACCESS_MAP.get(group.table_actions, [])
            vol_access = ACCESS_MAP.get(group.volume_action, [])
            if not table_access:
                logger.warning(
                    "Invalid access level",
                        extra={
                            "event": "invalid_access_level",
                            "group": group.group,
                            "access": group.table_actions,
                        },
                    )
            else:
                table_changes.append({"add": table_access, "principal": f"{group.group}"})

            if not vol_access:
                logger.warning(
                    "Invalid access level",
                        extra={
                            "event": "invalid_access_level",
                            "group": group.group,
                            "access": group.volume_action,
                        },
                    )
            else:
                volume_changes.append({"add": vol_access, "principal": f"{group.group}"})

        access_payload = {"changes": table_changes}
        logger.debug(
            "Prepared access payload for analysis schema",
            extra={
                "event": "access_payload_prepared",
                "schema": schema_name,
                "changes_count": len(table_changes),
            },
        )

        for schema in payload.storage_setup.data_layer_schemas:
            full_name = f"{catalog_name}.{study}_{payload.business_metadata.analysis_type}_{schema}"
            with timed_op(
                logger=logger,
                event="grant_permissions",
                extra={
                    "object_type": "SCHEMA",
                    "full_name": full_name,
                    "changes_count": len(table_changes),
                },
            ):
                dbx.grant_permissions("SCHEMA", full_name, access_payload)

        access_payload = {"changes": volume_changes}
        logger.debug(
            "Prepared access payload for volume schema",
            extra={
                "event": "access_payload_prepared",
                "schema": schema_name,
                "changes_count": len(volume_changes),
            },
        )

        full_name = f"{catalog_name}.{study}_volumes"
        with timed_op(
            logger=logger,
            event="grant_permissions",
            extra={
                "object_type": "SCHEMA",
                "full_name": full_name,
                "changes_count": len(volume_changes),
            },
        ):
            dbx.grant_permissions("SCHEMA", full_name, access_payload)

    total_duration_ms = int((perf_counter() - start_total) * 1000)
    logger.info(
        "Completed process_payload successfully",
        extra={"event": "process_payload_completed", "duration_ms": total_duration_ms},
    )

    return {"status": "success", "message": "All resources created successfully"}
