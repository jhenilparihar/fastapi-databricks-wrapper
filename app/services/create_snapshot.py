import time
import app.databricks_api as dbx
from app.models.snapshot_payload import CreateSnapshotPayload


def create_snapshot(payload: CreateSnapshotPayload):
    catalog_name = payload.product.lower()
    study = payload.study

    # 1. Check if catalog exists
    catalogs = dbx.list_catalogs().get("catalogs", [])

    if catalog_name not in [c["name"] for c in catalogs]:
        raise dbx.DatabricksAPIError(404, f"Catalog {catalog_name} not found")

    # 2. Check table exists

    table_fullname = dbx.get_tables(payload.source_table_fullname)
    if not table_fullname:
        raise dbx.DatabricksAPIError(
            404, f"Table {payload.source_table_fullname} not found"
        )

    # 3. create schema

    dbx.create_schema(schema_name=f"{study}_snapshot", catalog_name=catalog_name)

    # 3. Create snapshot
    new_table = f"{catalog_name}.{study}_snapshot.{payload.source_table_fullname.split('.')[-1]}"
    statement = f"CREATE TABLE {new_table} DEEP CLONE {payload.source_table_fullname} TIMESTAMP AS OF '{payload.timestamp}'"
    result = dbx.execute_statement(statement=statement)
    print(f"Snapshot job started: {result}")
    while result["status"]["state"] in ["PENDING", "RUNNING"]:
        time.sleep(3)
        result = dbx.sql_status(result["statement_id"])
    if result["status"]["state"] != "SUCCEEDED":
        raise dbx.DatabricksAPIError(500, f"Snapshot {new_table} failed to complete")
