from pydantic_model import Payload
import databricks_api as dbx

ACCESS_MAP = {
    "read_only": ["SELECT", "READ_VOLUME", "EXECUTE"],
    "create": ["CREATE_VOLUME", "CREATE_TABLE"],
    "manage": ["MANAGE"],
    "full": ["ALL_PRIVILEGES"],
}


def process_payload(payload: Payload):

    catalog_name = payload.business_metadata.product.lower()
    study = payload.business_metadata.study

    # 1. Check if catalog exists
    catalogs = dbx.list_catalogs().get("catalogs", [])

    if catalog_name not in [c["name"] for c in catalogs]:
        raise dbx.DatabricksAPIError(404, f"Catalog {catalog_name} not found")

    # 2. Create schemas
    for schema in payload.storage_setup.data_schemas:
        schema_name = f"{study}_{schema}"
        dbx.create_schema(schema_name, catalog_name)
        print(f"Schema {schema_name} created in catalog {catalog_name}")

    # 3. Create volumes under studyname_volumes schema
    volume_schema = f"{study}_volumes"
    for schema in payload.storage_setup.data_schemas:
        if schema == "volumes":
            continue
        volume_name = f"vol_{schema}"
        dbx.create_volume(volume_name, volume_schema, catalog_name)
        print(f"Volume {volume_name} created in schema {volume_schema}")

    # 4. Create directories inside volumes
    for volume_type, dirs in payload.storage_setup.volume_directories:
        volume_name = f"vol_{volume_type}"
        for directory_name in dirs:
            dbx.create_directory(
                directory_name, volume_name, volume_schema, catalog_name
            )
            print(f"Directory {directory_name} created in volume {volume_name}")

    # 5. Apply access controls
    access_controls = payload.access_controls

    if access_controls:
        for schema_name, control in access_controls.items():

            changes = []

            for group in control.groups or []:
                access = ACCESS_MAP.get(group.access, [])
                if len(access) == 0:
                    print(
                        f"Invalid access level {group.access} for group {group.group}"
                    )
                    continue

                changes.append(
                    {
                        "add": access,
                        "principal": f"{group.group}",
                    }
                )

            for user in control.users or []:
                access = ACCESS_MAP.get(user.access, [])
                if len(access) == 0:
                    print(f"Invalid access level {user.access} for user {user.user}")
                    continue

                changes.append(
                    {
                        "add": access,
                        "principal": f"{user.user}",
                    }
                )

            access_payload = {"changes": changes}

            dbx.grant_permissions(
                "schema", f"{catalog_name}.{study}_{schema_name}", access_payload
            )
            print(f"Access controls applied for {catalog_name}.{study}_{schema_name}")

    return {"status": "success", "message": "All resources created successfully"}
