from fastapi import APIRouter, HTTPException
from core.catalog_client import get_catalog_client
from pyiceberg.transforms import DayTransform
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.catalog import (
    NoSuchNamespaceError,
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError
)
from last_value.iceberg__schema import IngestionTrackingSchema

router = APIRouter(prefix="/tracking", tags=["tracking"])


@router.post("/create")
def create():
    namespace = "order_fulfillment"
    table_name = "Tracking"
    table_identifier = f"{namespace}.{table_name}"

    catalog = get_catalog_client()

    try:
        # -------------------------
        # 1️⃣ Build Schema
        # -------------------------
        tracking_schema = Schema(*IngestionTrackingSchema)

        updated_field = tracking_schema.find_field("updated_at")
        id_field = tracking_schema.find_field("id")  # replace with actual PK column

        if not updated_field:
            raise ValueError("updated_at field missing in schema")

        # -------------------------
        # 2️⃣ Partition Spec
        # -------------------------
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=updated_field.field_id,
                field_id=2001,
                transform=DayTransform(),
                name="updated_day",
            ),
        )

        # -------------------------
        # 3️⃣ Ensure Namespace
        # -------------------------
        try:
            catalog.load_namespace_properties(namespace)
        except NoSuchNamespaceError:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        # -------------------------
        # 4️⃣ Create Table
        # -------------------------
        catalog.create_table(
            identifier=table_identifier,
            schema=tracking_schema,
            partition_spec=partition_spec,
            properties={
                "format-version": "2",
                "table-type": "MERGE_ON_READ",
                "identifier-field-ids": str(id_field.field_id),
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",
                "write.target-file-size-bytes": "268435456"
            },
        )

        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "partition_by": "day(updated_at)",
            "identifier_field": id_field.name
        }

    except TableAlreadyExistsError:
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": str(e),
                "table": table_identifier
            }
        )