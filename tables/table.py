from core.catalog_client import get_catalog_client
from pyiceberg.partitioning import PartitionSpec,PartitionField
from pyiceberg.schema import Schema
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from core.logger import get_logger
from fastapi import HTTPException

logger = get_logger("create-table")

def _create_iceberg_table(
        namespace: str,
        table_name: str,
        schema: Schema,
        partition_spec: PartitionSpec,
        sort_order: str,
        identifier_field_ids: str = "1"
):
    """
    Helper function to create an Iceberg table with standard configuration.
    """
    table_identifier = f"{namespace}.{table_name}"
    logger.info(f"Creating table: {table_identifier}")

    # Connect to catalog
    catalog = get_catalog_client()

    # Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Create table
    try:
        properties = {
            "format-version": "2",
            "table-type": "MERGE_ON_READ",
            "identifier-field-ids": identifier_field_ids,
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
            "write.partition.path-style": "hierarchical",
            "write.sort.order": sort_order,
            "write.target-file-size-bytes": "268435456"
        }

        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties,
        )
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Determine partition field name for the response
        partition_desc = "unknown"
        if partition_spec.fields:
            # Assuming single partition field for now as per existing code
            field_id = partition_spec.fields[0].source_id
            field_name = schema.find_field(field_id).name
            partition_desc = f"year({field_name})"

        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(schema.fields),
            "partition_by": partition_desc
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

def _process_chunk(chunk, arrow_schema):
    import pyarrow as pa
    from datetime import datetime, date
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}
        # print(f" Processing row {row_idx} -> keys: {list(row.keys())}")

        for field in arrow_schema:
            val = row.get(field.name, None)

            # Debug mismatched field
            if field.name not in row:
                print(f"Field '{field.name}' missing in row; available keys: {list(row.keys())}")

            try:
                # --- Handle empty or None values ---
                if val in ("", " ", None):
                    converted_row[field.name] = None
                    continue

                # --- Integer fields ---
                if pa.types.is_integer(field.type):
                    converted_row[field.name] = int(val)

                # --- Float fields ---
                elif pa.types.is_floating(field.type):
                    converted_row[field.name] = float(val)

                # --- Timestamp or date fields ---
                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    parsed_date = None

                    if isinstance(val, (datetime, date)):
                        parsed_date = val
                    elif isinstance(val, str):
                        val = val.strip()
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(val, fmt)
                                break
                            except ValueError:
                                continue

                    if parsed_date:
                        converted_row[field.name] = (
                            parsed_date if isinstance(parsed_date, datetime)
                            else datetime.combine(parsed_date, datetime.min.time())
                        )
                    else:
                        print(f" Row {row_idx}: Unrecognized date in '{field.name}': {val}")
                        converted_row[field.name] = None

                # --- Default: keep as string or object ---
                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(f" Row {row_idx}, Field '{field.name}', Value: {val}, Error: {e}")
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)