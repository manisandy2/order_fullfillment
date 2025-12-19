from fastapi import FastAPI, APIRouter, Query, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, DateType,TimestampType
import logging
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError, ValidationError
# from sqlalchemy.sql.sqltypes import NullType

from core.catalog_client import get_catalog_client
from fastapi import APIRouter,Query,HTTPException

app = FastAPI()
router = APIRouter(prefix="", tags=["Schema"])
logger = logging.getLogger(__name__)


@router.get("/Schema/list")
def list_schema(
        namespace: str = Query(default="pos_transactions01",description="Namespace"),
        table_name: str = Query(default="transaction01",description="Table name"),

):
    # namespace, table_name = "pos_transactions01", "transaction_phone_in_con_sum"
    # namespace, table_name = "pos_transactions01", "transaction01"
    # namespace, table_name = "pos_transactions01", "transaction_with_in"
    # namespace, table_name = "pos_transactions01", "transaction_with_out"
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        return {"status": "success", "schema": table.schema().fields}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/Schema/create")
def create_table(
    namespace: str = Query(default="pos_transactions01",description="Namespace"),
    table_name: str = Query(default="transaction01",description="Table name"),
    columns: list = Query(..., description="List of columns as [{'name':'col1','type':'string','required':True}, ...]")
):
    try:
        catalog = get_catalog_client()
        fields = []
        for idx, col in enumerate(columns):
            type_map = {"string": StringType(), "long": LongType(), "date": DateType()}
            col_type = type_map.get(col["type"].lower(), StringType())
            fields.append(NestedField(idx + 1, col["name"], col_type, col.get("required", False)))

        schema = Schema(*fields)
        catalog.create_table(f"{namespace}.{table_name}", schema=schema)
        return {"status": "success", "message": f"Table '{table_name}' created successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @router.put("/schema/update")
# def update_schema(
#     namespace: str = Query(...),
#     table_name: str = Query(...),
#     column_name: str = Query(...),
#     new_type: str = Query(..., description="New type: string, long, date")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#         # if not old_field:
#         #     raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")
#         #
#         print("old:",old_field)
#         type_map = {"string": StringType(), "long": LongType(), "date": DateType()}
#         print("ty",type_map)
#         new_field = NestedField(old_field.field_id, old_field.name, DateType(), old_field.required)
#         print("fields:",new_field)
#         table.update_schema().update_column(f"{namespace}.{table_name}", new_field).commit()
#
#         return {"status": "success",
#                 "old_field": old_field,
#                 "table_name": table_name,
#                 # "message": f"Column '{column_name}' "
#                 #            f"updated to '{new_type}'."
#                 }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @router.put("/schema/update")
# def update_schema(
#     namespace: str = Query(...),
#     table_name: str = Query(...),
#     column_name: str = Query(...),
#     new_type: str = Query(..., description="New type: string, long, date")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # Find the column
#         old_field = next((f for f in table.schema().fields if f.name.lower() == column_name.lower()), None)
#         if not old_field:
#             raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")
#
#         # Map string type to Iceberg type
#         type_map = {"string": StringType(), "long": LongType(), "date": DateType()}
#         iceberg_type = type_map.get(new_type.lower())
#         print(iceberg_type)
#         # if iceberg_type:
#         if not iceberg_type:
#             raise HTTPException(status_code=400, detail=f"Unsupported type '{new_type}'")
#         print("id",old_field.field_id)
#         # Create new NestedField with same field_id but new type
#         new_field = NestedField(old_field.field_id, old_field.name, DateType(), old_field.required)
#         print(new_field)
#         # Correct update_column call — only pass the NestedField
#         table.update_schema(allow_incompatible_changes=True).update_column(new_field).commit()
#
#         # JSON-serializable response
#         old_field_info = {
#             "id": old_field.field_id,
#             "name": old_field.name,
#             "type": str(old_field.type),
#             "required": old_field.required
#         }
#
#         return {
#             "status": "success",
#             "table_name": table_name,
#             "column_name": column_name,
#             "old_field": old_field_info,
#             "message": f"Column '{column_name}' updated to '{new_type}'."
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


@router.put("/Schema/update")
def update_schema(
    namespace: str = Query(default="pos_transactions01",description="Namespace"),
    table_name: str = Query(default="transaction01",description="Table name"),
    column_name: str = Query(...),
    new_type: str = Query(..., description="New type: string, long, date,datetime")
):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(f"{namespace}.{table_name}")


        old_field = next((f for f in table.schema().fields if f.name.lower() == column_name.lower()), None)

        print("#"*100)
        if not old_field:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

        # Map string type to Iceberg type
        type_map = {
            "string": StringType(),
            "long": LongType(),
            "date": DateType(),
            "datetime": TimestampType()
        }
        iceberg_type = type_map.get(new_type.lower())
        # iceberg_type = type_map.get(new_type)
        if not iceberg_type:
            raise HTTPException(status_code=400, detail=f"Unsupported type '{new_type}'")

        try:
            (
                table.update_schema(allow_incompatible_changes=True)
                .update_column(column_name, iceberg_type)
                .commit()
            )
        except NoSuchTableError:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=f"Schema validation failed: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Schema update failed: {str(e)}")

            # Reload table to get new schema
        updated_table = catalog.load_table(table_identifier)
        new_field = next((f for f in updated_table.schema().fields if f.name.lower() == column_name.lower()), None)

        return {
            "status": "success",
            "column": column_name,
            "old_type": str(old_field.field_type),
            "new_type": str(new_field.field_type),
            "message": f"Column '{column_name}' successfully updated from '{old_field.field_type}' to '{new_field.field_type}'",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
import pyarrow as pa
import pandas as pd

@router.post("/schema/copy-column")
def copy_column_with_schema(
    namespace: str = Query(...),
    table_name: str = Query(...),
    source_column: str = Query(..., description="Existing column (STRING)"),
    target_column: str = Query(..., description="New column (LONG)"),
):
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"

    # -----------------------------
    # Load table
    # -----------------------------
    try:
        table = catalog.load_table(table_identifier)
    except Exception:
        raise HTTPException(status_code=404, detail="Table not found")

    schema = table.schema()

    src_field = next(
        (f for f in schema.fields if f.name.lower() == source_column.lower()),
        None
    )
    if not src_field:
        raise HTTPException(status_code=404, detail="Source column not found")

    if not isinstance(src_field.field_type, StringType):
        raise HTTPException(
            status_code=422,
            detail="Source column must be STRING"
        )

    # -----------------------------
    # Step 1: Add new column
    # -----------------------------
    if target_column not in [f.name for f in schema.fields]:
        with table.update_schema() as s:
            s.add_column(target_column, LongType())

    # -----------------------------
    # Step 2: Copy data (chunked)
    # -----------------------------
    rows_written = 0
    invalid_rows = 0

    scan = table.scan(
        selected_fields=[
            source_column,
            "line_item_id",
            "order_line_item_id",
            "master_order_id",
            "master_sale_order_id",
            "delivery_from",
            "customer_status",
            "inventory_status",
            "erp_item_code",
            "created_at",
        ])

    for batch in scan.to_arrow_batch_reader():
        df = batch.to_pandas()

        # Safe cast STRING → LONG
        # df[target_column] = pd.to_numeric(
        #     df[source_column],
        #     errors="coerce"
        # )
        df[target_column] = (
            pd.to_numeric(df[source_column], errors="coerce")
            .astype("Int64")  # nullable integer
        )

        invalid_rows += df[target_column].isna().sum()
        df = df.dropna(subset=[target_column])

        if not df.empty:
            arrow_table = pa.Table.from_pandas(df)
            table.append(arrow_table)
            rows_written += len(df)

    return {
        "status": "success",
        "table": table_identifier,
        "source_column": source_column,
        "target_column": target_column,
        "rows_written": rows_written,
        "invalid_rows_skipped": int(invalid_rows),
        "message": "Column copied safely with schema change",
    }

@router.delete("/Schema/delete")
def delete_table(namespace: str = Query(...), table_name: str = Query(...)):
    try:
        catalog = get_catalog_client()
        catalog.drop_table(f"{namespace}.{table_name}")
        return {"status": "success", "message": f"Table '{table_name}' deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
