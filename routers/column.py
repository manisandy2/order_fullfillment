
from fastapi import APIRouter,Query,Body
from core.catalog_client import get_catalog_client
from fastapi.exceptions import HTTPException
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from pyiceberg.types import StringType
import pyarrow as pa
from pyiceberg.schema import Schema
from .table_utility import TABLE_LIST
from typing import Annotated
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["column"])

@router.get("/total-count")
def get_total_count(
    namespace: str = Query(..., example="order_fulfillment"),
    table_name: Annotated[
            str,
            Query(
                description="Select Iceberg table",
                enum=TABLE_LIST
            )
        ] = "masterorders",
):
    """
    Returns total row count of an Iceberg table using metadata (very fast).
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        snapshot = table.current_snapshot()
        if not snapshot:
            return {"status": "success", "total_rows": 0}

        total_rows = snapshot.summary.get("total-records")

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "total_rows": int(total_rows)
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch table row count: {e}"
        )


@router.get("/column")
def column_stats(
    namespace: str = Query(..., example="order_fulfillment"),
    table_name: Annotated[
            str,
            Query(
                description="Select Iceberg table",
                enum=TABLE_LIST
            )
        ] = "masterorders",
    column_name: str = Query(..., description="Column name to analyze (e.g. 'pri_id')")
):
    """
    Returns total row count, NaN/null count, non-null count, and unique value count
    for a specific column in an Iceberg table.
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Fast total row count using Iceberg metadata
        snapshot = table.current_snapshot()
        total_rows = int(snapshot.summary.get("total-records", 0)) if snapshot else 0

        # Load table into pandas for column-level stats
        scan = table.scan()
        df = scan.to_pandas()

        if column_name not in df.columns:
            raise HTTPException(
                status_code=404,
                detail=f"Column '{column_name}' not found in table '{table_name}'"
            )

        # Column statistics
        nan_count = df[column_name].isna().sum()
        non_null_count = total_rows - nan_count
        unique_count = df[column_name].nunique(dropna=True)

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "column": column_name,
            "total_rows": total_rows,
            "nan_count": int(nan_count),
            "non_null_count": int(non_null_count),
            "unique_value_count": int(unique_count)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate column stats: {e}")


@router.get("/range")
def get_data_range(
    namespace: str = Query(...),
    table_name: str = Query(...),
    start: int = Query(1, description="Start row number (1-based index)"),
    end: int = Query(100, description="End row number (inclusive)")
):
    """
    Returns a range of rows from an Iceberg table (1 to 100 or any range).
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Load entire table into Pandas
        scan = table.scan()
        df = scan.to_pandas()

        # Convert start/end to zero-based index
        start_index = max(start - 1, 0)
        end_index = end

        # Slice data
        df_slice = df.iloc[start_index:end_index]

        return {
            "status": "success",
            "namespace": namespace,
            "table": table_name,
            "start": start,
            "end": end,
            "count": len(df_slice),
            "rows": df_slice.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch data range: {e}"
        )


@router.post("/table-add-columns")
def add_columns(
    namespace: str = Query(..., description="Namespace (e.g. 'order_fulfillment')"),
    table_name: str = Query(..., description="Table name (e.g. 'orderlineitems')"),
):

    table_identifier = f"{namespace}.{table_name}"

    catalog = get_catalog_client()

    # -----------------------------
    # Load table
    # -----------------------------
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(
            status_code=404,
            detail=f"Table not found: {table_identifier}"
        )

    # -----------------------------
    # Schema Evolution
    # -----------------------------
    try:
        with table.update_schema() as schema_update:
            schema_update.add_column("erp_item_code",
                field_type=StringType(),
                doc="erp_item_code"
            )
            # schema_update.add_column(
            #     "inventory_status",
            #     field_type=StringType(),
            #     doc="inventory_status"
            # )

        return {
            "success": True,
            "status": "columns_added",
            "table": table_identifier,
            "columns_added": ["erp_item_code"],
            "schema_version": table.schema().schema_id,
            "timestamp": datetime.utcnow().isoformat()
        }

    except (ValueError, TypeError) as e:
        logger.error(f"Invalid column definition: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid column definition: {e}")
    except Exception as e:
        logger.exception(f"Schema update failed for {table_identifier}")
        raise HTTPException(
            status_code=500,
            detail={
            "error": "SCHEMA_UPDATE_FAILED",
            "table": table_identifier,
            "details": str(e)
        }
    )

# @router.delete("/delete-between-dates")
# def delete_between_dates(
#     namespace: str = Query(...),
#     table_name: str = Query(...),
#     date_column: str = Query(...),
#     start_date: str = Query(...),
#     end_date: str = Query(...),
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # Convert input to ISO-8601 format manually
#         start_dt = datetime.fromisoformat(start_date.replace(" ", "T"))
#         end_dt = datetime.fromisoformat(end_date.replace(" ", "T"))
#
#         if start_dt > end_dt:
#             raise HTTPException(400, "start_date must be <= end_date")
#
#         # Force ISO format with 'T'
#         start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
#         end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
#
#         delete_expression = (
#             f"{date_column} >= '{start_iso}' "
#             f"AND {date_column} <= '{end_iso}'"
#         )
#
#         print("Delete expression:", delete_expression)  # Debug
#
#         table.delete(delete_expression)
#
#         return {
#             "status": "success",
#             "deleted_between": f"{start_iso} → {end_iso}"
#         }
#
#     except Exception as e:
#         raise HTTPException(500, str(e))

@router.delete("/delete-between-dates")
def delete_between_dates(
    namespace: str = Query(...),
    table_name: str = Query(...),
    date_column: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Convert input to ISO format
        start_dt = datetime.fromisoformat(start_date.replace(" ", "T"))
        end_dt = datetime.fromisoformat(end_date.replace(" ", "T"))

        if start_dt > end_dt:
            raise HTTPException(400, "start_date must be <= end_date")

        start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

        delete_expression = (
            f"{date_column} >= '{start_iso}' AND {date_column} <= '{end_iso}'"
        )

        print("Delete expression:", delete_expression)

        # -------- Get count before delete --------
        filtered_table = table.scan().filter(delete_expression).to_arrow()
        delete_count = filtered_table.num_rows

        # -------- Perform delete --------
        if delete_count > 0:
            table.delete(delete_expression)

        return {
            "status": "success",
            "deleted_between": f"{start_iso} → {end_iso}",
            "deleted_count": delete_count
        }

    except Exception as e:
        raise HTTPException(500, str(e))