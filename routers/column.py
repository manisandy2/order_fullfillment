
from fastapi import APIRouter,Query,Body
from core.catalog_client import get_catalog_client
from fastapi.exceptions import HTTPException

router = APIRouter(prefix="", tags=["column"])


@router.get("/total-count")
def get_total_count(
    namespace: str = Query(..., description="Namespace"),
    table_name: str = Query(..., description="Table name")
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
    namespace: str = Query(..., description="Namespace (e.g. 'sales')"),
    table_name: str = Query(..., description="Table name (e.g. 'transactions')"),
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