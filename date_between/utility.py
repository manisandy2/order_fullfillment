from core.catalog_client import *
from pyiceberg.catalog import NoSuchTableError
from date_between.insert_data import process_chunk
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import HTTPException
from datetime import datetime
import time
from typing import Dict, List, Tuple, Any,Optional
from datetime import date, timedelta
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)
from date_between.error_handler import handle_ingestion_error
import gc
import psutil

def load_table_identifier(namespace: str, table_name: str) -> Any:
    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        tbl = catalog.load_table(table_identifier)

        logger.info(
            "Iceberg table loaded successfully",
            extra={"table": table_identifier}
        )
        return tbl

    except NoSuchTableError:
        logger.error(
            "Iceberg table not found",
            extra={"table": table_identifier}
        )
        raise HTTPException(
            status_code=404,
            detail=f"Iceberg table '{table_identifier}' not found"
        )

    except Exception as e:
        logger.exception(
            "Iceberg table load failed",
            extra={"table": table_identifier}
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to load Iceberg table"
        )


def multi_executor(arrow_schema, chunks, arrow_tables, failed_chunks):
    # arrow_tables = []
    # failed_chunks = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_chunk, chunk, arrow_schema): idx
            for idx, chunk in enumerate(chunks)
        }

        for future in as_completed(futures):
            idx = futures[future]
            try:
                tbl = future.result()
                arrow_tables.append(tbl)
                del tbl
                gc.collect()

                logger.info(
                    f"Chunk {idx+1}/{len(chunks)} success | rows={tbl.num_rows}"
                )
            except Exception as e:
                logger.error(f"Chunk {idx+1} failed | {e}")
                failed_chunks.append({
                    "chunk_index": idx,
                    "chunk_data": chunks[idx],
                    "error": str(e),
                })


def validate_date_range(start_date: datetime, end_date: datetime) -> None:
    """
    Validate start and end date range.
    Raises HTTPException if invalid.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be less than or equal to end_date"
        )


def handle_failed_chunks(
    *,
    table_name: str,
    failed_chunks: List[Dict[str, Any]],
    error_type: str,
    use_error_table: bool = True,
):

    if not failed_chunks:
        return None


    # -------- Flatten failed records --------
    failed_records = []
    for chunk in failed_chunks:
        failed_records.extend(chunk.get("chunk_data", []))

    # -------- Save to error table --------
    result = handle_ingestion_error(
        table_name=table_name,
        failed_records=failed_records,
        error_type=error_type,
        error_message=f"Failed chunks: {[fc.get('chunk_index') for fc in failed_chunks]}",
        use_error_table=use_error_table,
    )

    logger.info(
        "Failed records saved to error table",
        extra={
            "table": table_name,
            "failed_records": len(failed_records),
        }
    )

    return result

def fetch_mysql_date_range(
    *,
    mysql_client,
    dbname: str,
    fetch_fn,
    start_date: datetime,
    end_date: datetime,
    # empty_status_code: int = 400,
) -> List[Any]:

    try:
        start_time = time.time()

        rows = fetch_fn(
            dbname,
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
        )

        elapsed = round(time.time() - start_time, 2)

        if not rows:
            logger.warning(
                "No rows found for given date range",
                extra={
                    "dbname": dbname,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            return [] # 🔥 RETURN EMPTY LIST INSTEAD OF ERROR

        logger.info(
            "MySQL fetch success",
            extra={
                "dbname": dbname,
                "rows": len(rows),
                "time": elapsed,
            },
        )

        return rows

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(
            status_code=500,
            detail=f"MySQL fetch error: {e}",
        )



def get_last_date_value(namespace,table,column):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table}"
        iceberg_table = catalog.load_table(table_identifier)

        # ---- FAST STRATEGY ----
        # Read only 1 row sorted DESC (no full scan)
        scan = (
            iceberg_table.scan(
                # row_filter=AlwaysTrue(),
                selected_fields=[column]
            )
            .to_arrow()
        )

        if scan.num_rows == 0:
            return {
                "namespace": namespace,
                "table": table,
                "column": column,
                "last_value": None
            }

        # Convert to pandas (small data only)
        df = scan.to_pandas()
        last_value = df[column].max()
        # ✅ ADD +1 SECOND (only if datetime)
        if isinstance(last_value, datetime):
            last_value = last_value + timedelta(seconds=1)
            last_value = last_value.isoformat()
        else:
            last_value = str(last_value)

        return {
            "namespace": namespace,
            "table": table,
            "column": column,
            "last_value": (
                last_value.isoformat()
                if isinstance(last_value, (datetime,))
                else str(last_value)
            )
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch last date value: {str(e)}")

import pyarrow.compute as pc

# def get_last_date_value(namespace, table, column):
#     try:
#         catalog = get_catalog_client()
#         table_identifier = f"{namespace}.{table}"
#         iceberg_table = catalog.load_table(table_identifier)
#
#         # Read ONLY required column
#         arrow_table = iceberg_table.scan(
#             selected_fields=[column]
#         ).to_arrow()
#
#         if arrow_table.num_rows == 0:
#             return {
#                 "namespace": namespace,
#                 "table": table,
#                 "column": column,
#                 "last_value": None
#             }
#
#         # Compute max directly in Arrow (NO pandas)
#         max_value = pc.max(arrow_table[column]).as_py()
#
#         # Cleanup early
#         del arrow_table
#         gc.collect()
#
#         if isinstance(max_value, datetime):
#             max_value = (max_value + timedelta(seconds=1)).isoformat()
#         else:
#             max_value = str(max_value)
#
#         return {
#             "namespace": namespace,
#             "table": table,
#             "column": column,
#             "last_value": max_value
#         }
#
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to fetch last date value: {str(e)}"
#         )

def yesterday():
    y = datetime.now() - timedelta(days=1)
    return y.replace(hour=23, minute=59, second=59, microsecond=0)

def clean_rows(
    rows: List[Dict[str, Any]],
    boolean_fields: Optional[List[str]] = None,
    timestamps_fields: Optional[List[str]] = None,
    date_fields: Optional[List[str]] = None,
    float_fields: Optional[List[str]] = None,


    field_overrides: Optional[Dict[str, tuple]] = None,
) -> List[Dict[str, Any]]:


    boolean_fields = set(boolean_fields or [])
    timestamps_fields = set(timestamps_fields or [])
    date_fields = set(date_fields or [])
    field_overrides = field_overrides or {}

    protected_fields = boolean_fields | timestamps_fields | date_fields

    dt_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
    ]

    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    for row in rows:

        # --------------------------------------------------
        # 1️⃣ Boolean fields
        # --------------------------------------------------
        for f in boolean_fields:
            val = row.get(f)

            if val is None:
                row[f] = False
            elif isinstance(val, bool):
                row[f] = val
            elif isinstance(val, (int, float)):
                row[f] = bool(val)
            elif isinstance(val, str):
                row[f] = val.strip().lower() in {"1", "true", "yes", "on"}
            else:
                row[f] = False

        # --------------------------------------------------
        # 2️⃣ Timestamp fields
        # --------------------------------------------------
        for f in timestamps_fields:
            val = row.get(f)

            if val in (None, ""):
                row[f] = datetime.now()
                continue

            if isinstance(val, datetime):
                continue

            parsed = None
            for fmt in dt_formats:
                try:
                    parsed = datetime.strptime(str(val), fmt)
                    break
                except Exception:
                    pass

            row[f] = parsed if parsed else datetime.now()

        # --------------------------------------------------
        # 3️⃣ Date fields
        # --------------------------------------------------
        for f in date_fields:
            val = row.get(f)

            if val in (None, ""):
                row[f] = date.today()
                continue

            if isinstance(val, date) and not isinstance(val, datetime):
                continue

            parsed = None
            for fmt in date_formats:
                try:
                    parsed = datetime.strptime(str(val), fmt).date()
                    break
                except Exception:
                    pass

            row[f] = parsed if parsed else date.today()

        # --------------------------------------------------
        # 4️⃣ Schema-driven normalization (CRITICAL)
        # --------------------------------------------------
        for key, val in row.items():
            if key in protected_fields:
                continue

            if key in field_overrides:
                ice_type, arrow_type, is_required = field_overrides[key]

                # NULL handling
                if val is None or val == "":
                    row[key] = None if not is_required else ""
                    continue

                # 🔥 FLOAT / DOUBLE (latitude, longitude, amounts, etc.)
                if pa.types.is_floating(arrow_type):
                    try:
                        row[key] = float(val)
                    except Exception:
                        row[key] = None
                    continue

                # 🔥 INTEGER / LONG
                if pa.types.is_integer(arrow_type):
                    if val is None or val == "":
                        row[key] = None
                        continue
                    try:
                        row[key] = int(val)
                    except Exception:
                        row[key] = None
                    continue

                # 🔥 STRING
                if pa.types.is_string(arrow_type):
                    row[key] = str(val)
                    continue

                # fallback
                row[key] = val

            else:
                # No override → SAFE DEFAULT (stringify)
                if isinstance(val, bool):
                    row[key] = "true" if val else "false"
                else:
                    row[key] = str(val) if val is not None else None
    return rows

# def schema(
#     record: Dict[str, Any],
#     # required_fields: Optional[List[str]] = None,
#     field_overrides: Optional[Dict[str, tuple]] = None
# ) -> Tuple[Schema, pa.Schema]:
#
#     # required_fields = set(required_fields or [])
#     field_overrides = field_overrides or {}
#
#     iceberg_fields = []
#     arrow_fields = []
#
#     for idx, (name, value) in enumerate(sorted(record.items()), start=1):
#
#         if name in field_overrides:
#             ice_type, arrow_type, required = field_overrides[name]
#         else:
#             required = name in required_fields
#
#             if isinstance(value, bool):
#                 ice_type, arrow_type = BooleanType(), pa.bool_()
#             elif isinstance(value, int):
#                 ice_type, arrow_type = LongType(), pa.int64()
#             elif isinstance(value, float):
#                 ice_type, arrow_type = DoubleType(), pa.float64()
#             elif isinstance(value, date) and not isinstance(value, datetime):
#                 ice_type, arrow_type = DateType(), pa.date32()
#             elif isinstance(value, datetime):
#                 ice_type, arrow_type = TimestampType(), pa.timestamp("ms")
#             else:
#                 ice_type, arrow_type = StringType(), pa.string()
#
#         iceberg_fields.append(
#             NestedField(idx, name, ice_type, required)
#         )
#         arrow_fields.append(
#             pa.field(name, arrow_type, nullable=not required)
#         )
#
#     return Schema(*iceberg_fields), pa.schema(arrow_fields)

def schema(record: Dict[str, Any], field_overrides: Dict[str, tuple]):

    iceberg_fields = []
    arrow_fields = []

    for idx, (name, value) in enumerate(sorted(record.items()), start=1):

        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()
            elif isinstance(value, int):
                ice_type, arrow_type = LongType(), pa.int64()
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()
            elif isinstance(value, date):
                ice_type, arrow_type = DateType(), pa.date32()
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp("ms")
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(idx, name, ice_type, required)
        )
        arrow_fields.append(
            pa.field(name, arrow_type, nullable=not required)
        )

    return Schema(*iceberg_fields), pa.schema(arrow_fields)

def get_memory_mb():
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / 1024 / 1024, 2)

def check_memory_limit(limit_mb=2000):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / 1024 / 1024

    if mem > limit_mb:
        raise MemoryError(f"Memory exceeded {limit_mb} MB")