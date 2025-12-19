from fastapi import APIRouter,Query,Body
import time
import uuid
from core.mysql_client import MysqlCatalog
from .masterOrderUtility import *
from core.catalog_client import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from .insert_data import process_chunk
from fastapi import status
from core.logger import get_logger


logger = get_logger("masterorder-w-api")

router = APIRouter(prefix="", tags=["MasterOrder_w"])

# insert-master-order-data
# multithreading

@router.post("/masterorder-w/insert-master-order-data")
def insert(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(1000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "masterorders_w"
    dbname = "masterorders_w"

    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"range=({start_range},{end_range}) chunk_size={chunk_size}"
    )

    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------

    try:
        rows = mysql_creds.get_master_order_w(dbname, start_range, end_range)

        if not rows:
            logger.warning("No rows found for given range")
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        logger.info(f"MySQL fetch success | rows={len(rows)}")

    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # oms_rows = [row for row in rows if row.get("oms_data_migration_status") == 1]

    # if not oms_rows:
    #     raise HTTPException(status_code=400, detail="oms_data_migration_status not found")

    try:
        masterOrder_clean_rows(rows)
        logger.info("Row cleaning completed")
    except Exception as e:
        logger.exception("Row cleaning failed")
        raise HTTPException(status_code=500, detail=f"Row cleaning error: {e}")

    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    iceberg_schema, arrow_schema = masterorder_schema(rows[0])


    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]


    arrow_tables = []
    logger.info(f"Arrow conversion started | chunks={len(chunks)}")
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    tbl = future.result()
                    arrow_tables.append(tbl)
                    logger.info(f"Chunk {idx + 1}/{len(chunks)} processed with {tbl.num_rows} rows")
                except Exception as e:
                    logger.exception("Arrow chunk conversion failed")
                    raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")
    except Exception:
        logger.exception("Arrow conversion failed")
        raise HTTPException(status_code=500, detail=f"Arrow conversion error: {e}")

    arrow_end = time.time()
    logger.info(f"Arrow conversion completed in {arrow_end - arrow_start:.2f}s")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------

    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)
        logger.info("Iceberg table loaded successfully")

    except NoSuchTableError:
        logger.error("Iceberg table not found")
        raise HTTPException(status_code=404, detail=f"Iceberg table not found")
    except Exception as e:
        logger.exception("Iceberg table load failed")
        raise HTTPException(status_code=500, detail=str(e))

    append_start = time.time()
    try:
        for i, batch in enumerate(arrow_tables, start=1):
            tbl.append(batch)  # commit each
            logger.info(
                f"Iceberg append success | batch={i}/{len(arrow_tables)} rows={batch.num_rows}"
            )

    except Exception as e:
        logger.exception("Iceberg append failed")
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ICEBERG_APPEND_FAILED",
                "message": f"Data append failed for table {table_identifier}",
                "exception": str(e),
            },
        )
    append_end = time.time()
    total_end = time.time()
    logger.info(
        f"END ingestion | rows={len(rows)} total_time={total_end - total_start:.2f}s"
    )
    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    return {
        "success": True,
        "message": "Data appended successfully with multithreading",
        "rows_fetched": len(rows),
        "chunks": len(chunks),
        "execution_times": {
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "append": round(append_end - append_start, 2),
            "total_time": round(total_end - total_start, 2),
        },
    }

@router.post("/masterorder-w/insert-single-within-mysql")
def insert_pickup_delivery_items(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "masterorders_w"
    dbname = "masterorders_w"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_pickup_delivery_items(dbname, start_range, end_range)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        print("Sample Row:", rows[0])

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    mysql_end = time.time()
    print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    # -------------------------------------------------
    # Step 2: Clean Rows
    # -------------------------------------------------
    masterOrder_clean_rows(rows)
    print("Cleaned Rows Sample:", rows[:2])

    # -------------------------------------------------
    # Step 3: Infer Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = masterorder_schema(rows[0])

    print("Inferred Iceberg Schema:", iceberg_schema)
    print("Inferred Arrow Schema:", arrow_schema)

    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 4: Convert Entire Dataset to Arrow Table (NO MULTITHREADING)
    # -------------------------------------------------
    arrow_start = time.time()
    try:
        arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
        print(f"Arrow table created with {arrow_table.num_rows} rows")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")

    arrow_end = time.time()
    print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")

    # -------------------------------------------------
    # Step 5: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    catalog_end = time.time()
    print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")

    # -------------------------------------------------
    # Step 6: Append to Iceberg Table (Single Commit)
    # -------------------------------------------------
    append_start = time.time()
    try:
        print(f"Appending full table ({arrow_table.num_rows} rows)")
        tbl.append(arrow_table)

    except Exception as e:
        print(f"Appending full table ({arrow_table.num_rows} rows)")
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ICEBERG_APPEND_FAILED",
                "message": f"Data append failed for table {table_identifier}",
                "exception": str(e),
            },
        )

    append_end = time.time()
    print(f"Append completed in {append_end - append_start:.2f} sec")

    total_end = time.time()

    # -------------------------------------------------
    # Step 7: Final API Response
    # -------------------------------------------------
    return {
        "success": True,
        "message": "Data appended successfully",
        "rows_fetched": len(rows),
        "execution_times": {
            "mysql_fetch": round(mysql_end - mysql_start, 2),
            "schema_infer": round(schema_end - schema_start, 2),
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append": round(append_end - append_start, 2),
            "total_time": round(total_end - total_start, 2),
        },
    }

# with out mysql
# @router.post("/masterorder/insert-without-mysql")
# def insert_without_mysql(
#     rows: list = Body(..., description="List of pickup-delivery item rows"),
#     # rows: dict = Body(..., description="List of pickup-delivery item rows"),
# ):
#     total_start = time.time()
#     namespace, table_name = "order_fulfillment", "masterorders"
#
#     # -------------------------------------------------
#     # Step 1: Validate Input
#     # -------------------------------------------------
#     if not isinstance(rows, list) or len(rows) == 0:
#         raise HTTPException(status_code=400, detail="Rows must be a non-empty list")
#
#     print(f"Received {len(rows)} rows")
#
#     # Print sample
#     print("Sample Row:", rows[0])
#
#     # -------------------------------------------------
#     # Step 2: Clean Rows
#     # -------------------------------------------------
#     clean_start = time.time()
#     masterOrder_clean_rows(rows)
#     clean_end = time.time()
#     print(f"Row cleaning completed in {clean_end - clean_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 3: Infer Schema (Iceberg + Arrow)
#     # -------------------------------------------------
#     schema_start = time.time()
#     iceberg_schema, arrow_schema = masterorder_schema(rows[0])
#     schema_end = time.time()
#
#     print("Inferred Iceberg Schema:", iceberg_schema)
#     print("Inferred Arrow Schema:", arrow_schema)
#
#     print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 4: Convert Full Rows to Arrow Table
#     # -------------------------------------------------
#     arrow_start = time.time()
#     try:
#         arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
#         print(f"Arrow table created with {arrow_table.num_rows} rows")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")
#     arrow_end = time.time()
#
#     print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 5: Load Iceberg Table
#     # -------------------------------------------------
#     catalog_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#
#     catalog_end = time.time()
#     print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 6: Append to Iceberg Table
#     # -------------------------------------------------
#     append_start = time.time()
#     try:
#         tbl.append(arrow_table)
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "error_code": "ICEBERG_APPEND_FAILED",
#                 "message": f"Data append failed for table {table_identifier}",
#                 "exception": str(e),
#             },
#         )
#     append_end = time.time()
#
#     print(f"Append completed in {append_end - append_start:.2f} sec")
#
#     total_end = time.time()
#
#     # -------------------------------------------------
#     # Step 7: Final API Response
#     # -------------------------------------------------
#     return {
#         "success": True,
#         "message": "Data appended successfully",
#         "rows_received": len(rows),
#         "execution_times": {
#             "clean_rows": round(clean_end - clean_start, 2),
#             "schema_infer": round(schema_end - schema_start, 2),
#             "arrow_convert": round(arrow_end - arrow_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "append": round(append_end - append_start, 2),
#             "total_time": round(total_end - total_start, 2),
#         },
#     }

# @router.post("/masterorder/insert-without-mysql")
# def insert_without_mysql(
#     row: dict = Body(..., description="Single pickup-delivery item row"),
# ):
#     total_start = time.time()
#     namespace, table_name = "order_fulfillment", "masterorders"
#     table_identifier = f"{namespace}.{table_name}"
#
#     # -------------------------------------------------
#     # Step 1: Validate Input
#     # -------------------------------------------------
#     if not isinstance(row, dict):
#         raise HTTPException(status_code=400, detail="Input must be a dictionary")
#
#     print("Received Row:", row)
#
#     # -------------------------------------------------
#     # Step 2: Clean Row
#     # -------------------------------------------------
#     clean_start = time.time()
#
#     try:
#         cleaned = masterOrder_clean_rows([row])     # pass as list internally
#         if cleaned:
#             row = cleaned[0]
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Row cleaning failed: {e}")
#
#     clean_end = time.time()
#
#     # -------------------------------------------------
#     # Step 3: Infer Schema (Iceberg + Arrow)
#     # -------------------------------------------------
#     schema_start = time.time()
#
#     try:
#         iceberg_schema, arrow_schema = masterorder_schema(row)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Schema inference failed: {e}")
#
#     schema_end = time.time()
#
#     # -------------------------------------------------
#     # Step 4: Convert to Arrow Table (single row)
#     # -------------------------------------------------
#     arrow_start = time.time()
#
#     try:
#         arrow_table = pa.Table.from_pylist([row], schema=arrow_schema)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")
#
#     arrow_end = time.time()
#
#     # -------------------------------------------------
#     # Step 5: Load Iceberg Table
#     # -------------------------------------------------
#     catalog_start = time.time()
#     catalog = get_catalog_client()
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Catalog load failed: {e}")
#
#     catalog_end = time.time()
#
#     # -------------------------------------------------
#     # Step 6: Append to Iceberg Table
#     # -------------------------------------------------
#     append_start = time.time()
#
#     try:
#         tbl.append(arrow_table)
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "error_code": "ICEBERG_APPEND_FAILED",
#                 "message": f"Append failed for table {table_identifier}",
#                 "exception": str(e),
#             },
#         )
#
#     append_end = time.time()
#
#     total_end = time.time()
#
#     return {
#         "success": True,
#         "message": "1 row appended successfully",
#         "execution_times": {
#             "clean_rows": round(clean_end - clean_start, 2),
#             "schema_infer": round(schema_end - schema_start, 2),
#             "arrow_convert": round(arrow_end - arrow_start, 2),
#             "catalog_load": round(catalog_end - catalog_start, 2),
#             "append": round(append_end - append_start, 2),
#             "total_time": round(total_end - total_start, 2),
#         },
#     }

# @router.post("/masterorder-w/insert-without-mysql")
# def insert_without_mysql(
#     row: dict = Body(..., description="Single pickup-delivery item row"),
# ):
#
#     namespace, table_name = "order_fulfillment", "masterorders_w"
#     table_identifier = f"{namespace}.{table_name}"
#
#     # -------------------------------------------------
#     # Step 1: Validate Input
#     # -------------------------------------------------
#     if not isinstance(row, dict):
#         raise HTTPException(status_code=400, detail="Input must be a dictionary")
#
#     print("Received Row:", row)
#
#     # -------------------------------------------------
#     # Step 2: Clean Row
#     # -------------------------------------------------
#
#
#     try:
#         cleaned = masterOrder_clean_rows([row])     # pass as list internally
#         if cleaned:
#             row = cleaned[0]
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Row cleaning failed: {e}")
#
#
#
#     # -------------------------------------------------
#     # Step 3: Infer Schema (Iceberg + Arrow)
#     # -------------------------------------------------
#
#
#     try:
#         iceberg_schema, arrow_schema = masterorder_schema(row)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Schema inference failed: {e}")
#
#
#
#     # -------------------------------------------------
#     # Step 4: Convert to Arrow Table (single row)
#     # -------------------------------------------------
#
#
#     try:
#         arrow_table = pa.Table.from_pylist([row], schema=arrow_schema)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")
#
#
#
#     # -------------------------------------------------
#     # Step 5: Load Iceberg Table
#     # -------------------------------------------------
#
#     catalog = get_catalog_client()
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Catalog load failed: {e}")
#
#
#
#     # -------------------------------------------------
#     # Step 6: Append to Iceberg Table
#     # -------------------------------------------------
#
#
#     try:
#         tbl.append(arrow_table)
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "error_code": "ICEBERG_APPEND_FAILED",
#                 "message": f"Append failed for table {table_identifier}",
#                 "exception": str(e),
#             },
#         )
#
#
#
#     return {
#         "status_code":201,
#         "success": True,
#         "message": "successfully",
#     }

from routers.queue import order_queue

@router.post("/masterorder-w/insert-without-mysql")
async def insert_without_mysql(
    row: dict = Body(...),
):
    # Validate input
    if not isinstance(row, dict) or not row:
        logger.error("Invalid input: expected non-empty dictionary")
        raise HTTPException(
            status_code=400, 
            detail="Input must be a non-empty dictionary"
        )

     # Generate tracking ID
    request_id = str(uuid.uuid4())
    row["_request_id"] = request_id

    try:
        # Check queue capacity
        if order_queue.full():
            logger.warning(f"Queue full, rejecting request {request_id}")
            raise HTTPException(
                status_code=503,
                detail="Queue is full, please retry later"
            )
        await order_queue.put(row)
        logger.info(f"Request {request_id} queued successfully")
        return {
            "status_code": 202,
            "success": True,
            "message": "queued"
        }
    except Exception as e:
        logger.error(f"Failed to queue request {request_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to queue request: {str(e)}"
        )