from fastapi import APIRouter, HTTPException
from core.mysql_client import MysqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from fastapi import APIRouter,HTTPException,Query,Body
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from .status_eventsUtility import *
from .insert_data import process_chunk
from core.catalog_client import get_catalog_client
from pyiceberg.catalog import NoSuchTableError
from core.logger import get_logger


logger = get_logger("Status-Events-w-api")

router = APIRouter(prefix="", tags=["Status Events"])
# Status Events
# multithreading
@router.post("/status-events/insert-multi-with-mysql")
def multi_within_mysql(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "status_events"
    dbname = "status_events"


    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"range=({start_range},{end_range}) chunk_size={chunk_size}"
    )
    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_creds = MysqlCatalog()


    try:
        start_time = time.time()
        rows = mysql_creds.get_status_events(dbname, start_range, end_range,"2025-12-12")

        print("mysql fetch time", time.time() - start_time)

        if not rows:
            logger.warning("No rows found for given range")
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        logger.info(f"MySQL fetch success | rows={len(rows)}")


    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")


    try:
        statusEvent_clean_rows(rows)
        logger.info("Row cleaning completed")
    except Exception as e:
        logger.exception("Row cleaning failed")
        raise HTTPException(status_code=500, detail=f"Row cleaning error: {e}")
    
    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    iceberg_schema, arrow_schema = statusEvent_schema(rows[0])
    
    # print("iceberg_schema",iceberg_schema)
    # print("arrow_schema",arrow_schema)

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    # print("chunks",chunks)
    arrow_tables = []
    failed_chunks = []  # Track failed chunks for error handling
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
                    logger.error(f"Chunk {idx + 1} failed: {e}")
                    failed_chunks.append({
                        "chunk_index": idx,
                        "chunk_data": chunks[idx],
                        "error": str(e)
                    })

    except Exception:
        logger.exception("Arrow conversion failed")
        raise HTTPException(status_code=500, detail=f"Arrow conversion error: {e}")

    arrow_end = time.time()
    logger.info(f"Arrow conversion completed in {arrow_end - arrow_start:.2f}s")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------
    # Handle failed chunks - save to error table
    error_save_result = None
    if failed_chunks:
        from .error_handler import handle_ingestion_error

        logger.warning(f"{len(failed_chunks)} chunks failed during Arrow conversion")

        # Flatten failed records from all failed chunks
        failed_records = []
        for failed_chunk in failed_chunks:
            failed_records.extend(failed_chunk["chunk_data"])

        # Save to error table
        error_save_result = handle_ingestion_error(
            table_name=table_name,
            failed_records=failed_records,
            error_type="ARROW_CONVERSION_FAILED",
            error_message=f"Failed chunks: {[fc['chunk_index'] for fc in failed_chunks]}",
            use_error_table=True
        )

        logger.info(f"Saved {len(failed_records)} failed records to error table")

    # If all chunks failed, raise error
    if not arrow_tables:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "All chunks failed during Arrow conversion",
                "failed_chunks": len(failed_chunks),
                "error_table_result": error_save_result
            }
        )

    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)
        logger.info("Iceberg table loaded successfully")
    except NoSuchTableError:
        logger.error("Iceberg table not found")
        raise HTTPException(status_code=404, detail=f"Table not found")
    except Exception as e:
        logger.exception("Iceberg table load failed")
        raise HTTPException(status_code=500, detail=str(e))

    append_start = time.time()
    failed_batches = []  # Track failed batch appends
    try:
        for i, batch in enumerate(arrow_tables, start=1):
            try:
                tbl.append(batch)  # commit each
                logger.info(
                    f"Iceberg append success | batch={i}/{len(arrow_tables)} rows={batch.num_rows}"
                )
            except Exception as batch_error:
                logger.error(f"Batch {i} append failed: {batch_error}")
                failed_batches.append({
                    "batch_index": i,
                    "batch_data": batch.to_pylist(),
                    "error": str(batch_error)
                })
                # Continue with other batches
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
    # Handle failed batch appends
    batch_error_result = None
    if failed_batches:
        from .error_handler import handle_ingestion_error

        logger.warning(f"{len(failed_batches)} batches failed during Iceberg append")

        # Flatten failed records from all failed batches
        failed_records = []
        for failed_batch in failed_batches:
            failed_records.extend(failed_batch["batch_data"])

        # Save to error table
        batch_error_result = handle_ingestion_error(
            table_name=table_name,
            failed_records=failed_records,
            error_type="ICEBERG_APPEND_FAILED",
            error_message=f"Failed batches: {[fb['batch_index'] for fb in failed_batches]}",
            use_error_table=True
        )

        logger.info(f"Saved {len(failed_records)} failed records from append errors to error table")
    append_end = time.time()
    total_end = time.time()

    successful_rows = len(rows) - len([r for fc in failed_chunks for r in fc.get("chunk_data", [])]) - len(
        [r for fb in failed_batches for r in fb.get("batch_data", [])])

    logger.info(
        f"END ingestion | total_rows={len(rows)} successful={successful_rows} "
        f"failed_chunks={len(failed_chunks)} failed_batches={len(failed_batches)} "
        f"total_time={total_end - total_start:.2f}s"
    )
    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    response = {
        "success": True,
        "message": "Data ingestion completed with error handling",
        "rows_fetched": len(rows),
        "rows_successful": successful_rows,
        "chunks": len(chunks),
        "chunks_successful": len(arrow_tables),
        "chunks_failed": len(failed_chunks),
        "batches_failed": len(failed_batches),
        "execution_times": {
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "append_refresh": round(append_end - append_start, 2),
            "total_time": round(total_end - total_start, 2),
        },
    }

    # Add error handling results if any failures occurred
    if error_save_result:
        response["arrow_conversion_errors"] = error_save_result
    if batch_error_result:
        response["append_errors"] = batch_error_result

    return response


@router.post("/status-events-date-range/insert-multi-with-mysql")
def multi_within_mysql_date_range(
        start_date: str = Query(..., description="Start datetime YYYY-MM-DD HH:MM:SS"),
        end_date: str = Query(..., description="End datetime YYYY-MM-DD HH:MM:SS"),
        chunk_size: int = Query(10000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "status_events"
    dbname = "status_events"

    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"date_range=({start_date},{end_date}) chunk_size={chunk_size}"
    )
    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_creds = MysqlCatalog()

    try:
        start_time = time.time()
        rows = mysql_creds.get_master_order_date_range(dbname, start_date, end_date)

        print("mysql fetch time", time.time() - start_time)

        if not rows:
            logger.warning("No rows found for given range")
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        logger.info(f"MySQL fetch success | rows={len(rows)}")


    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    try:
        statusEvent_clean_rows(rows)
        logger.info("Row cleaning completed")
    except Exception as e:
        logger.exception("Row cleaning failed")
        raise HTTPException(status_code=500, detail=f"Row cleaning error: {e}")

    iceberg_schema, arrow_schema = statusEvent_schema(rows[0])

    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    # print("chunks",chunks)
    arrow_tables = []
    failed_chunks = []  # Track failed chunks for error handling
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
                    logger.error(f"Chunk {idx + 1} failed: {e}")
                    failed_chunks.append({
                        "chunk_index": idx,
                        "chunk_data": chunks[idx],
                        "error": str(e)
                    })
                    raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")
    except Exception:
        logger.exception("Arrow conversion failed")
        raise HTTPException(status_code=500, detail=f"Arrow conversion error: {e}")

    arrow_end = time.time()
    logger.info(f"Arrow conversion completed in {arrow_end - arrow_start:.2f}s")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------
    # Handle failed chunks - save to error table
    error_save_result = None
    if failed_chunks:
        from .error_handler import handle_ingestion_error

        logger.warning(f"{len(failed_chunks)} chunks failed during Arrow conversion")

        # Flatten failed records from all failed chunks
        failed_records = []
        for failed_chunk in failed_chunks:
            failed_records.extend(failed_chunk["chunk_data"])

        # Save to error table
        error_save_result = handle_ingestion_error(
            table_name=table_name,
            failed_records=failed_records,
            error_type="ARROW_CONVERSION_FAILED",
            error_message=f"Failed chunks: {[fc['chunk_index'] for fc in failed_chunks]}",
            use_error_table=True
        )

        logger.info(f"Saved {len(failed_records)} failed records to error table")

    # If all chunks failed, raise error
    if not arrow_tables:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "All chunks failed during Arrow conversion",
                "failed_chunks": len(failed_chunks),
                "error_table_result": error_save_result
            }
        )

    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)
        logger.info("Iceberg table loaded successfully")
    except NoSuchTableError:
        logger.error("Iceberg table not found")
        raise HTTPException(status_code=404, detail=f"Table not found")
    except Exception as e:
        logger.exception("Iceberg table load failed")
        raise HTTPException(status_code=500, detail=str(e))

    append_start = time.time()
    failed_batches = []  # Track failed batch appends
    try:
        for i, batch in enumerate(arrow_tables, start=1):
            try:
                tbl.append(batch)  # commit each
                logger.info(
                    f"Iceberg append success | batch={i}/{len(arrow_tables)} rows={batch.num_rows}"
                )
            except Exception as batch_error:
                logger.error(f"Batch {i} append failed: {batch_error}")
                failed_batches.append({
                    "batch_index": i,
                    "batch_data": batch.to_pylist(),
                    "error": str(batch_error)
                })
                # Continue with other batches
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
    # Handle failed batch appends
    batch_error_result = None
    if failed_batches:
        from .error_handler import handle_ingestion_error

        logger.warning(f"{len(failed_batches)} batches failed during Iceberg append")

        # Flatten failed records from all failed batches
        failed_records = []
        for failed_batch in failed_batches:
            failed_records.extend(failed_batch["batch_data"])

        # Save to error table
        batch_error_result = handle_ingestion_error(
            table_name=table_name,
            failed_records=failed_records,
            error_type="ICEBERG_APPEND_FAILED",
            error_message=f"Failed batches: {[fb['batch_index'] for fb in failed_batches]}",
            use_error_table=True
        )

        logger.info(f"Saved {len(failed_records)} failed records from append errors to error table")
    append_end = time.time()
    total_end = time.time()

    successful_rows = len(rows) - len([r for fc in failed_chunks for r in fc.get("chunk_data", [])]) - len(
        [r for fb in failed_batches for r in fb.get("batch_data", [])])

    logger.info(
        f"END ingestion | total_rows={len(rows)} successful={successful_rows} "
        f"failed_chunks={len(failed_chunks)} failed_batches={len(failed_batches)} "
        f"total_time={total_end - total_start:.2f}s"
    )
    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    response = {
        "success": True,
        "message": "Data ingestion completed with error handling",
        "rows_fetched": len(rows),
        "rows_successful": successful_rows,
        "chunks": len(chunks),
        "chunks_successful": len(arrow_tables),
        "chunks_failed": len(failed_chunks),
        "batches_failed": len(failed_batches),
        "execution_times": {
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "append_refresh": round(append_end - append_start, 2),
            "total_time": round(total_end - total_start, 2),
        },
    }

    # Add error handling results if any failures occurred
    if error_save_result:
        response["arrow_conversion_errors"] = error_save_result
    if batch_error_result:
        response["append_errors"] = batch_error_result

    return response


# single core
# pickup-delivery-items
@router.post("/status-events/insert-single-within-mysql")
def insert(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "status_events"
    dbname = "status_events"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_status_events(dbname, start_range, end_range)
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
    statusEvent_clean_rows(rows)
    print("Cleaned Rows Sample:", rows[:2])

    # -------------------------------------------------
    # Step 3: Infer Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = statusEvent_schema(rows[0])

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
# @router.post("/status-events/insert-without-mysql")
# def insert(
#     rows: list = Body(..., description="List of pickup-delivery item rows"),
# ):
#     total_start = time.time()
#     namespace, table_name = "order_fulfillment", "status_events"
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
#     statusEvent_clean_rows(rows)
#     clean_end = time.time()
#     print(f"Row cleaning completed in {clean_end - clean_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 3: Infer Schema (Iceberg + Arrow)
#     # -------------------------------------------------
#     schema_start = time.time()
#     iceberg_schema, arrow_schema = statusEvent_schema(rows[0])
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
#######################################################################################

@router.post("/status-events/insert-without-mysql")
def insert(
    rows: dict = Body(..., description="Status event row as a dictionary"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "status_events"
    table_identifier = f"{namespace}.{table_name}"

    # -------------------------------------------------
    # Step 1: Validate Input (Must be dict)
    # -------------------------------------------------
    if not isinstance(rows, dict):
        raise HTTPException(status_code=400, detail="Input must be a dictionary")

    print("Received Row:", rows)

    # -------------------------------------------------
    # Step 2: Clean Row (cleaner expects list → wrap it)
    # -------------------------------------------------
    clean_start = time.time()
    cleaned = statusEvent_clean_rows([rows])
    if cleaned:
        rows = cleaned[0]
    clean_end = time.time()

    print(f"Row cleaning completed in {clean_end - clean_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Infer Schema (Pass dict)
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = statusEvent_schema(rows)
    schema_end = time.time()

    print("Inferred Iceberg Schema:", iceberg_schema)
    print("Inferred Arrow Schema:", arrow_schema)
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 4: Convert dict → Arrow Table
    # -------------------------------------------------
    arrow_start = time.time()
    try:
        # single dict → wrap into list
        arrow_table = pa.Table.from_pylist([rows], schema=arrow_schema)
        print("Arrow table created with 1 row")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")
    arrow_end = time.time()

    print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")

    # -------------------------------------------------
    # Step 5: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    catalog_end = time.time()

    print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")

    # -------------------------------------------------
    # Step 6: Append to Iceberg Table
    # -------------------------------------------------
    append_start = time.time()
    try:
        tbl.append(arrow_table)
    except Exception as e:
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
        "message": "1 row appended successfully",
        "execution_times": {
            "clean_rows": round(clean_end - clean_start, 2),
            "schema_infer": round(schema_end - schema_start, 2),
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append": round(append_end - append_start, 2),
            "total_time": round(total_end - total_start, 2),
        },
    }