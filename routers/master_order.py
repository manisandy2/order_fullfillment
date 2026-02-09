from fastapi import APIRouter,Query,Body,HTTPException
import time
from core.mysql_client import MysqlCatalog
from .masterOrderUtility import *
from core.catalog_client import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from .insert_data import process_chunk
from fastapi import status
from core.logger import get_logger
from core.between_range import MydatabaseRange

url_prefix = "masterorders"

logger = get_logger("masterorders")

router = APIRouter(prefix="", tags=["MasterOrder"])


# mysql | range | chunk_size | multithreading | arrow | append
@router.post("/masterorders/ingest/mysql-range")
def masterorder_between_range(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10000, description="Chunk size for multithreading"),

):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", f"{url_prefix}"
    dbname = f"{url_prefix}"


    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"range=({start_range},{end_range}) chunk_size={chunk_size}"
    )

    mysql_creds = MydatabaseRange()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    try:
        start_time = time.time()
        # rows = mysql_creds.get_master_order(dbname, start_range, end_range,"2025-12-12")
        rows = mysql_creds.get_master_orders(dbname, start_range, end_range)
        print(f"{len(rows)} rows fetched from get_master_order")
        logger.debug(f"MySQL fetch completed in {time.time() - start_time:.2f}s")
        if not rows:
            logger.warning("No rows found for given range")
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        logger.info(f"MySQL fetch success | rows={len(rows)}")

    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    try:
        masterOrder_clean_rows(rows)
        logger.info("Row cleaning completed")
    except Exception as e:
        logger.exception("Row cleaning failed")
        raise HTTPException(status_code=500, detail=f"Row cleaning error: {e}")


    iceberg_schema, arrow_schema = masterorder_schema(rows[0])
    

    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    arrow_tables = []
    failed_chunks = []
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
                    # raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")
    except Exception as e:
        logger.exception("Arrow conversion failed")
        raise HTTPException(status_code=500, detail=f"Arrow conversion error: {e}")

    arrow_end = time.time()
    logger.info(f"Arrow conversion completed in {arrow_end - arrow_start:.2f}s")

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
        logger.info(f"Iceberg table loaded successfully")
    except NoSuchTableError:
        logger.error("Iceberg table not found")
        raise HTTPException(status_code=404, detail=f"Table not found")
    except Exception as e:
        logger.exception("Iceberg table load failed")
        raise HTTPException(status_code=500, detail=str(e))

    append_start = time.time()
    failed_batches = []

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

    failed_chunks_count = sum(len(fc.get("chunk_data", [])) for fc in failed_chunks)
    failed_batches_count = sum(len(fb.get("batch_data", [])) for fb in failed_batches)
    successful_rows = len(rows) - failed_chunks_count - failed_batches_count

    # successful_rows = len(rows) - len([r for fc in failed_chunks for r in fc.get("chunk_data", [])]) - len(
    #     [r for fb in failed_batches for r in fb.get("batch_data", [])])

    logger.info(
        f"END ingestion | total_rows={len(rows)} successful={successful_rows} "
        f"failed_chunks={len(failed_chunks)} failed_batches={len(failed_batches)} "
        f"total_time={total_end - total_start:.2f}s"
    )
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

# mysql | date_range | chunk_size | iceberg | arrow | append
@router.post("/masterorder-date-range/insert-master-with-mysql")
def masterorder_between_date(
        # start_date: datetime = Query(..., description="Start datetime YYYY-MM-DD HH:MM:SS"),
        # end_date: datetime = Query(..., description="End datetime YYYY-MM-DD HH:MM:SS"),
        chunk_size: int = Query(10000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "masterorders"
    dbname = "masterorders"


    start_date = datetime.strptime("2026-01-29 20:34:50", "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime("2026-02-02 23:59:59", "%Y-%m-%d %H:%M:%S")

    print("start:",start_date)
    print("end:",end_date)
    # exit()

    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be less than or equal to end_date"
        )

    logger.info(
        f"START ingestion | table={namespace}.{table_name} "
        f"date_range=({start_date},{end_date}) chunk_size={chunk_size}"
    )

    mysql_creds = MysqlCatalog()

    try:
        start_time = time.time()
        start_dt = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_dt = end_date.strftime("%Y-%m-%d %H:%M:%S")
        # rows = mysql_creds.get_master_order(dbname, start_range, end_range,"2025-12-12")
        rows = mysql_creds.get_master_order_date_range(dbname, start_dt, end_dt)
        print("rows:",len(rows))
        print("mysql fetch time", time.time() - start_time)

        if not rows:
            logger.warning("No rows found for given range")
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        logger.info(f"MySQL fetch success | rows={len(rows)}")

    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

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

    # print("iceberg_schema",iceberg_schema)
    # print("arrow_schema",arrow_schema)

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    # print("chunks",chunks)
    arrow_tables = []
    failed_chunks = []
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
    except Exception as e:
        logger.exception("Arrow conversion failed")
        raise HTTPException(status_code=500, detail=f"Arrow conversion error: {e}")

    arrow_end = time.time()
    logger.info(f"Arrow conversion completed in {arrow_end - arrow_start:.2f}s")

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

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------

    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        tbl = catalog.load_table(table_identifier)
        logger.info(f"Iceberg table loaded successfully")
    except NoSuchTableError:
        logger.error("Iceberg table not found")
        raise HTTPException(status_code=404, detail=f"Table not found")
    except Exception as e:
        logger.exception("Iceberg table load failed")
        raise HTTPException(status_code=500, detail=str(e))

    append_start = time.time()
    failed_batches = []

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
    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    successful_rows = len(rows) - len([r for fc in failed_chunks for r in fc.get("chunk_data", [])]) - len(
        [r for fb in failed_batches for r in fb.get("batch_data", [])])

    logger.info(
        f"END ingestion | total_rows={len(rows)} successful={successful_rows} "
        f"failed_chunks={len(failed_chunks)} failed_batches={len(failed_batches)} "
        f"total_time={total_end - total_start:.2f}s"
    )
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

# mysql | single_data | iceberg | arrow | append
@router.post("/masterorder/insert-single-within-mysql")
def masterorder_single(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "masterorders"
    dbname = "masterorders"
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

# without mysql | single data | iceberg | arrow | append
@router.post("/masterorder/insert-without-mysql")
def masterorder_single_without_mysql(
    row: dict = Body(..., description="Single pickup-delivery item row"),
):

    namespace, table_name = "order_fulfillment", "masterorders"
    table_identifier = f"{namespace}.{table_name}"

    # -------------------------------------------------
    # Step 1: Validate Input
    # -------------------------------------------------
    if not isinstance(row, dict):
        raise HTTPException(status_code=400, detail="Input must be a dictionary")

    print("Received Row:", row)

    # -------------------------------------------------
    # Step 2: Clean Row
    # -------------------------------------------------


    try:
        cleaned = masterOrder_clean_rows([row])     # pass as list internally
        if cleaned:
            row = cleaned[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Row cleaning failed: {e}")



    # -------------------------------------------------
    # Step 3: Infer Schema (Iceberg + Arrow)
    # -------------------------------------------------


    try:
        iceberg_schema, arrow_schema = masterorder_schema(row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema inference failed: {e}")



    # -------------------------------------------------
    # Step 4: Convert to Arrow Table (single row)
    # -------------------------------------------------


    try:
        arrow_table = pa.Table.from_pylist([row], schema=arrow_schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")



    # -------------------------------------------------
    # Step 5: Load Iceberg Table
    # -------------------------------------------------

    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Catalog load failed: {e}")



    # -------------------------------------------------
    # Step 6: Append to Iceberg Table
    # -------------------------------------------------


    try:
        tbl.append(arrow_table)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ICEBERG_APPEND_FAILED",
                "message": f"Append failed for table {table_identifier}",
                "exception": str(e),
            },
        )



    return {
        "status_code":201,
        "success": True,
        "message": "successfully",
    }