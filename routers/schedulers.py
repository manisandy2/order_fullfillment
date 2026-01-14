from fastapi import APIRouter, Query, Body, HTTPException
import time

from r2_transfer.exchange_masterorders import url_prefix
# from .intransit_pickup_delivery_itemsUtility import *
from .schedulersUtility import *
from core.catalog_client import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.catalog import NoSuchTableError
from .insert_data import process_chunk
from core.between_range import MydatabaseRange
from fastapi import status
from core.logger import get_logger

url_prefix = "schedulers"

logger = get_logger("schedulers")

router = APIRouter(prefix=f"/{url_prefix}", tags=["roles"])


# mysql | range | chunk_size | multithreading | arrow | append
@router.post("/ingest/mysql-range")
def insert_schedulers_between_range(
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

    try:
        start_time = time.time()
        # rows = mysql_creds.get_master_order(dbname, start_range, end_range,"2025-12-12")
        rows = mysql_creds.get_schedulers(dbname, start_range, end_range)
        print(f"{len(rows)} rows fetched from get_schedulers")
        logger.debug(f"MySQL fetch completed in {time.time() - start_time:.2f}s")
        if not rows:
            logger.warning("No rows found for given range")
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        logger.info(f"MySQL fetch success | rows={len(rows)}")

    except Exception as e:
        logger.exception("MySQL fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    try:
        # print(rows[0])

        clean_rows(rows)
        # external_call_logs_clean_rows(rows)

        logger.info("Row cleaning completed")
    except Exception as e:
        logger.exception("Row cleaning failed")
        raise HTTPException(status_code=500, detail=f"Row cleaning error: {e}")

    # iceberg_schema, arrow_schema = exchange_informations_schema(rows[0])
    iceberg_schema, arrow_schema = schema(rows[0])
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