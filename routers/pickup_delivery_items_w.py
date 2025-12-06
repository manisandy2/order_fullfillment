from fastapi import APIRouter, HTTPException
from core.mysql_client import MysqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from fastapi import APIRouter,HTTPException,Query,Body
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from .pickup_delivery_items_Utility import *  
from .insert_data import process_chunk
from core.catalog_client import get_catalog_client
from pyiceberg.catalog import NoSuchTableError


router = APIRouter(prefix="", tags=["Pick up Delivery Items W"])
# pickup_delivery_items
# multithreading
@router.post("/pickup-delivery-items-w/insert-multi-with-mysql")
def multi_within_mysql(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(1000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "pickup_delivery_items_w"
    dbname = "pickup_delivery_items_w"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_pickup_delivery_items(dbname, start_range, end_range)
        

        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")
        
        print("Sample Row:", rows[0])

        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
    
    
    pickup_delivery_items_clean_rows(rows)
    
    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = pickup_delivery_items_schema(rows[0])
    
    # print("iceberg_schema",iceberg_schema)
    # print("arrow_schema",arrow_schema)

    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    # print("chunks",chunks)
    arrow_tables = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}
        
        for future in as_completed(futures):
            idx = futures[future]
            try:
                tbl = future.result()
                arrow_tables.append(tbl)
                print(f"Chunk {idx + 1}/{len(chunks)} processed with {tbl.num_rows} rows")
            except Exception as e:
                print(f"Chunk {idx + 1} failed: {e}")
                raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")


    arrow_end = time.time()
    print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    # print(f"catalog table_identifier: {table_identifier}")
    try:
        tbl = catalog.load_table(table_identifier)
        catalog_end = time.time()
        print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")


    append_start = time.time()
    try:

        for i, batch in enumerate(arrow_tables, start=1):
            print(f"Appending batch {i}/{len(arrow_tables)} rows={batch.num_rows}")
            tbl.append(batch)  # commit each
            print("success post")

        append_end = time.time()

    except Exception as e:    
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ICEBERG_APPEND_FAILED",
                "message": f"Data append failed for table {table_identifier}",
                "exception": str(e),
            },
        )

    print(f" Append completed in {append_end - append_start:.2f} sec")
    total_end = time.time()
    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    return {
        "success": True,
        "message": "Data appended successfully with multithreading",
        "rows_fetched": len(rows),
        "chunks": len(chunks),
        "execution_times": {
            "mysql_fetch": round(mysql_end - mysql_start, 2),
            "schema_infer": round(schema_end - schema_start, 2),
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append_refresh": round(append_end - append_start, 2),
            "total_time": round(total_end - total_start, 2),
        },
    }


# single core
# pickup-delivery-items
@router.post("/pickup-delivery-items-w/insert-single-within-mysql")
def insert_pickup_delivery_items(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "pickup_delivery_items_w"
    dbname = "pickup_delivery_items_w"
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
    pickup_delivery_items_clean_rows(rows)
    print("Cleaned Rows Sample:", rows[:2])

    # -------------------------------------------------
    # Step 3: Infer Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = pickup_delivery_items_schema(rows[0])

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
# @router.post("/insert-without-mysql")
# def insert_without_mysql(
#     rows: list = Body(..., description="List of pickup-delivery item rows"),
# ):
#     total_start = time.time()
#     namespace, table_name = "order_fulfillment", "pickup_delivery_items"
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
#     pickup_delivery_items_clean_rows(rows)
#     clean_end = time.time()
#     print(f"Row cleaning completed in {clean_end - clean_start:.2f} sec")
#
#     # -------------------------------------------------
#     # Step 3: Infer Schema (Iceberg + Arrow)
#     # -------------------------------------------------
#     schema_start = time.time()
#     iceberg_schema, arrow_schema = pickup_delivery_items_schema(rows[0])
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

@router.post("/pickup-delivery-items-w/insert-without-mysql")
def insert_without_mysql(
    rows: dict = Body(..., description="Pickup-delivery item row as a dictionary"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "pickup_delivery_items_w"
    table_identifier = f"{namespace}.{table_name}"

    # -------------------------------------------------
    # Step 1: Validate Input
    # -------------------------------------------------
    if not isinstance(rows, dict):
        raise HTTPException(status_code=400, detail="Input must be a dictionary")

    print("Received Row:", rows)

    # -------------------------------------------------
    # Step 2: Clean Row
    # -------------------------------------------------
    clean_start = time.time()

    # clean function still expects list → wrap it
    cleaned = pickup_delivery_items_clean_rows([rows])
    if cleaned:
        rows = cleaned[0]

    clean_end = time.time()
    print(f"Row cleaning completed in {clean_end - clean_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Infer Schema (Iceberg + Arrow)
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = pickup_delivery_items_schema(rows)
    schema_end = time.time()

    print("Inferred Iceberg Schema:", iceberg_schema)
    print("Inferred Arrow Schema:", arrow_schema)

    # -------------------------------------------------
    # Step 4: Convert dict → Arrow Table
    # -------------------------------------------------
    arrow_start = time.time()
    try:
        arrow_table = pa.Table.from_pylist([rows], schema=arrow_schema)
        print("Arrow table created with 1 row")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Arrow conversion failed: {e}")

    arrow_end = time.time()

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
                "message": f"Append failed for table {table_identifier}",
                "exception": str(e),
            },
        )
    append_end = time.time()

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