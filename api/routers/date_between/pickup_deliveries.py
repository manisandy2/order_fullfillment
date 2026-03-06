from core.between_date import MysqlCatalog
from date_between.utility import *
from date_between.pickup_deliveriesUtility import *

# def pickup_deliveries_between_date():
#
#     namespace = "order_fulfillment"
#     table_name = "pickup_deliveries"
#     dbname = "pickup_deliveries"
#     chunk_size = 1000
#
#     last_val = get_last_date_value(namespace, table_name, "row_added_dttm")
#     if not last_val["last_value"]:
#         return {"status": "NO_EXISTING_DATA"}
#     start_date = datetime.fromisoformat(last_val["last_value"])
#     end_date = yesterday()
#
#     validate_date_range(start_date, end_date)
#
#     with MysqlCatalog() as mysql:
#         rows = fetch_mysql_date_range(
#             mysql_client=mysql,
#             dbname=dbname,
#             fetch_fn=mysql.get_pickup_deliveries_date_between,
#             start_date=start_date,
#             end_date=end_date,
#         )
#     if not rows:
#         return {
#             "status": "NO_DATA",
#             "rows_fetched": 0,
#             "start_date": start_date,
#             "end_date": end_date,
#         }
#     clean_rows(
#         rows,
#         boolean_fields=BOOLEAN_FIELDS,
#         timestamps_fields=TIMESTAMP_FIELDS,
#         date_fields=DATE_FIELDS,
#         field_overrides=FIELD_OVERRIDES,
#     )
#
#     _, arrow_schema = schema(rows[0], FIELD_OVERRIDES)
#
#     chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
#     tbl = load_table_identifier(namespace, table_name)
#     # arrow_tables = []
#     failed_chunks = []
#     success_chunks = 0
#     # arrow_tables, failed_chunks = multi_executor(arrow_schema, chunks, arrow_tables, failed_chunks)
#     # multi_executor(arrow_schema, chunks, arrow_tables, failed_chunks)
#
#     # arrow_errors = handle_failed_chunks(
#     #     table_name=table_name,
#     #     failed_chunks=failed_chunks,
#     #     error_type="ARROW_CONVERSION_FAILED",
#     # )
#
#     for idx, chunk in enumerate(chunks):
#
#         try:
#             # 🔥 Memory check before processing
#             check_memory_limit(3000)
#
#             arrow_table = process_chunk(chunk, arrow_schema)
#
#             tbl.append(arrow_table)
#
#             success_chunks += 1
#
#             # 🔥 Cleanup immediately
#             del arrow_table
#             gc.collect()
#
#             # 🔥 Memory check after cleanup
#             check_memory_limit(3000)
#
#         except Exception as e:
#             failed_chunks.append({
#                 "chunk_index": idx,
#                 "chunk_data": chunk,
#                 "error": str(e),
#             })
#
#     # failed_batches = []
#     # for batch in arrow_tables:
#     #     try:
#     #         tbl.append(batch)
#     #     except Exception as e:
#     #         failed_batches.append({
#     #             "chunk_data": batch.to_pylist(),
#     #             "error": str(e)
#     #         })
#
#     append_errors = handle_failed_chunks(
#         table_name=table_name,
#         failed_chunks=failed_chunks,
#         error_type="CHUNK_PROCESS_OR_APPEND_FAILED",
#     )
#
#     return {
#         "rows_fetched": len(rows),
#         "start_date": start_date,
#         "end_date": end_date,
#         "chunks_total": len(chunks),
#         # "chunks_success": len(arrow_tables),
#         "chunks_failed": len(failed_chunks),
#         # "append_failed": len(failed_batches),
#         # "arrow_errors": arrow_errors,
#         "append_errors": append_errors,
#         "status": "COMPLETED"
#     }

# def pickup_deliveries_between_date():
#
#     namespace = "order_fulfillment"
#     table_name = "pickup_deliveries"
#     dbname = "pickup_deliveries"
#     chunk_size = 1000
#
#     last_val = get_last_date_value(namespace, table_name, "row_added_dttm")
#     if not last_val["last_value"]:
#         return {"status": "NO_EXISTING_DATA"}
#
#     start_date = datetime.fromisoformat(last_val["last_value"])
#     end_date = yesterday()
#
#     validate_date_range(start_date, end_date)
#
#     tbl = load_table_identifier(namespace, table_name)
#
#     success_chunks = 0
#     failed_chunks = 0
#     total_rows = 0
#
#     with MysqlCatalog() as mysql:
#
#         # IMPORTANT: Your MySQL method must support LIMIT + ORDER BY
#         while True:
#
#             rows = fetch_mysql_date_range(
#                 mysql_client=mysql,
#                 dbname=dbname,
#                 fetch_fn=mysql.get_pickup_deliveries_date_between,
#                 start_date=start_date,
#                 end_date=end_date,
#                 # limit=chunk_size
#             )
#
#             if not rows:
#                 break
#
#             total_rows += len(rows)
#
#             try:
#                 clean_rows(
#                     rows,
#                     boolean_fields=BOOLEAN_FIELDS,
#                     timestamps_fields=TIMESTAMP_FIELDS,
#                     date_fields=DATE_FIELDS,
#                     field_overrides=FIELD_OVERRIDES,
#                 )
#
#                 _, arrow_schema = schema(rows[0], FIELD_OVERRIDES)
#
#                 arrow_table = process_chunk(rows, arrow_schema)
#
#                 tbl.append(arrow_table)
#
#                 success_chunks += 1
#
#                 # Move forward
#                 start_date = max(r["row_added_dttm"] for r in rows)
#
#                 # 🔥 CRITICAL MEMORY CLEANUP
#                 del rows
#                 del arrow_table
#                 gc.collect()
#                 pa.default_memory_pool().release_unused()
#
#             except Exception as e:
#                 failed_chunks += 1
#                 print("Chunk failed:", e)
#
#                 del rows
#                 gc.collect()
#
#     return {
#         "rows_fetched": total_rows,
#         "chunks_success": success_chunks,
#         "chunks_failed": failed_chunks,
#         "status": "COMPLETED"
#     }

def pickup_deliveries_between_date():

    namespace = "order_fulfillment"
    table_name = "pickup_deliveries"
    dbname = "pickup_deliveries"
    chunk_size = 1000

    last_val = get_last_date_value(namespace, table_name, "row_added_dttm")
    if not last_val["last_value"]:
        return {"status": "NO_EXISTING_DATA"}

    start_date = datetime.fromisoformat(last_val["last_value"])
    end_date = yesterday()

    validate_date_range(start_date, end_date)

    with MysqlCatalog() as mysql:

        rows = fetch_mysql_date_range(
            mysql_client=mysql,
            dbname=dbname,
            fetch_fn=mysql.get_pickup_deliveries_date_between,
            start_date=start_date,
            end_date=end_date,

        )
    if not rows:
        return {
            "status": "NO_DATA",
            "rows_fetched": 0,
            "start_date": start_date,
            "end_date": end_date,
        }


    clean_rows(
        rows,
        boolean_fields=BOOLEAN_FIELDS,
        timestamps_fields=TIMESTAMP_FIELDS,
        date_fields=DATE_FIELDS,
        field_overrides=FIELD_OVERRIDES,
    )

    _, arrow_schema = schema(rows[0], FIELD_OVERRIDES)
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
    tbl = load_table_identifier(namespace, table_name)
    success_chunks = 0
    failed_chunks = []

    for idx, chunk in enumerate(chunks):
        try:
            check_memory_limit(3000)
            arrow_table = process_chunk(chunk, arrow_schema)
            tbl.append(arrow_table)
            success_chunks += 1
            del arrow_table
            gc.collect()
            check_memory_limit(3000)

        except Exception as e:
            failed_chunks.append({
                "chunk_index": idx,
                "chunk_data": chunk,
                "error": str(e),
            })

    append_errors = handle_failed_chunks(
        table_name=table_name,
        failed_chunks=failed_chunks,
        error_type="CHUNK_PROCESS_OR_APPEND_FAILED",

    )

    return {
        "rows_fetched": len(rows),
        "start_date": start_date,
        "end_date": end_date,
        "chunks_total": len(chunks),
        "chunks_failed": len(failed_chunks),
        "append_errors": append_errors,
        "status": "COMPLETED"
    }


def run():
    return pickup_deliveries_between_date()

if __name__ == "__main__":
    print(f"🧠 Initial Memory: {get_memory_mb()} MB")
    print(run())
    print(f"🧠 Final Memory: {get_memory_mb()} MB")