from .intransit_manifestsUtility import *
from core.between_date import MysqlCatalog
from date_between.utility import *


def intransit_manifests_between_date():

    namespace = "order_fulfillment"
    table_name = "intransit_manifests"
    dbname = "intransit_manifests"
    chunk_size = 1000

    last_val = get_last_date_value(namespace, table_name, "created_at")
    if not last_val["last_value"]:
        return {"status": "NO_EXISTING_DATA"}

    start_date = datetime.fromisoformat(last_val["last_value"])
    end_date = yesterday()

    validate_date_range(start_date, end_date)

    with MysqlCatalog() as mysql:

        rows = fetch_mysql_date_range(
            mysql_client=mysql,
            dbname=dbname,
            fetch_fn=mysql.get_intransit_manifests_date_between,
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
    failed_chunks = []
    success_chunks = 0

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
        error_type="ICEBERG_APPEND_FAILED",

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
    return intransit_manifests_between_date()

if __name__ == "__main__":
    result = run()
    print(result)