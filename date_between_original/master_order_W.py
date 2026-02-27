from utility import *
from core.between_date import MysqlCatalog
from master_order_WUtility import *

def master_order_w_between_date():

    namespace = "order_fulfillment"
    table_name = "masterorders_w"
    dbname = "masterorders_w"
    chunk_size = 1000

    last_val = get_last_date_value(namespace, table_name, "created_at")
    start_date = datetime.fromisoformat(last_val["last_value"])
    end_date = yesterday()

    validate_date_range(start_date, end_date)

    mysql = MysqlCatalog()

    rows = fetch_mysql_date_range(
        mysql_client=mysql,
        dbname=dbname,
        fetch_fn=mysql.get_master_order_w_date_between,
        start_date=start_date,
        end_date=end_date,
    )

    clean_rows(
        rows,
        boolean_fields=BOOLEAN_FIELDS,
        timestamps_fields=TIMESTAMP_FIELDS,
        date_fields=DATE_FIELDS,
        field_overrides=FIELD_OVERRIDES,
    )

    # Create a deterministic schema using all expected columns + fields handled by clean_rows
    # This prevents runtime type inference errors if rows[0] is missing fields or contains None
    from core.db_colums import masterorder_columns
    
    schema_keys = set(masterorder_columns) | set(BOOLEAN_FIELDS) | set(TIMESTAMP_FIELDS) | set(DATE_FIELDS)
    dummy_row = {key: None for key in schema_keys}
    _, arrow_schema = schema(dummy_row, FIELD_OVERRIDES)

    chunks = [rows[i:i+chunk_size] for i in range(0, len(rows), chunk_size)]
    arrow_tables = []
    failed_chunks = []
    # arrow_tables, failed_chunks = multi_executor(arrow_schema, chunks, arrow_tables, failed_chunks)
    multi_executor(arrow_schema, chunks, arrow_tables, failed_chunks)

    arrow_errors = handle_failed_chunks(
        table_name=table_name,
        failed_chunks=failed_chunks,
        error_type="ARROW_CONVERSION_FAILED",
    )

    tbl = load_table_identifier(namespace, table_name)

    failed_batches = []
    for batch in arrow_tables:
        try:
            tbl.append(batch)
        except Exception as e:
            failed_batches.append({
                "chunk_data": batch.to_pylist(),
                "error": str(e)
            })

    append_errors = handle_failed_chunks(
        table_name=table_name,
        failed_chunks=failed_chunks,
        error_type="ICEBERG_APPEND_FAILED",

    )

    return {
        "rows_fetched": len(rows),
        "start_date":start_date,
        "end_date":end_date,
        "chunks_total": len(chunks),
        "chunks_success": len(arrow_tables),
        "chunks_failed": len(failed_chunks),
        "append_failed": len(failed_batches),
        "arrow_errors": arrow_errors,
        "append_errors": append_errors,
        "status": "COMPLETED"
    }

def run():
    return master_order_w_between_date()