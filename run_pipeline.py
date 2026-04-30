

def run_between_date_pipeline(namespace, table_name, dbname, fetch_fn, chunk_size=1000):

    last_val = get_last_date_value(namespace, table_name, "created_at")

    if not last_val["last_value"]:
        return {"table": table_name, "status": "NO_EXISTING_DATA"}

    start_date = datetime.fromisoformat(last_val["last_value"])
    end_date = yesterday()

    validate_date_range(start_date, end_date)

    tbl = load_table_identifier(namespace, table_name)

    with MysqlCatalog() as mysql:
        rows = fetch_mysql_date_range(
            mysql_client=mysql,
            dbname=dbname,
            fetch_fn=fetch_fn,
            start_date=start_date,
            end_date=end_date,
        )

    if not rows:
        return {
            "table": table_name,
            "status": "NO_DATA",
            "rows_fetched": 0,
        }

    clean_rows(
        rows,
        boolean_fields=BOOLEAN_FIELDS,
        timestamps_fields=TIMESTAMP_FIELDS,
        field_overrides=FIELD_OVERRIDES,
    )

    _, arrow_schema = schema(rows[0], FIELD_OVERRIDES)

    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

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

        except Exception as e:
            failed_chunks.append({
                "chunk_index": idx,
                "error": str(e),
            })

    return {
        "table": table_name,
        "rows_fetched": len(rows),
        "chunks_total": len(chunks),
        "chunks_failed": len(failed_chunks),
        "status": "COMPLETED",
    }