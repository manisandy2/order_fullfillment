from pyiceberg.catalog import NoSuchTableError
from core.catalog_client import get_catalog_client
from fastapi import APIRouter,HTTPException,Query
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
import time
from pyiceberg.expressions import And, EqualTo,GreaterThan,LessThan
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(prefix="", tags=["filters"])

@router.get("/filters/get-master-order")
def filter_customer_phone_master_order(
    # namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    # table_name: str = Query("transaction01", description="Iceberg table name"),
    customer_mobile: str | None = Query(None, description="customer_mobile")
):
    import datetime

    namespace, table_name = "order_fulfillment", "masterorders"
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    if customer_mobile:
        try:
            cond = EqualTo("cust_primary_contact", str(customer_mobile))
        except:
            raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
        expr = cond
    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        # df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "customer_mobile": customer_mobile,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": timeline
    }

@router.get("/filters/get-orderlineitems")
def filter_orderlineitems(
    # namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    # table_name: str = Query("transaction01", description="Iceberg table name"),
    # customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c")
    line_item_id: str | None = Query(None, description="Filter by customer_mobile__c")
):
    import datetime

    namespace, table_name = "order_fulfillment", "orderlineitems"
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    if line_item_id:
        try:
            cond = EqualTo("line_item_id", str(line_item_id))
        except:
            raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
        expr = cond
    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        # df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "line_item_id": line_item_id,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": timeline
    }

@router.get("/filters/get_pickup_delivery_items")
def filter_pickup_delivery_items(
    # namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    # table_name: str = Query("transaction01", description="Iceberg table name"),
    # customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c")
    pickup_delivery_req_item_id: str | None = Query(None, description="pickup_delivery_req_item_id")
):
    import datetime

    namespace, table_name = "order_fulfillment", "pickup_delivery_items"
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    if pickup_delivery_req_item_id:
        try:
            cond = EqualTo("pickup_delivery_req_item_id", str(pickup_delivery_req_item_id))
        except:
            raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
        expr = cond
    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        # df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "pickup_delivery_req_item_id": pickup_delivery_req_item_id,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": timeline
    }

@router.get("/filters/get_status_event")
def filter_status_event(
    # namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    # table_name: str = Query("transaction01", description="Iceberg table name"),
    # customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c")
    status_events: str | None = Query(None, description="Filter by customer_mobile__c")
):
    import datetime

    namespace, table_name = "order_fulfillment", "status_events"
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    if status_events:
        try:
            cond = EqualTo("status_event_id", str(status_events))
        except:
            raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")
        expr = cond
    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        # df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    timeline = round(time.perf_counter() - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "status_events": status_events,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": timeline
    }

# @router.get("/filters/get_status_event")
# def filter_status_event(
#     range_field: str | None = Query(None, description="Column name to apply range filter"),
#     range_start: str | None = Query(None, description="Range start value"),
#     range_end: str | None = Query(None, description="Range end value"),
#     # ... other filters ...
# ):
#     namespace, table_name = "order_fulfillment", "status_events"
#     start_time = time.perf_counter()
#     table_identifier = f"{namespace}.{table_name}"
#
#     catalog = get_catalog_client()
#     tbl = catalog.load_table(table_identifier)
#
#     filters = []
#
#     # ------------------------------------------------------------
#     # 🔥 RANGE / BETWEEN FILTER
#     # ------------------------------------------------------------
#     if range_field:
#         if range_start and range_end:
#             filters.append(
#                 And(
#                     GreaterThanOrEqual(range_field, range_start),
#                     LessThanOrEqual(range_field, range_end)
#                 )
#             )
#         elif range_start:
#             filters.append(GreaterThanOrEqual(range_field, range_start))
#         elif range_end:
#             filters.append(LessThanOrEqual(range_field, range_end))
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail="range_field provided, but no range_start or range_end found."
#             )
#
#     # ------------------------------------------------------------
#     # Combine with existing filters
#     # ------------------------------------------------------------
#     final_expr = None
#     if filters:
#         final_expr = filters[0]
#         for f in filters[1:]:
#             final_expr = And(final_expr, f)
#
#     # ------------------------------------------------------------
#     # Run scan
#     # ------------------------------------------------------------
#     scan = tbl.scan(row_filter=final_expr) if final_expr else tbl.scan()
#     df = scan.to_arrow().to_pandas()
#
#     timeline = round(time.perf_counter() - start_time, 3)
#
#     return {
#         "namespace": namespace,
#         "table_name": table_name,
#         "range_field": range_field,
#         "range_start": range_start,
#         "range_end": range_end,
#         "count": len(df),
#         "sample_rows": df.head(20).to_dict(orient="records"),
#         "timeline_seconds": timeline
#     }

# @router.get("/filters/get")
# def filter_customer_phones_mysql(
#     namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
#     table_name: str = Query("transaction01", description="Iceberg table name"),
#     phone: str = Query(None, description="Filter by customer_mobile__c"),
# ):
#     import datetime
#     start_time = time.perf_counter()

#     # Iceberg table identifier
#     table_identifier = f"{namespace}.{table_name}"

#     catalog = get_catalog_client()

#     # --- Load the table ---
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

#     # --- Build Iceberg Filter ---
#     expr = None
#     if phone:
#         try:
#             expr = EqualTo("customer_mobile__c", int(phone))
#         except:
#             raise HTTPException(status_code=400, detail="Invalid phone number")

#     # --- Perform scan ---
#     try:
#         scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
#         df = scan.to_arrow().to_pandas()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

#     timeline = round(time.perf_counter() - start_time, 3)

#     # --- Response ---
#     return {
#         "namespace": namespace,
#         "table_name": table_name,
#         "phone": phone,
#         "count": len(df),
#         "sample_rows": df.head(10).to_dict(orient="records"),
#         "timeline_seconds": timeline
#     }

@router.get("/filters/exact-date")
def filter_exact_date(
    # namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
    # table_name: str = Query("transaction01", description="Iceberg table name"),
    bill_date: str | None = Query(None, description="Filter by Bill_Date__c (YYYY-MM-DD)"),
    customer_mobile: int | None = Query(None, description="Filter by customer_mobile__c")
):
    import datetime
    """
    Inspect an existing Iceberg table's metadata.
    Optionally filter by partition values (bill_date, store_code, customer_mobile).
    Adds a timeline field to measure total execution time.
    """
    start_time = time.perf_counter()  # Start timeline measurement
    namespace, table_name = "order_fulfillment", "master_order"
    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    # --- Load the table ---
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # --- Build filter expressions dynamically ---
    expr = None
    try:
        if bill_date:
            bill_date_parsed = datetime.datetime.strptime(bill_date, "%Y-%m-%d").date()
            expr = EqualTo("Bill_Date__c", bill_date_parsed)

        if customer_mobile:
            cond = EqualTo("customer_mobile__c", int(customer_mobile))
            expr = cond if expr is None else And(expr, cond)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid filter value: {str(e)}")

    # --- Perform scan ---
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    end_time = time.perf_counter()  # End timeline measurement
    total_time = round(end_time - start_time, 3)  # seconds (rounded to 3 decimals)

    # --- Construct response ---
    return {
        "namespace": namespace,
        "table_name": table_name,
        "filter_applied": {
            "bill_date": bill_date,
            "customer_mobile": customer_mobile
        },
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }

@router.get("/filters/date-range")
def filter_between_date_range(
    # namespace: str = Query("pos_transactions"),
    # table_name: str = Query("iceberg_with_partitioning"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    phone: str | None = Query(None, description="Filter by customer_mobile__c")
):
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual, EqualTo
    import datetime
    start = time.perf_counter()
    namespace, table_name = "order_fulfillment", "master_order"

    # validate dates
    try:
        d1 = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        d2 = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    # base expr = date range
    expr = And(
        GreaterThanOrEqual("Bill_Date__c", d1),
        LessThanOrEqual("Bill_Date__c", d2),
    )

    # add phone filter if present
    if phone:
        try:
            phone_int = int(phone)
        except:
            raise HTTPException(status_code=400, detail="phone must be integer digits")
        expr = And(expr, EqualTo("customer_mobile__c", phone_int))

    # scan / read data
    try:
        df = tbl.scan(row_filter=expr).to_arrow().to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    return {
        "namespace": namespace,
        "table_name": table_name,
        "start_date": start_date,
        "end_date": end_date,
        "phone": phone,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": round(time.perf_counter() - start, 3)
    }

@router.get("/filters/pri_id")
def filter_id(
    namespace: str = Query("pos_transactions"),
    table_name: str = Query("iceberg_with_partitioning"),
    pri_id: str = Query(default=None),

):

    start_time = time.perf_counter()
    table_identifier = f"{namespace}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")

    if pri_id is None:
        raise HTTPException(status_code=400, detail="pri_id is required")

    try:
        pri_id_value = int(pri_id)
    except:
        raise HTTPException(status_code=400, detail="pri_id must be integer")

    expr = EqualTo("pri_id", pri_id_value)

    try:
        df = tbl.scan(row_filter=expr).to_arrow().to_pandas().reset_index(drop=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

    total_time = round(time.perf_counter() - start_time, 3)

    return {
        "namespace": namespace,
        "table_name": table_name,
        "count": len(df),
        "sample_rows": df.head(10).to_dict(orient="records"),
        "timeline_seconds": total_time
    }



def process_table(namespace: str, table_name: str, customer_mobile: str | None):
    """Worker function for each table query (runs in parallel threads)."""
    start_time = time.perf_counter()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"

    result = {
        "namespace": namespace,
        "table_name": table_name,
        "record_count": 0,
        "timeline_seconds": 0,
        "sample_rows": [],
        "error": None
    }

    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        result["error"] = f"Table not found: {table_identifier}"
        return result
    except Exception as e:
        result["error"] = f"Error loading table: {str(e)}"
        return result

    # Build filter condition
    expr = None
    if customer_mobile:
        try:
            expr = EqualTo("customer_mobile__c", int(customer_mobile))
        except Exception as e:
            result["error"] = f"Invalid filter value: {str(e)}"
            return result

    # Perform table scan
    try:
        scan = tbl.scan(row_filter=expr) if expr else tbl.scan()
        df = scan.to_arrow().to_pandas()
        result["record_count"] = len(df)
        result["sample_rows"] = df.head(3).to_dict(orient="records")
    except Exception as e:
        result["error"] = f"Error reading table: {str(e)}"
        return result

    result["timeline_seconds"] = round(time.perf_counter() - start_time, 3)
    return result

# @router.get("/filters/get-multi")
# def filter_customer_phone_multi(
#     # namespaces: list[str] = Query(["pos_transactions01", "pos_transactions02", "pos_transactions03", "pos_transactions04"], description="List of Iceberg namespaces"),
#     # table_names: list[str] = Query(["transaction01", "transaction02", "transaction03", "transaction04"], description="List of Iceberg table names (same order as namespaces)"),
#     customer_mobile: str | None = Query(None, description="Filter by customer_mobile__c"),
#     max_threads: int = Query(4, description="Maximum number of parallel threads"),
# ):
#     """
#     Multithreaded filter across multiple Iceberg namespaces and tables.
#     Executes all queries concurrently and returns per-table metrics.
#     """
#
#     total_start = time.perf_counter()
#     all_results = []
#     namespace, table_name = "order_fulfillment", "master_order"
#     # --- Use ThreadPoolExecutor for parallel querying ---
#     with ThreadPoolExecutor(max_workers=max_threads) as executor:
#         futures = [
#             executor.submit(process_table, ns, tbl_name, customer_mobile)
#             for ns, tbl_name in zip(namespaces, table_names)
#         ]
#
#         # Collect completed results
#         for future in as_completed(futures):
#             all_results.append(future.result())
#
#     total_time = round(time.perf_counter() - total_start, 3)
#
#     return {
#         "total_namespaces": len(namespaces),
#         "total_tables": len(table_names),
#         "thread_count": max_threads,
#         "total_execution_time": total_time,
#         "details": all_results
#     }

# @router.get("/filters/ph-count")
# def filter_customer_phones_mysql(
#     # namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
#     # table_name: str = Query("transaction01", description="Iceberg table name"),
#     phone: str = Query(None, description="Filter by customer_mobile__c"),
# ):
#     import datetime
#     start_time = time.perf_counter()
#     print(phone)
#     namespace, table_name = "order_fulfillment", "master_order"
#
#     # Iceberg table identifier
#     table_identifier = f"{namespace}.{table_name}"
#
#     catalog = get_catalog_client()
#
#     # --- Load the table ---
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")
#
#     # --- Build Iceberg Filter ---
#     expr = None
#     if phone:
#         try:
#             expr = EqualTo("customer_mobile__c", int(phone))
#         except:
#             raise HTTPException(status_code=400, detail="Invalid phone number")
#
#     # --- Perform scan ---
#     print(expr)
#     try:
#         scan = tbl.scan(row_filter=expr).count()
#         print(scan)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")
#
#     timeline = round(time.perf_counter() - start_time, 3)
#
#     # --- Response ---
#     return {
#         "namespace": namespace,
#         "table_name": table_name,
#         "phone": phone,
#         "count": scan,
#         "timeline_seconds": timeline
#     }

@router.get("/filters/get-orderlineitems_test")
def filter_orderlineitems(
    start_id: int | None = Query(None),
    end_id: int | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    namespace, table_name = "order_fulfillment", "orderlineitems_test"
    table_identifier = f"{namespace}.{table_name}"

    start_time = time.perf_counter()
    catalog = get_catalog_client()

    # -----------------------------
    # Load table
    # -----------------------------
    tbl = catalog.load_table(table_identifier)

    # -----------------------------
    # Build Iceberg filter
    # -----------------------------
    expr = None
    if start_id is not None and end_id is not None:
        expr = And(
            GreaterThanOrEqual("line_item_id", start_id),
            LessThanOrEqual("line_item_id", end_id)
        )
    elif start_id is not None:
        expr = GreaterThanOrEqual("line_item_id", start_id)
    elif end_id is not None:
        expr = LessThanOrEqual("line_item_id", end_id)

    # -----------------------------
    # Scan Iceberg
    # -----------------------------
    scan = tbl.scan(
        row_filter=expr,
        selected_fields=[
            "line_item_id",
            "order_line_item_id",
            "master_order_id",
            "created_at",
        ],
    )

    df = scan.to_arrow().to_pandas()

    # -----------------------------
    # Apply LIMIT + OFFSET (client-side)
    # -----------------------------
    paged_df = df.iloc[offset : offset + limit]

    timeline = round(time.perf_counter() - start_time, 3)

    return {
        "count": len(paged_df),
        "limit": limit,
        "offset": offset,
        "rows": paged_df.to_dict(orient="records"),
        "timeline_seconds": timeline,
    }
############
# test
# @router.get("/powerbi/orderlineitems")
# def powerbi_data(start_date: str, end_date: str):
#     table = catalog.load_table("order_fulfillment.orderlineitems")
#     df = table.scan(
#         row_filter=And(
#             GreaterThanOrEqual("created_at", start_date),
#             LessThan("created_at", end_date)
#         )
#     ).to_pandas()
#
#     return df.to_dict(orient="records")