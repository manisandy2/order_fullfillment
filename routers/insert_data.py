from fastapi import APIRouter, HTTPException
from pyarrow.dataset import partitioning
from fastapi.encoders import jsonable_encoder
from core.mysql_client import MysqlCatalog
import io
from pyiceberg.schema import Schema
from pyiceberg.types import *
from botocore.client import Config
import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import NoSuchTableError
import pyarrow as pa
from datetime import datetime, date
from fastapi import APIRouter,HTTPException,Query
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from core.catalog_client import get_catalog_client

LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)

router = APIRouter(prefix="", tags=["data Insert"])

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT"),
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto"
)

type_mapping = {
    "int": LongType(),
    'bigint': LongType(),
    'varchar': StringType(),
    'char': StringType(),
    'text': StringType(),
    'longtext': StringType(),
    'date': DateType(),
    'datetime': TimestampType(),
    'timestamp': TimestampType(),
    'float': FloatType(),
    'double': DoubleType(),
    'boolean': BooleanType(),
    'tinyint': BooleanType()
}

arrow_mapping = {
    # 'int': pa.int32(),
    "int": pa.int64(),
    'bigint': pa.int64(),
    'varchar': pa.string(),
    'char': pa.string(),
    'text': pa.string(),
    'longtext': pa.string(),
    'date': pa.date32(),
    'datetime': pa.timestamp('ms'),
    'timestamp': pa.timestamp('ms'),
    'float': pa.float32(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'tinyint': pa.bool_(),
    'bit': pa.bool_(),
    # 'decimal': lambda p=18, s=6: pa.decimal128(p, s)
    'decimal' : pa.decimal128(18, 6)
}

def infer_schema_from_record(record: dict):
    iceberg_fields = []
    arrow_fields = []

    # Custom field overrides (by name)
    field_overrides = {
    # Required keys
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # Integer fields
    "oms_data_migration_status": (IntegerType(), pa.int32(), False),
    "cust_id_update": (IntegerType(), pa.int32(), False),

    # Float fields
    "latitude": (FloatType(), pa.float32(), False),
    "longitude": (FloatType(), pa.float32(), False),

    # Date fields
    "invoice_date": (DateType(), pa.date32(), False),
    "updated_at_new": (DateType(), pa.date32(), False),

    # Timestamp fields
    # "invoice_date": (TimestampType(), pa.timestamp('ms'), False),
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
    # "updated_at_new": (TimestampType(), pa.timestamp('ms'), False),

    # Other explicit string fields
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),
    "channel": (StringType(), pa.string(), False),
    "channel_medium": (StringType(), pa.string(), False),
    "order_status": (StringType(), pa.string(), False),
    "order_tag": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "order_type": (StringType(), pa.string(), False),
    "delivery_from": (StringType(), pa.string(), False),
    "delivery_from_branchcode": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),
    "cust_id": (StringType(), pa.string(), False),
    "cust_primary_email": (StringType(), pa.string(), False),
    "cust_primary_contact": (StringType(), pa.string(), False),
    "cust_mobile": (StringType(), pa.string(), False),
    "customer_address": (StringType(), pa.string(), False),
    "shipment_address": (StringType(), pa.string(), False),
    "billing_address": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),
    "refund_details": (StringType(), pa.string(), False),
    "voucher_details": (StringType(), pa.string(), False),
    "employee_sale_details": (StringType(), pa.string(), False),
    "order_summary_details": (StringType(), pa.string(), False),
    "other_details": (StringType(), pa.string(), False),
    "service_details": (StringType(), pa.string(), False),
    "invoice_pdf": (StringType(), pa.string(), False),
    "lineitems": (StringType(), pa.string(), False),
    "lineitem_status": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
    "multi_invoice": (StringType(), pa.string(), False),
    }

    for idx, (name, value) in enumerate(record.items(), start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False
            # Boolean
            if isinstance(value, bool):
                ice_type,arrow_type = BooleanType(),pa.bool_()

            # Integer
            elif isinstance(value, int):
                ice_type,arrow_type = LongType(),pa.int64()

            # Float
            elif isinstance(value, float):
                ice_type,arrow_type = DoubleType(),pa.float64()

            # Date only
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # Timestamp
            elif isinstance(value, datetime):
                ice_type,arrow_type = DateType(),pa.date32()

            # String
            else:
                ice_type,arrow_type = StringType(),pa.string()
                 

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema

def process_chunk(chunk, arrow_schema):
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}
        # print(f" Processing row {row_idx} -> keys: {list(row.keys())}")

        for field in arrow_schema:
            val = row.get(field.name, None)

            # Debug mismatched field
            if field.name not in row:
                print(f"Field '{field.name}' missing in row; available keys: {list(row.keys())}")

            try:
                # --- Handle empty or None values ---
                if val in ("", " ", None):
                    converted_row[field.name] = None
                    continue

                # --- Integer fields ---
                if pa.types.is_integer(field.type):
                    converted_row[field.name] = int(val)

                # --- Float fields ---
                elif pa.types.is_floating(field.type):
                    converted_row[field.name] = float(val)

                # --- Timestamp or date fields ---
                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    parsed_date = None

                    if isinstance(val, (datetime, date)):
                        parsed_date = val
                    elif isinstance(val, str):
                        val = val.strip()
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(val, fmt)
                                break
                            except ValueError:
                                continue

                    if parsed_date:
                        converted_row[field.name] = (
                            parsed_date if isinstance(parsed_date, datetime)
                            else datetime.combine(parsed_date, datetime.min.time())
                        )
                    else:
                        print(f" Row {row_idx}: Unrecognized date in '{field.name}': {val}")
                        converted_row[field.name] = None

                # --- Default: keep as string or object ---
                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(f" Row {row_idx}, Field '{field.name}', Value: {val}, Error: {e}")
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)


def clean_rows(rows):

    float_fields = ["latitude", "longitude"]
    integer_fields = ["cust_id_update","oms_data_migration_status"]
    timestamp_fields = ["created_at", "updated_at","invoice_date","updated_at_new"]
    string_fields = ["order_id", "sale_order_id", "invoice_no", "invoice_reff_no", "invoice_reff_date",
                     "channel", "channel_medium", "order_status", "order_tag", "order_inv_status", "order_type",
                     "delivery_from", "delivery_from_branchcode", "billing_branch_code", "cust_id", "cust_primary_email",
                     "cust_primary_contact", "cust_mobile", "customer_address", "shipment_address", "billing_address",
                     "payment_details", "refund_details", "voucher_details", "employee_sale_details", "order_summary_details",
                     "other_details", "service_details", "invoice_pdf", "lineitems", "lineitem_status", "created_by", "updated_by",
                     "multi_invoice"]            

    for row in rows:

        # 1 -------- Float Fields ----------------------------------------
        for f in float_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = float(val)
                except ValueError:
                    row[f] = 0.0
            elif val is None:
                row[f] = 0.0

        # 2 -------- Integer Fields --------------------------------------
        for f in integer_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    row[f] = 0
            elif val is None:
                row[f] = 0

        # 3 -------- String Fields ---------------------------------------
        for f in string_fields:
            val = row.get(f)
            if val is None:
                row[f] = ""
            else:
                row[f] = str(val)

        # 4 -------- Timestamp Fields ------------------------------------
        for f in timestamp_fields:
            val = row.get(f)

            if val is None or val == "":
                row[f] = None
                continue

            if isinstance(val, datetime):
                continue

            # try multiple formats
            parsed = None
            dt_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d",
            ]

            for fmt in dt_formats:
                try:
                    parsed = datetime.strptime(val, fmt)
                    break
                except:
                    pass

            row[f] = parsed if parsed else None

    return rows

@router.post("/insert-master-data")
def insert_transaction_phone_data(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "order_fulfillment", "master_order"
    dbname = "masterorders"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        for row in rows[0]:
            print("Sample Row:", row)
            print(row,type(row))

        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
    
    print("#"*100)
    clean_rows(rows)
    print("Cleaned Rows Sample:", rows[:2])
    print("#"*100)
    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    print("#"*100)
    print("Inferred Iceberg Schema:", iceberg_schema)
    print("#"*100)
    print("Inferred Arrow Schema:", arrow_schema)
    print("#"*100)

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
