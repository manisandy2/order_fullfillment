from fastapi import APIRouter, HTTPException
from pyarrow.dataset import partitioning
from fastapi.encoders import jsonable_encoder
from core.mysql_client import MysqlCatalog
import io
from pyiceberg.schema import Schema
from pyiceberg.types import *
from botocore.client import Config
import botocore
import boto3
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform,YearTransform,MonthTransform,DayTransform,BucketTransform,VoidTransform
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError

import traceback
import pyarrow as pa
from datetime import datetime, date
from fastapi import APIRouter,HTTPException,Query
from dateutil import parser

import time

from concurrent.futures import ThreadPoolExecutor, as_completed
import os

LOGS_FOLDER = "logs/iceberg_upload"
os.makedirs(LOGS_FOLDER, exist_ok=True)

router = APIRouter(prefix="", tags=["data insertion"])

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
        "pri_id": (LongType(), pa.int64(), True),
        "Invoice_Amount__c": (DoubleType(), pa.float64(), False),
        "Bill_Date__c": (DateType(), pa.date32(), False),
        "CreatedDate": (DateType(), pa.date32(), False),
    }

    for idx, (name, value) in enumerate(record.items(), start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            # Type inference
            if isinstance(value, bool):
                ice_type = BooleanType()
                arrow_type = pa.bool_()
            elif isinstance(value, int):
                ice_type = LongType()
                arrow_type = pa.int64()
            elif isinstance(value, float):
                ice_type = DoubleType()
                arrow_type = pa.float64()
            elif isinstance(value, (date, datetime)):
                ice_type = DateType()
                arrow_type = pa.date32()
            else:
                ice_type = StringType()
                arrow_type = pa.string()
            required = False

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema




@router.post("/insert-ph-direct-data")
def insert_transaction_phone_data(
    start_range: int = Query(0, description="Start row offset for MySQL data fetch"),
    end_range: int = Query(100000, description="End row offset for MySQL data fetch"),
    chunk_size: int = Query(10000, description="Chunk size for multithreading"),
):
    total_start = time.time()
    namespace, table_name = "pos_transactions", "iceberg_with_partitioning"
    dbname = "Transaction"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        converted_rows = []


        for row in rows:
            # 1 Convert float fields safely
            float_fields = ["bill_tax__c", "bill_grand_total__c", "Invoice_Amount__c"]
            for f in float_fields:
                val = row.get(f)
                if isinstance(val, str):
                    try:
                        row[f] = float(val)
                    except ValueError:
                        row[f] = 0.0
                elif val is None:
                    row[f] = 0.0

            # Convert mobile numbers to int64
            mobile_val = row.get("customer_mobile__c")
            if isinstance(mobile_val, str):
                try:
                    row["customer_mobile__c"] = int(mobile_val)
                except ValueError:
                    row["customer_mobile__c"] = None

            # Convert Item_Code__c to int64
            item_val = row.get("Item_Code__c")
            if isinstance(item_val, str):
                try:
                    row["Item_Code__c"] = int(item_val)
                except ValueError:
                    row["Item_Code__c"] = 0

            # Convert date strings to Python `date` object (yyyy-mm-dd only)
            for date_field in ["Bill_Date__c",  "CreatedDate"]:
                val = row.get(date_field)

                if not val or str(val).strip() == "":
                    row[date_field] = None
                    continue

                try:
                    # use auto parser
                    dt = parser.parse(str(val))  # can parse both '6/24/2021 0:00' and '2021-06-24 00:00:00'
                    row[date_field] = dt
                except Exception as e:
                    print(f"⚠️ Error converting {date_field}: {val} ({e})")
                    row[date_field] = None

            converted_rows.append(row)



        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    # print("iceberg_schema",iceberg_schema)
    # print("arrow_schema",arrow_schema)

    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [converted_rows[i:i + chunk_size] for i in range(0, len(converted_rows), chunk_size)]

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
        error_message = str(e)
        error_code = "ICEBERG_APPEND_FAILED"
        print(f"❌ {error_code}: {error_message}")

        raise HTTPException(
            status_code=500,
            detail={
                "error_code": error_code,
                "message": f"Data append failed for table {table_identifier}",
                "exception": error_message,
            },
        )

    print(f"✅ Append completed in {append_end - append_start:.2f} sec")

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
            # "total_time": round(total_end - total_start, 2),
        },
    }
