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
from fastapi import APIRouter,HTTPException,Query,Body
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from core.catalog_client import get_catalog_client
# from pickup_delivery_items import pickup_delivery_items_clean_rows,pickup_delivery_items_schema
from .pickup_delivery_items_Utility import *  
from .masterOrderUtility import masterOrder_clean_rows,masterorder_schema

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

# type_mapping = {
#     "int": LongType(),
#     'bigint': LongType(),
#     'varchar': StringType(),
#     'char': StringType(),
#     'text': StringType(),
#     'longtext': StringType(),
#     'date': DateType(),
#     'datetime': TimestampType(),
#     'timestamp': TimestampType(),
#     'float': FloatType(),
#     'double': DoubleType(),
#     'boolean': BooleanType(),
#     'tinyint': BooleanType()
# }

# arrow_mapping = {
#     # 'int': pa.int32(),
#     "int": pa.int64(),
#     'bigint': pa.int64(),
#     'varchar': pa.string(),
#     'char': pa.string(),
#     'text': pa.string(),
#     'longtext': pa.string(),
#     'date': pa.date32(),
#     'datetime': pa.timestamp('ms'),
#     'timestamp': pa.timestamp('ms'),
#     'float': pa.float32(),
#     'double': pa.float64(),
#     'boolean': pa.bool_(),
#     'tinyint': pa.bool_(),
#     'bit': pa.bool_(),
#     # 'decimal': lambda p=18, s=6: pa.decimal128(p, s)
#     'decimal' : pa.decimal128(18, 6)
# }


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
######################################################################



