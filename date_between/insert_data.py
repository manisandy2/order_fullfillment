from botocore.client import Config
import boto3
import pyarrow as pa
from datetime import datetime, date
from fastapi import APIRouter
import os


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


def process_chunk(chunk, arrow_schema):
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}

        for field in arrow_schema:
            val = row.get(field.name)

            try:
                # -------------------------
                # NULL / EMPTY
                # -------------------------
                if val in ("", " ", None):
                    converted_row[field.name] = None
                    continue

                # -------------------------
                # INTEGER
                # -------------------------
                if pa.types.is_integer(field.type):
                    converted_row[field.name] = int(val)

                # -------------------------
                # FLOAT
                # -------------------------
                elif pa.types.is_floating(field.type):
                    converted_row[field.name] = float(val)

                # -------------------------
                # TIMESTAMP / DATE
                # -------------------------
                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    if isinstance(val, (datetime, date)):
                        converted_row[field.name] = (
                            val if isinstance(val, datetime)
                            else datetime.combine(val, datetime.min.time())
                        )
                    elif isinstance(val, str):
                        parsed = None
                        for fmt in date_formats:
                            try:
                                parsed = datetime.strptime(val.strip(), fmt)
                                break
                            except ValueError:
                                continue
                        converted_row[field.name] = parsed
                    else:
                        converted_row[field.name] = None

                # -------------------------
                # STRING (🔥 CRITICAL FIX 🔥)
                # -------------------------
                elif pa.types.is_string(field.type):
                    if isinstance(val, bool):
                        converted_row[field.name] = "true" if val else "false"
                    else:
                        converted_row[field.name] = str(val)

                # -------------------------
                # BOOLEAN
                # -------------------------
                elif pa.types.is_boolean(field.type):
                    converted_row[field.name] = bool(val)

                # -------------------------
                # FALLBACK
                # -------------------------
                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(
                    f"Row {row_idx}, Field '{field.name}', "
                    f"Value={val} ({type(val)}), Error={e}"
                )
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)


