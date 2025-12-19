import os
import io
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

def discover_collections():
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://3adc773036202b79e98ea19eb62480e2.r2.cloudflarestorage.com",
        aws_access_key_id="8e3c29f40c09ac75061428be50aabc3e",
        aws_secret_access_key="7e99455164348619a42b05956dde380482afc51989c76a7e4ce26e235a687be9",
        config=Config(signature_version="s3v4"),
    )

    resp = s3.list_objects_v2(
        Bucket="mongo-migration-bucket",
        Delimiter="/"
    )

    return [p["Prefix"].rstrip("/") for p in resp.get("CommonPrefixes", [])]

def get_r2_data_and_save(collection, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/{collection}.parquet"

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://3adc773036202b79e98ea19eb62480e2.r2.cloudflarestorage.com",
        aws_access_key_id="8e3c29f40c09ac75061428be50aabc3e",
        aws_secret_access_key="7e99455164348619a42b05956dde380482afc51989c76a7e4ce26e235a687be9",
        config=Config(signature_version="s3v4"),
    )

    prefix = f"{collection}/"
    paginator = s3.get_paginator("list_objects_v2")

    writer = None
    files_written = 0

    for page in paginator.paginate(Bucket="mongo-migration-bucket", Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            try:
                response = s3.get_object(Bucket="mongo-migration-bucket", Key=key)
                df = pd.read_parquet(io.BytesIO(response["Body"].read()))

                if df.empty:
                    continue

                table = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(
                        output_file,
                        table.schema,
                        compression="snappy"
                    )

                writer.write_table(table)
                files_written += 1

            except Exception as e:
                print(f"Failed to process {key}: {e}")

    if writer:
        writer.close()
        return output_file, files_written

    return None, 0

def run():
    collections = discover_collections()

    if not collections:
        print("No collections found")
        return

    for collection in collections:
        path, count = get_r2_data_and_save(collection)

        if path:
            print(f"✅ Saved {count} parquet files into: {path}")
        else:
            print(f"⚠️ No parquet data for collection: {collection}")

run()