import pandas as pd
import boto3
import io
import pyarrow.parquet as pq
from botocore.config import Config

# ==========================================
# POWER BI PYTHON SCRIPT - PRODUCTION
# ==========================================

# 1. R2 CREDENTIALS
R2_ACCOUNT_ID = "3adc773036202b79e98ea19eb62480e2"
R2_ACCESS_KEY_ID = "8e3c29f40c09ac75061428be50aabc3e"
R2_SECRET_ACCESS_KEY = "7e99455164348619a42b05956dde380482afc51989c76a7e4ce26e235a687be9"
BUCKET_NAME = "mongo-migration-bucket"

def save_parquet_incremental(all_dfs, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    writer = None

    for df in all_dfs:
        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(
                file_path,
                table.schema,
                compression="snappy"
            )

        writer.write_table(table)

    if writer:
        writer.close()

# 2. FUNCTION TO FETCH DATA (PARTITION AWARE)
def get_r2_data(collection):
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )
    print(s3)
    try:
        # Partitioned Read Strategy
        prefix = f"{collection}/"

        # List all objects
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
        print(pages)
        all_dfs = []

        for page in pages:
            if 'Contents' in page:
                for obj_meta in page['Contents']:
                    key = obj_meta['Key']
                    if key.endswith(".parquet"):
                        try:
                            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                            print(df)
                            all_dfs.append(df)
                        except Exception:
                            pass

        # if all_dfs:
        #     print(all_dfs)
        #     return pd.concat(all_dfs, ignore_index=True)
        if all_dfs:
            # save_parquet_incremental(
            #     all_dfs,
            #     file_path=f"output/{collection}.parquet"
            # )
            # return f"Saved Parquet: output/{collection}.parquet"
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df.to_parquet(
                f"json_backup/{collection}.parquet",
                engine="pyarrow",
                compression="snappy",
                index=False
            )

        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# 3. DYNAMIC DISCOVERY & LOAD
# Automatically find all collections in the bucket
try:
    s3_loader = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

    # List top-level folders (Collections)
    resp = s3_loader.list_objects_v2(Bucket=BUCKET_NAME, Delimiter='/')

    found_collections = []
    if 'CommonPrefixes' in resp:
        found_collections = [p['Prefix'].rstrip('/') for p in resp['CommonPrefixes']]

    # Load each discovered collection into a global variable
    for col_name in found_collections:
        df = get_r2_data(col_name)
        if not df.empty:
            # Create a global variable with the collection name
            globals()[col_name] = df

except Exception as e:
    print(f"Error discovering collections: {e}")