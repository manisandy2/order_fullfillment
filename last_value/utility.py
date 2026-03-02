from core.catalog_client import *
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pyarrow as pa
from datetime import datetime
from fastapi import HTTPException
import uuid

load_dotenv()

def get_last_date_value(namespace, table, column):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table}"
        iceberg_table = catalog.load_table(table_identifier)

        # ---- FAST STRATEGY ----
        # Read only 1 row sorted DESC (no full scan)
        scan = (
            iceberg_table.scan(
                # row_filter=AlwaysTrue(),
                selected_fields=[column]
            )
            .to_arrow()
        )

        if scan.num_rows == 0:
            return {
                "namespace": namespace,
                "table": table,
                "column": column,
                "last_value": None
            }

        # Convert to pandas (small data only)
        df = scan.to_pandas()
        last_value = df[column].max()
        # ✅ ADD +1 SECOND (only if datetime)
        if isinstance(last_value, datetime):
            last_value = last_value + timedelta(seconds=1)
            last_value = last_value.isoformat()
        else:
            last_value = str(last_value)

        return {
            "namespace": namespace,
            "table": table,
            "column": column,
            "last_value": (
                last_value.isoformat()
                if isinstance(last_value, (datetime,))
                else str(last_value)
            )
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch last date value: {str(e)}")


def insert_last_value(namespace: str, table_name: str, column: str):

    try:
        catalog = get_catalog_client()

        # --------------------------------
        # 1️⃣ Get last value from table
        # --------------------------------
        last_value = get_last_date_value(namespace, table_name, column)

        # If no data → skip
        if not last_value:
            return {
                "table": table_name,
                "status": "no_data"
            }

        # --------------------------------
        # 2️⃣ Load Tracking table
        # --------------------------------
        tracking_table = catalog.load_table("order_fulfillment.Tracking")

        # --------------------------------
        # 3️⃣ Build Insert Record
        # --------------------------------
        record = {
            "id": str(uuid.uuid4()),  # simple unique id
            "table_name": str(table_name),
            "last_value": str(last_value),
            "status": "UPDATED",
            "updated_at": datetime.utcnow()
        }

        arrow_schema = tracking_table.schema().as_arrow()
        arrow_table = pa.Table.from_pylist([record], schema=arrow_schema)

        # --------------------------------
        # 4️⃣ Append
        # --------------------------------
        tracking_table.append(arrow_table)

        return {
            "table": table_name,
            "last_value": str(last_value),
            "status": "inserted"
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Tracking insert failed: {e}"
        )