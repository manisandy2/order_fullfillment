import asyncio
import pyarrow as pa
from datetime import datetime
from .queue import order_queue
from core.catalog_client import get_catalog_client
# from core.cleaner import masterOrder_clean_rows
# from core.schema import masterorder_schema
from .masterOrderUtility import masterOrder_clean_rows,masterorder_schema

BATCH_SIZE = 500
FLUSH_INTERVAL = 5  # seconds

async def iceberg_worker():
    buffer = []
    last_flush = datetime.utcnow()

    while True:
        try:
            row = await asyncio.wait_for(order_queue.get(), timeout=1)
            buffer.append(row)
        except asyncio.TimeoutError:
            pass

        now = datetime.utcnow()

        if (
            len(buffer) >= BATCH_SIZE or
            (buffer and (now - last_flush).seconds >= FLUSH_INTERVAL)
        ):
            write_batch(buffer)
            buffer.clear()
            last_flush = now


def write_batch(rows: list[dict]):
    cleaned = masterOrder_clean_rows(rows)

    iceberg_schema, arrow_schema = masterorder_schema(cleaned[0])
    arrow_table = pa.Table.from_pylist(cleaned, schema=arrow_schema)

    catalog = get_catalog_client()
    table = catalog.load_table("order_fulfillment.masterorders_w")

    table.append(arrow_table)