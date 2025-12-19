from fastapi import FastAPI,Query,HTTPException
from botocore.exceptions import ClientError
import time, json, boto3, os
from pyiceberg.exceptions import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError
from pydantic import BaseModel
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema, NestedField
import json,os,time
from fastapi import FastAPI, Query,Body, HTTPException,UploadFile, File
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual,EqualTo
from decimal import Decimal
from typing import List
import json
import decimal
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from mysql.connector import Error
import pandas as pd
import logging
# from routers import bucket
from routers import namespace
from routers import table
from routers import insert_data
from routers import filters
from routers import master_order
from routers import master_order_w
from routers import pickup_delivery_items
from routers import pickup_delivery_items_w
from routers import status_events
from routers import orderlineitems
from routers import orderlineitems_test
from routers import column
from routers import schema
from routers.worker import iceberg_worker
from core.mysql_client import MysqlCatalog
import asyncio

logger = logging.getLogger(__name__)

app = FastAPI()


# app.include_router(bucket.router)
app.include_router(namespace.router)
app.include_router(table.router)
# app.include_router(insert_data.router)
app.include_router(master_order.router)
app.include_router(master_order_w.router)
app.include_router(pickup_delivery_items.router)
app.include_router(pickup_delivery_items_w.router)
app.include_router(status_events.router)
app.include_router(orderlineitems.router)
app.include_router(orderlineitems_test.router)
app.include_router(filters.router)
app.include_router(column.router)
app.include_router(schema.router)

@app.on_event("startup")
async def start_worker():
    asyncio.create_task(iceberg_worker())



@app.get("/")
def root():
    tables_name = ["Order_fullfillment", ]

    return {"message": "API is running",
            "version": "1.0",
            "Tables": tables_name
            }



@app.get("/table/schema")
def table_schema(table_name: str = Query(..., description="Table name")):
    catalog = MysqlCatalog()
    try:
        description = catalog.get_describe(table_name)
        if not description:
            raise HTTPException(
                status_code=404,
                detail={
                    "error_code": "TABLE_NOT_FOUND",
                    "message": f"Table '{table_name}' not found"
                }
            )
        return {"schema": description}

    except Error as e:
        # Database-related error
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "DB_ERROR",
                "message": str(e)
            }
        )
    except Exception as e:
        # Unexpected error
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "BAD_REQUEST",
                "message": str(e)
            }
        )
    finally:
        catalog.close()

