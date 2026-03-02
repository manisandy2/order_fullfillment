from fastapi import FastAPI, Query,Body, HTTPException,UploadFile, File
from core.mysql_client import MysqlCatalog
import logging
from routers import all_routers
from last_value import table

from api import health
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Order Fulfillment Iceberg API",
    version="1.0.0"
)
app.include_router(health.router, tags=["Health"])
app.include_router(table.router)
# app.include_router(tables.router, prefix="/tables", tags=["Tables"])
for router in all_routers:
    app.include_router(router)
# for r in all_routers_table:
#     app.include_router(r)

# app.include_router(bucket.router)
# app.include_router(namespace.router)
# app.include_router(table.router)
# # app.include_router(insert_data.router)
# app.include_router(master_order.router)
# app.include_router(master_order_w.router)
# app.include_router(pickup_delivery_items.router)
# app.include_router(pickup_delivery_items_w.router)
# app.include_router(status_events.router)
# app.include_router(orderlineitems.router)
# app.include_router(bluedart_zone_masters.router)
# app.include_router(courier_masters.router)
# app.include_router(drivers.router)
# app.include_router(exchangeInformations.router)
#
# app.include_router(orderlineitems_test.router)
# app.include_router(filters.router)
# app.include_router(column.router)
# app.include_router(schema.router)

# @app.on_event("startup")
# async def start_worker():
#     asyncio.create_task(iceberg_worker())



@app.get("/")
def root():
    name_space = ["Order_fullfillment", ]
    tables = [
        "masterorders",
        "masterorders_w",
        "pickup_delivery_items",
        "pickup_delivery_items_w",
        "orderlineitems",
        "status_events"
    ]
    return {"message": "API is running",
            "version": "1.0",
            "metadata":{
            "namespace": name_space,
            "table":tables
            },
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

