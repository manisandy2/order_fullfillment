from fastapi import APIRouter, HTTPException
from core.catalog_client import get_catalog_client
# from datetime import datetime,timedelta
import datetime
from datetime import datetime, timedelta
import logging
import pyarrow.compute as pc
from core.between_range import MydatabaseRange

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Jobs"])

NAMESPACE = "order_fulfillment"
DB_LIST = [
    "masterorders", 
    # "masterorders_w",
    # "orderlineitems",
    # "pickup_delivery_items",
    # "pickup_delivery_items_w",
    # "status_events"
]

def get_yesterday_end():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0
    )

@router.get("/last-dates")
def get_last_dates():
    """
    Fetch the maximum 'created_at' value for all specified tables in the namespace.
    This loop correctly iterates through all tables and aggregates the results.
    """
    catalog = get_catalog_client()
    results = []
    
    try:
        for table in DB_LIST:
            column = "created_at"
            table_identifier = f"{NAMESPACE}.{table}"
            logger.info(f"Fetching last date for table: {table_identifier}")

            try:
                iceberg_table = catalog.load_table(table_identifier)

                # ---- OPTIMIZED STRATEGY ----
                # Scan only the target column and use PyArrow compute for efficiency
                scan = (
                    iceberg_table.scan(
                        selected_fields=[column]
                    )
                    .to_arrow()
                )

                if scan.num_rows == 0:
                    results.append({
                        "namespace": NAMESPACE,
                        "table": table,
                        "column": column,
                        "last_value": None
                    })
                    continue

                # Compute max using pyarrow directly (much faster than pandas for large columns)
                # .as_py() converts the Arrow scalar to a Python object (datetime, int, etc.)
                last_value = pc.max(scan.column(column)).as_py()
                print("yesterday",last_value- timedelta(days=1))
                end_date = get_yesterday_end()

                logger.info(
                    f"{table} | Iceberg={last_value} | MySQL<= {end_date}"
                )
                print(end_date)
                mydb = MydatabaseRange()
                print(mydb.get_master_order("masterorders",  last_value,end_date))
                results.append({
                    "namespace": NAMESPACE,
                    "table": table,
                    "column": column,
                    "last_value": (
                        last_value.isoformat()
                        if isinstance(last_value, datetime)
                        else str(last_value) if last_value is not None else None
                    )
                })

            except Exception as e:
                logger.error(f"Error processing table {table}: {str(e)}")
                results.append({
                    "namespace": NAMESPACE,
                    "table": table,
                    "column": column,
                    "error": f"Failed to fetch: {str(e)}"
                })

        return {
            "status": "success",
            "namespace": NAMESPACE,
            "data": results
        }

    except Exception as e:
        logger.error(f"Unexpected error in get_last_dates: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
    finally:
        # Best practice to close catalog if the implementation supports it
        try:
            if hasattr(catalog, 'close'):
                catalog.close()
        except Exception:
            pass

if __name__ == "__main__":
    # This allows running the logic as a standalone script for debugging
    print("Running standalone find_last_date logic...")
    # Normally you would initialize the catalog and call get_last_dates()
    # For now, this serves as a placeholder for CLI usage.
