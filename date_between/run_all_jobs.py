import logging
import os
import sys

# Ensure we can import modules if running as a script
sys.path.append(os.getcwd())

from date_between import (
    bluedart_zone_masters,
    courier_masters,
    drivers,
    exchange_informations,
    exchange_masterorders,
    exchange_masterorders_w,
    exchange_orderlineitems,
    externalcalllogs,
    hub_masters,
    installation_services,
    intransit_manifests,
    intransit_pickup_delivery_items,
    intransit_shipments,
    invoice_masters,
    manifests,
    master_order,
    master_order_W,
    orderlineitems,
    pick_lists,
    pickup_deliveries,
    pickup_delivery_items,
    pickup_delivery_items_W,

    reason_messages,
    return_masterorders,
    return_masterorders_w,
    return_orderlineitems,
    roles,
    scheduler_retention_log,
    schedulers,
    schedulers_w,
    service_history_c,
    service_history_h,
    service_master_c,
    service_master_h,
    shipments,
    status_events,
    users,
    vehicles

)

# Use a local log file to avoid PermissionError in /var/log
log_file = os.path.join(os.getcwd(), "ingestion_all.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

JOBS = [
    ("masterorders", master_order.run),
    ("masterorders_w", master_order_W.run),
    ("pickup_delivery_items", pickup_delivery_items.run),
    ("pickup_delivery_items_w", pickup_delivery_items_W.run), 
    ("orderlineitems", orderlineitems.run),
    ("status_events", status_events.run),
    ("bluedart_zone_masters", bluedart_zone_masters.run),
    ("exchange_informations", exchange_informations.run),
]

if __name__ == "__main__":
    print(f"Starting ingestion jobs. Logs: {log_file}")
    for name, job in JOBS:
        try:
            logging.info(f"JOB START | {name}")
            print(f"Running {name}...")
            result = job()
            logging.info(f"JOB SUCCESS | {name} | {result}")
            print(f"SUCCESS {name}")
        except Exception as e:
            logging.exception(f"JOB FAILED | {name} | {str(e)}")
            print(f"FAILED {name}: {e}")