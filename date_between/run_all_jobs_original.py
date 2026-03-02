import logging
import os
import sys
import psutil
import time
import gc
from date_between.utility import get_memory_mb
import subprocess
# Ensure we can import modules if running as a script
sys.path.append(os.getcwd())

from date_between import (
    bluedart_zone_masters,
    courier_masters,
    drivers,
    drivers_dob_error,
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
    # ("bluedart_zone_masters", bluedart_zone_masters.run),
    # ("courier_masters", courier_masters.run),
    # ("drivers", drivers.run),
    # ("drivers_dob_error", drivers_dob_error.run),
    # ("exchange_informations", exchange_informations.run),
    # ("exchange_masterorders", exchange_masterorders.run),
    # ("exchange_masterorders_w", exchange_masterorders_w.run),
    # ("exchange_orderlineitems", exchange_orderlineitems.run),
    # ("externalcalllogs", externalcalllogs.run),
    # ("hub_masters", hub_masters.run),
    # ("installation_services", installation_services.run),
    # ("intransit_manifests", intransit_manifests.run),
    # ("intransit_pickup_delivery_items", intransit_pickup_delivery_items.run),
    # ("intransit_shipments", intransit_shipments.run),
    # ("invoice_masters", invoice_masters.run),
    # ("manifests", manifests.run),
    # ("masterorders", master_order.run),
    # ("masterorders_w", master_order_W.run),
    # ("orderlineitems", orderlineitems.run),
    # ("pick_lists", pick_lists.run),
    # ("pickup_deliveries", pickup_deliveries.run),
    # ("pickup_delivery_items", pickup_delivery_items.run),
    # ("pickup_delivery_items_w", pickup_delivery_items_W.run),
    # ("reason_messages", reason_messages.run),
    # ("return_masterorders", return_masterorders.run),
    # ("return_masterorders_w", return_masterorders_w.run),
    # ("return_orderlineitems", return_orderlineitems.run),
    # ("roles", roles.run),
    # ("scheduler_retention_log", scheduler_retention_log.run),
    # ("schedulers", schedulers.run),
    # ("schedulers_w", schedulers_w.run),
    # ("service_history_c", service_history_c.run),
    # ("service_history_h", service_history_h.run),
    # ("service_master_c", service_master_c.run),
    # ("service_master_h", service_master_h.run),
    # ("shipments", shipments.run),
    ("status_events", status_events.run),
    # ("users", users.run),
    # ("vehicles", vehicles.run),


]

if __name__ == "__main__":
    print(f"Starting ingestion jobs. Logs: {log_file}")
    for name, job in JOBS:
        try:
            mem_before = get_memory_mb()
            start_time = time.time()

            print(f"Running {name} | Memory Before: {mem_before} MB")
            logging.info(f"START | {name} | MEM: {mem_before} MB")
            # print(f"Running {name}...")
            result = job()
            gc.collect()

            mem_after = get_memory_mb()
            end_time = time.time()
            duration = round(time.time() - start_time, 2)
            print(f"SUCCESS {name} | result: {result} | Time: {duration}s | Memory After: {mem_after} MB")
            logging.info(f"SUCCESS | {name} | TIME: {duration}s | MEM: {mem_after} MB")
        except Exception as e:
            logging.exception(f"JOB FAILED | {name} | {str(e)}")
            print(f"FAILED {name}: {e}")

# if __name__ == "__main__":
#     print(f"Starting ingestion jobs. Logs: {log_file}")
#     for name, job in JOBS:
#         mem_before = get_memory_mb()
#         start_time = time.time()
#
#         try:
#             print(f"Running {name} | Memory Before: {mem_before} MB")
#             logging.info(f"START | {name} | MEM_BEFORE: {mem_before} MB")
#
#             result = job()
#
#             status = "SUCCESS"
#
#             # status = "SUCCESS"
#
#             logging.info(f"JOB SUCCESS | {name} | {result}")
#             print(f"JOB SUCCESS | {name} | {result}")
#             # rows_failed = result.get("rows_failed", 0)
#             # rows_fetched = result.get("rows_fetched", 0)
#
#         except Exception as e:
#             logging.exception(f"FAILED | {name} | {str(e)}")
#             print(f"FAILED {name}: {e}")
#             status = "FAILED"
#
#         finally:
#             gc.collect()  # ALWAYS cleanup
#
#             mem_after = get_memory_mb()
#             duration = round(time.time() - start_time, 2)
#
#             print(
#                 f"{status} {name} | "
#                 # f"Fetched: {rows_fetched} | "
#                 # f"Success: {rows_success} | "
#                 # f"Failed: {rows_failed} | "
#                 f"Time: {duration}s | "
#                 f"Memory After: {mem_after} MB"
#             )
#             print("-" * 60)

# if __name__ == "__main__":
#     print(f"Starting ingestion jobs. Logs: {log_file}")
#
#     for name, job in JOBS:   # 👈 ignore function
#
#         mem_before = get_memory_mb()
#         start_time = time.time()
#
#         try:
#             print(f"Running {name} | Memory Before: {mem_before} MB")
#             logging.info(f"START | {name} | MEM_BEFORE: {mem_before} MB")
#             result = job()
#             # 🔥 Run job in separate process
#             subprocess.run(
#                 [
#                     sys.executable,      # current python interpreter
#                     "-m",
#                     f"date_between.{name}"   # module name
#                 ],
#                 check=True
#             )
#
#             status = "SUCCESS"
#
#         except subprocess.CalledProcessError as e:
#             logging.exception(f"FAILED | {name} | {str(e)}")
#             print(f"FAILED {name}: {e}")
#             status = "FAILED"
#
#         finally:
#             gc.collect()
#
#             mem_after = get_memory_mb()
#             duration = round(time.time() - start_time, 2)
#
#             print(
#                 f"{status} {name} | "
#                 f"Time: {duration}s | "
#                 f"Memory After: {mem_after} MB"
#             )
#             print("-" * 60)
# new fun
####################################
# if __name__ == "__main__":
#
#     print("Parent PID:", os.getpid())
#     print("Starting ingestion jobs...\n")
#
#     for name in JOBS:
#
#         mem_before = get_memory_mb()
#         start_time = time.time()
#
#         print(f"Running {name} | Memory Before: {mem_before} MB")
#         try:
#             # 🔥 Run job in separate process
#             subprocess.run(
#                 [sys.executable, "-m", f"date_between.{name}"],
#                 check=True
#             )
#             print(f"Running {name} | Memory After: {mem_before} MB")
#         except subprocess.CalledProcessError as e:
#             print(f"FAILED {name}: {e}")
#             status = "FAILED"
#
#         duration = round(time.time() - start_time, 2)
#         mem_after = get_memory_mb()
#
#         print(
#             f"SUCCESS {name} | "
#             f"Time: {duration}s | "
#             f"Memory After: {mem_after} MB"
#         )
#         print("-" * 60)

