import os
import sys
import time
import logging
import subprocess
import psutil
# ==============================
# CONFIG
# ==============================

TIMEOUT_SECONDS = 300   # 5 minutes per job

# ==============================
# LOGGING
# ==============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)
# ==============================
# MEMORY UTILITY
# ==============================

def get_memory_mb():
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)

# ==============================
# JOB LIST (Module Names)
# ==============================

JOBS = [
    "bluedart_zone_masters",
    "courier_masters",
    "drivers",
    "drivers_dob_error",
    "exchange_informations",
    "exchange_masterorders",
    "exchange_masterorders_w",
    "exchange_orderlineitems",
    "externalcalllogs",
    "hub_masters",
    "installation_services",
    "intransit_manifests",
    "intransit_pickup_delivery_items",
    "intransit_shipments",
    "invoice_masters",
    "manifests",
    "master_order",
    "master_order_W",
    "orderlineitems",
    "pick_lists",
    "pickup_deliveries",
    "pickup_delivery_items",
    "pickup_delivery_items_W",
    "reason_messages",
    "return_masterorders",
    "return_masterorders_w",
    "return_orderlineitems",
    "roles",
    "scheduler_retention_log",
    "schedulers",
    "schedulers_w",
    "service_history_c",
    "service_history_h",
    "service_master_c",
    "service_master_h",
    "shipments",
    "status_events",
    "users",
    "vehicles",
]
# -------------------------------
# Example Table Processing
# -------------------------------
# def process_table(table_name):
#     """
#     Process a single table.
#     Replace this with your real logic.
#     """
#
#     logger.info(f"Processing table: {table_name}")
#
#     # Example failure simulation
#     if table_name == "bad_table":
#         raise Exception("Schema mismatch detected")
#
#     time.sleep(1)
#
#     logger.info(f"Completed table: {table_name}")


def run_job(module_name: str):
    # env = os.environ.copy()
    start_time = time.time()
    mem_before = get_memory_mb()
    status = "SUCCESS"

    try:
        subprocess.run(
            [sys.executable, "-m", f"date_between.{module_name}"],
            check=True,
            timeout=TIMEOUT_SECONDS,
            # env=env,
        )

    except subprocess.TimeoutExpired:
        status = "TIMEOUT"
        logging.error(f"{module_name} exceeded {TIMEOUT_SECONDS} seconds.")

    except subprocess.CalledProcessError as e:
        status = "FAILED"
        logging.exception(f"{module_name} failed: {e}")

    duration = round(time.time() - start_time, 2)
    mem_after = get_memory_mb()

    message = (
        f"{status} | {module_name} | "
        f"Time: {duration}s | "
        f"Memory Before: {mem_before} MB | "
        f"Memory After: {mem_after} MB"
    )

    logging.info(message)
    return status

# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    print("===========================================")
    print("Iceberg Ingestion Job Runner Started")
    print("===========================================")

    total_start = time.time()

    success = 0
    failed = 0

    for job in JOBS:
        result = run_job(job)

        if result == "SUCCESS":
            success += 1
        else:
            failed += 1

    total_duration = round(time.time() - total_start, 2)

    print("\n===========================================")
    print(f"ALL JOBS COMPLETED | Total Time: {total_duration}s")
    print(f"Success: {success}")
    print(f"Failed: {failed}")
    print(f"Total Time: {total_duration}s")
    print("===========================================")

    if failed > 0:
        sys.exit(1)
    print("===========================================")