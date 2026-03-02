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
LOG_FILE = os.path.join(os.getcwd(), "ingestion_all.log")

# ==============================
# LOGGING
# ==============================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

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
    "masterorders",
    "masterorders_w",
    "orderlineitems",
    "pick_lists",
    "pickup_deliveries",
    "pickup_delivery_items",
    "pickup_delivery_items_w",
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
def run_job(module_name: str):
    print(f"\n🚀 Running: {module_name}")
    logging.info(f"START | {module_name}")

    mem_before = get_memory_mb()
    start_time = time.time()

    try:
        subprocess.run(
            [sys.executable, "-m", f"date_between.{module_name}"],
            check=True,
            timeout=TIMEOUT_SECONDS
        )

        status = "SUCCESS"

    except subprocess.TimeoutExpired:
        status = "TIMEOUT"
        logging.error(f"{module_name} exceeded {TIMEOUT_SECONDS} seconds.")
        print(f"⛔ TIMEOUT: {module_name}")

    except subprocess.CalledProcessError as e:
        status = "FAILED"
        logging.exception(f"{module_name} failed: {e}")
        print(f"❌ FAILED: {module_name}")

    duration = round(time.time() - start_time, 2)
    mem_after = get_memory_mb()

    print(
        f"✅ {status} | {module_name} | "
        f"Time: {duration}s | "
        f"Memory Before: {mem_before} MB | "
        f"Memory After: {mem_after} MB"
    )

    logging.info(
        f"{status} | {module_name} | "
        f"Time: {duration}s | "
        f"MEM_BEFORE: {mem_before} MB | "
        f"MEM_AFTER: {mem_after} MB"
    )

# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    print("===========================================")
    print("🔥 Iceberg Ingestion Job Runner Started")
    print("Parent PID:", os.getpid())
    print("Log File:", LOG_FILE)
    print("===========================================")

    total_start = time.time()

    for job in JOBS:
        run_job(job)

    total_duration = round(time.time() - total_start, 2)

    print("\n===========================================")
    print(f"🏁 ALL JOBS COMPLETED | Total Time: {total_duration}s")
    print("===========================================")