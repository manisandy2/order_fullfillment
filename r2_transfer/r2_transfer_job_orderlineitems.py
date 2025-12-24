# import requests
# import time
# import math
# import logging
# from datetime import datetime
#
# # ------------------ CONFIGURATION ------------------
#
# API_URL = "http://127.0.0.1:8000/orderlineitems/insert-multi-with-mysql"  # FastAPI endpoint
#
# BATCH_SIZE = 50000
# START_ROWS = 550000
# TOTAL_ROWS = 5559251
#
# MAX_RETRIES = 3
# SLEEP_BETWEEN_BATCHES = 2
#
# SUCCESS_LOG_FILE = "logs/success_orderlineitems_live.log"
# FAILED_LOG_FILE = "logs/error_orderlineitems_live.log"
#
# # ------------------ LOGGING SETUP ------------------
#
# # Success logger
# success_logger = logging.getLogger("success_logger")
# success_handler = logging.FileHandler(SUCCESS_LOG_FILE)
# success_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
# success_handler.setFormatter(success_formatter)
# success_logger.addHandler(success_handler)
# success_logger.setLevel(logging.INFO)
#
# # Failed logger
# failed_logger = logging.getLogger("failed_logger")
# failed_handler = logging.FileHandler(FAILED_LOG_FILE)
# failed_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
# failed_handler.setFormatter(failed_formatter)
# failed_logger.addHandler(failed_handler)
# failed_logger.setLevel(logging.ERROR)
#
# # ------------------ MAIN FUNCTION ------------------
#
# def transfer_batches():
#     session = requests.Session()
#     start = START_ROWS
#     # print(start)
#     batch_no = 1
#     total_batches = math.ceil(TOTAL_ROWS / BATCH_SIZE)
#     success_batches = 0
#     failed_batches = 0
#
#     success_logger.info(f"Starting R2 Catalog Data Transfer")
#     success_logger.info(f"Total Rows: {TOTAL_ROWS:,} | Batch Size: {BATCH_SIZE:,} | Total Batches: {total_batches}")
#
#     while start < TOTAL_ROWS:
#         end = min(start + BATCH_SIZE, TOTAL_ROWS)
#         range_info = f"Rows {start:,}–{end:,}"
#         print(f" Processing Batch {batch_no}/{total_batches} → {range_info}")
#
#         success = False
#
#         for attempt in range(1, MAX_RETRIES + 1):
#             try:
#                 batch_start_time = time.time()
#                 response = session.post(API_URL, params={"start_range": start, "end_range": end}, timeout=1800)
#
#                 if response.status_code == 200:
#                     result = response.json()
#                     elapsed = round(time.time() - batch_start_time, 2)
#                     success_logger.info(f" Batch {batch_no} Success | {range_info} | Rows Written: {result.get('rows_written')} | Time: {elapsed}s")
#                     print(f"Batch {batch_no} Completed in {elapsed}s")
#                     success = True
#                     success_batches += 1
#                     break
#                 else:
#                     print(f" Batch {batch_no} Failed (Attempt {attempt}) | HTTP {response.status_code}")
#                     success_logger.warning(f"️ Batch {batch_no} Failed | {range_info} | Attempt {attempt} | HTTP {response.status_code}")
#             except Exception as e:
#                 print(f" Batch {batch_no} Error (Attempt {attempt}): {str(e)}")
#                 failed_logger.error(f" Batch {batch_no} Error | {range_info} | Attempt {attempt} | Error: {str(e)}")
#             time.sleep(5)
#
#         if not success:
#             failed_logger.error(f" Batch {batch_no} permanently failed after {MAX_RETRIES} retries | {range_info}")
#             print(f" Batch {batch_no} permanently failed. Skipping...")
#             failed_batches += 1
#
#         start += BATCH_SIZE
#         batch_no += 1
#         time.sleep(SLEEP_BETWEEN_BATCHES)
#
#     summary_msg = f" Transfer Completed | Success: {success_batches} | Failed: {failed_batches}"
#     print(summary_msg)
#     success_logger.info(summary_msg)
#     failed_logger.info(summary_msg)
#
#
# # ------------------ EXECUTE ------------------
#
# if __name__ == "__main__":
#     transfer_batches()

import requests
import time
import logging
from datetime import datetime, timedelta

# ------------------ CONFIGURATION ------------------

API_URL = "http://127.0.0.1:8000/orderlineitems-date-range/insert-multi-with-mysql"

START_DATE = "2025-12-12"
END_DATE = "2025-12-23"

CHUNK_SIZE = 10000
MAX_RETRIES = 3
SLEEP_BETWEEN_BATCHES = 2

# SUCCESS_LOG_FILE = "logs/success_status_events_live.log"
# FAILED_LOG_FILE = "logs/error_master_events_live.log"

SUCCESS_LOG_FILE = "logs/success_orderlineitems_live.log"
FAILED_LOG_FILE = "logs/error_orderlineitems_live.log"

# ------------------ LOGGING SETUP ------------------

logging.basicConfig(level=logging.INFO)

success_logger = logging.getLogger("success_logger")
success_logger.addHandler(logging.FileHandler(SUCCESS_LOG_FILE))

failed_logger = logging.getLogger("failed_logger")
failed_logger.addHandler(logging.FileHandler(FAILED_LOG_FILE))

# ------------------ MAIN FUNCTION ------------------

def daterange(start_date, end_date):
    current = start_date
    while current < end_date:
        yield current
        current += timedelta(days=1)

def transfer_by_date():
    session = requests.Session()

    start_dt = datetime.fromisoformat(START_DATE)
    end_dt = datetime.fromisoformat(END_DATE)

    batch_no = 1
    success_batches = 0
    failed_batches = 0

    for day in daterange(start_dt, end_dt):
        batch_start = day.strftime("%Y-%m-%d 00:00:00")
        batch_end = (day + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        print(f" Processing Batch {batch_no} | {batch_start} → {batch_end}")

        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                t0 = time.time()

                response = session.post(
                    API_URL,
                    params={
                        "start_date": batch_start,
                        "end_date": batch_end,
                        "chunk_size": CHUNK_SIZE
                    },
                    timeout=1800
                )

                if response.status_code == 200:
                    elapsed = round(time.time() - t0, 2)
                    success_logger.info(
                        f"Batch {batch_no} Success | {batch_start} → {batch_end} | Time: {elapsed}s"
                    )
                    print(f" Batch {batch_no} Completed in {elapsed}s")
                    success = True
                    success_batches += 1
                    break
                else:
                    failed_logger.error(
                        f"Batch {batch_no} Failed | HTTP {response.status_code} | {response.text}"
                    )

            except Exception as e:
                failed_logger.error(
                    f"Batch {batch_no} Error | Attempt {attempt} | {str(e)}"
                )

            time.sleep(5)

        if not success:
            failed_batches += 1
            failed_logger.error(
                f"Batch {batch_no} permanently failed | {batch_start} → {batch_end}"
            )

        batch_no += 1
        time.sleep(SLEEP_BETWEEN_BATCHES)

    summary = f"Transfer Completed | Success: {success_batches} | Failed: {failed_batches}"
    print(summary)
    success_logger.info(summary)
    failed_logger.info(summary)

# ------------------ EXECUTE ------------------

if __name__ == "__main__":
    transfer_by_date()