# import requests
# import time
# import math
# import logging
# from datetime import datetime
#
# # ------------------ CONFIGURATION ------------------
#
# API_URL = "http://127.0.0.1:8000/pickup-delivery-items/insert-multi-with-mysql"  # FastAPI endpoint
#
# BATCH_SIZE = 50000
# START_ROWS = 0
# TOTAL_ROWS = 361590
#
#
#
# MAX_RETRIES = 3
# SLEEP_BETWEEN_BATCHES = 2
#
# SUCCESS_LOG_FILE = "logs/success_pickup-delivery-items-live.log"
# FAILED_LOG_FILE = "logs/error_pickup-delivery-items-live.log"
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
###############################################################

# import requests
# import time
# import logging
# from datetime import datetime, timedelta

# # ------------------ CONFIGURATION ------------------

# API_URL = "http://127.0.0.1:8000/pickup-delivery-items-date-range/insert-multi-with-mysql"

# START_DATE = "2025-11-29T00:00:00"
# END_DATE = "2026-01-01T23:59:59"

# CHUNK_SIZE = 10000
# MAX_RETRIES = 3
# SLEEP_BETWEEN_BATCHES = 2

# SUCCESS_LOG_FILE = "logs/success-pickup-delivery-items-live.log"
# FAILED_LOG_FILE = "logs/error-pickup-delivery-items-live.log"

# # ------------------ LOGGING SETUP ------------------

# logging.basicConfig(level=logging.INFO)

# success_logger = logging.getLogger("success_logger")
# success_logger.addHandler(logging.FileHandler(SUCCESS_LOG_FILE))

# failed_logger = logging.getLogger("failed_logger")
# failed_logger.addHandler(logging.FileHandler(FAILED_LOG_FILE))

# # ------------------ MAIN FUNCTION ------------------

# def daterange(start_date, end_date):
#     current = start_date
#     while current < end_date:
#         yield current
#         current += timedelta(days=1)

# def transfer_by_date():
#     session = requests.Session()

#     start_dt = datetime.fromisoformat(START_DATE)
#     end_dt = datetime.fromisoformat(END_DATE)

#     batch_no = 1
#     success_batches = 0
#     failed_batches = 0

#     for day in daterange(start_dt, end_dt):
#         batch_start = day.strftime("%Y-%m-%d 00:00:00")
#         batch_end = (day + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

#         print(f" Processing Batch {batch_no} | {batch_start} → {batch_end}")

#         success = False

#         for attempt in range(1, MAX_RETRIES + 1):
#             try:
#                 t0 = time.time()

#                 response = session.post(
#                     API_URL,
#                     params={
#                         "start_date": batch_start,
#                         "end_date": batch_end,
#                         "chunk_size": CHUNK_SIZE
#                     },
#                     timeout=1800
#                 )

#                 if response.status_code == 200:
#                     elapsed = round(time.time() - t0, 2)
#                     success_logger.info(
#                         f"Batch {batch_no} Success | {batch_start} → {batch_end} | Time: {elapsed}s"
#                     )
#                     print(f" Batch {batch_no} Completed in {elapsed}s")
#                     success = True
#                     success_batches += 1
#                     break
#                 else:
#                     failed_logger.error(
#                         f"Batch {batch_no} Failed | HTTP {response.status_code} | {response.text}"
#                     )

#             except Exception as e:
#                 failed_logger.error(
#                     f"Batch {batch_no} Error | Attempt {attempt} | {str(e)}"
#                 )

#             time.sleep(5)

#         if not success:
#             failed_batches += 1
#             failed_logger.error(
#                 f"Batch {batch_no} permanently failed | {batch_start} → {batch_end}"
#             )

#         batch_no += 1
#         time.sleep(SLEEP_BETWEEN_BATCHES)

#     summary = f"Transfer Completed | Success: {success_batches} | Failed: {failed_batches}"
#     print(summary)
#     success_logger.info(summary)
#     failed_logger.info(summary)

# # ------------------ EXECUTE ------------------

# if __name__ == "__main__":
#     transfer_by_date()

################################################################
# import requests
# import time
# import logging
# import os
# import json
# import signal
# import sys
# from datetime import datetime, timedelta
# from pathlib import Path
# from typing import Optional
#
# data = ["url",{
#             "masterorder_url": "http://127.0.0.1:8000/masterorder-date-range/insert-master-with-mysql",
#             "masterorder_url_w": "http://127.0.0.1:8000/masterorder-w-date-range/insert-master-with-mysql",
#             "pickup-delivery": "http://127.0.0.1:8000/pickup-delivery-w-items-date-range/insert-multi-with-mysql",
#             "pickup-delivery-w": "http://127.0.0.1:8000/pickup-delivery-w-items-date-range/insert-multi-with-mysql",
#             "orderlineitems": "http://127.0.0.1:8000/orderlineitems-date-range/insert-multi-with-mysql",
#             "status-events": "http://127.0.0.1:8000/status-events-date-range/insert-multi-with-mysql",
#             },
#         "logs",{
#             "success_log_file": "logs/success-pickup-delivery-items-live.log",
#             "failed_log_file": "logs/error-pickup-delivery-items-live.log",
#         }
# ]
# # ------------------ CONFIGURATION ------------------
# # API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/pickup-delivery-items-date-range/insert-multi-with-mysql")
# START_DATE = os.getenv("START_DATE", "2025-11-28T00:00:00")
# END_DATE = os.getenv("END_DATE", "2026-01-01T23:59:59")
# CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
# MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
# SLEEP_BETWEEN_BATCHES = int(os.getenv("SLEEP_BETWEEN_BATCHES", "2"))
# REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "600"))  # 10 minutes
# LOG_DIR = Path("logs")
# SUCCESS_LOG_FILE = LOG_DIR / "success-pickup-delivery-items-live.log"
# FAILED_LOG_FILE = LOG_DIR / "error-pickup-delivery-items-live.log"
# PROGRESS_FILE = LOG_DIR / "transfer-progress.json"
# # ------------------ LOGGING SETUP ------------------
# # Create log directory
# LOG_DIR.mkdir(exist_ok=True)
# # Configure formatters
# formatter = logging.Formatter(
#     "%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S"
# )
# # Success logger
# success_logger = logging.getLogger("success_logger")
# success_logger.setLevel(logging.INFO)
# success_logger.propagate = False  # Prevent duplicate logs
# success_handler = logging.FileHandler(SUCCESS_LOG_FILE)
# success_handler.setFormatter(formatter)
# success_logger.addHandler(success_handler)
# # Failed logger
# failed_logger = logging.getLogger("failed_logger")
# failed_logger.setLevel(logging.ERROR)
# failed_logger.propagate = False
# failed_handler = logging.FileHandler(FAILED_LOG_FILE)
# failed_handler.setFormatter(formatter)
# failed_logger.addHandler(failed_handler)
# # Console logger
# console_logger = logging.getLogger("console")
# console_logger.setLevel(logging.INFO)
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# console_logger.addHandler(console_handler)
# # ------------------ PROGRESS TRACKING ------------------
# def save_progress(last_successful_date: str, stats: dict):
#     """Save progress to file for resume capability."""
#     progress = {
#         "last_successful_date": last_successful_date,
#         "timestamp": datetime.now().isoformat(),
#         "stats": stats
#     }
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump(progress, f, indent=2)
# def load_progress() -> Optional[dict]:
#     """Load progress from file if exists."""
#     if PROGRESS_FILE.exists():
#         with open(PROGRESS_FILE, "r") as f:
#             return json.load(f)
#     return None
# # ------------------ SIGNAL HANDLING ------------------
# shutdown_requested = False
# def signal_handler(signum, frame):
#     global shutdown_requested
#     console_logger.warning("Shutdown signal received. Finishing current batch...")
#     shutdown_requested = True
# signal.signal(signal.SIGINT, signal_handler)
# signal.signal(signal.SIGTERM, signal_handler)
# # ------------------ HELPER FUNCTIONS ------------------
# def daterange(start_date: datetime, end_date: datetime):
#     """Generate date range day by day."""
#     current = start_date
#     while current < end_date:
#         yield current
#         current += timedelta(days=1)
# def should_retry(status_code: int) -> bool:
#     """Determine if request should be retried based on status code."""
#     # Retry on server errors and rate limiting
#     return status_code >= 500 or status_code == 429
# def exponential_backoff(attempt: int, base_delay: int = 5) -> int:
#     """Calculate exponential backoff delay."""
#     return min(base_delay * (2 ** (attempt - 1)), 60)  # Max 60 seconds
# # ------------------ MAIN FUNCTION ------------------
# def transfer_by_date(resume: bool = True):
#     """
#     Transfer data by date range with retry logic and progress tracking.
#
#     Args:
#         resume: If True, resume from last successful date
#     """
#     session = requests.Session()
#
#     try:
#         start_dt = datetime.fromisoformat(START_DATE)
#         end_dt = datetime.fromisoformat(END_DATE)
#
#         # Resume from last successful date if available
#         if resume:
#             progress = load_progress()
#             if progress:
#                 last_date = datetime.fromisoformat(progress["last_successful_date"])
#                 start_dt = last_date + timedelta(days=1)
#                 console_logger.info(f"Resuming from {start_dt.date()}")
#
#         batch_no = 1
#         success_batches = 0
#         failed_batches = 0
#         total_rows_written = 0
#
#         console_logger.info(f"Starting transfer: {start_dt.date()} → {end_dt.date()}")
#
#         for day in daterange(start_dt, end_dt):
#             if shutdown_requested:
#                 console_logger.warning("Shutdown requested. Stopping...")
#                 break
#
#             batch_start = day.strftime("%Y-%m-%d 00:00:00")
#             batch_end = (day + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
#
#             console_logger.info(f"Processing Batch {batch_no} | {batch_start} → {batch_end}")
#
#             success = False
#             rows_written = 0
#
#             for attempt in range(1, MAX_RETRIES + 1):
#                 try:
#                     t0 = time.time()
#
#                     response = session.post(
#                         API_URL,
#                         params={
#                             "start_date": batch_start,
#                             "end_date": batch_end,
#                             "chunk_size": CHUNK_SIZE
#                         },
#                         timeout=REQUEST_TIMEOUT
#                     )
#
#                     if response.status_code == 200:
#                         elapsed = round(time.time() - t0, 2)
#
#                         # Parse response data
#                         try:
#                             result = response.json()
#                             rows_written = result.get("rows_written", 0)
#                             total_rows_written += rows_written
#                         except json.JSONDecodeError:
#                             rows_written = 0
#
#                         success_logger.info(
#                             f"Batch {batch_no} Success | {batch_start} → {batch_end} | "
#                             f"Rows: {rows_written} | Time: {elapsed}s"
#                         )
#                         console_logger.info(
#                             f"✓ Batch {batch_no} Completed | Rows: {rows_written} | {elapsed}s"
#                         )
#
#                         success = True
#                         success_batches += 1
#
#                         # Save progress
#                         save_progress(batch_start, {
#                             "success_batches": success_batches,
#                             "failed_batches": failed_batches,
#                             "total_rows_written": total_rows_written
#                         })
#
#                         break
#
#                     elif should_retry(response.status_code):
#                         # Retryable error
#                         delay = exponential_backoff(attempt)
#                         failed_logger.warning(
#                             f"Batch {batch_no} HTTP {response.status_code} | "
#                             f"Attempt {attempt}/{MAX_RETRIES} | Retrying in {delay}s"
#                         )
#                         time.sleep(delay)
#
#                     else:
#                         # Non-retryable error (4xx)
#                         error_msg = response.text[:500]  # Truncate
#                         failed_logger.error(
#                             f"Batch {batch_no} Failed | HTTP {response.status_code} | "
#                             f"Non-retryable error: {error_msg}"
#                         )
#                         break  # Don't retry
#
#                 except requests.exceptions.Timeout:
#                     delay = exponential_backoff(attempt)
#                     failed_logger.error(
#                         f"Batch {batch_no} Timeout | Attempt {attempt}/{MAX_RETRIES} | "
#                         f"Retrying in {delay}s"
#                     )
#                     time.sleep(delay)
#
#                 except requests.exceptions.RequestException as e:
#                     delay = exponential_backoff(attempt)
#                     failed_logger.error(
#                         f"Batch {batch_no} Network Error | Attempt {attempt}/{MAX_RETRIES} | "
#                         f"{str(e)} | Retrying in {delay}s"
#                     )
#                     time.sleep(delay)
#
#                 except Exception as e:
#                     failed_logger.error(
#                         f"Batch {batch_no} Unexpected Error | Attempt {attempt} | {str(e)}"
#                     )
#                     time.sleep(exponential_backoff(attempt))
#
#             if not success:
#                 failed_batches += 1
#                 failed_logger.error(
#                     f"Batch {batch_no} permanently failed after {MAX_RETRIES} retries | "
#                     f"{batch_start} → {batch_end}"
#                 )
#                 console_logger.error(f"✗ Batch {batch_no} Failed")
#
#             batch_no += 1
#             time.sleep(SLEEP_BETWEEN_BATCHES)
#
#         # Final summary
#         summary = (
#             f"Transfer Completed | Success: {success_batches} | Failed: {failed_batches} | "
#             f"Total Rows: {total_rows_written:,}"
#         )
#         console_logger.info(summary)
#         success_logger.info(summary)
#
#         if failed_batches > 0:
#             failed_logger.info(summary)
#             sys.exit(1)  # Exit with error code if any failures
#
#     finally:
#         session.close()
# # ------------------ EXECUTE ------------------
# if __name__ == "__main__":
#     import argparse
#
#     parser = argparse.ArgumentParser(description="Transfer pickup delivery items to R2")
#     parser.add_argument("--no-resume", action="store_true", help="Start from beginning, ignore progress")
#     parser.add_argument("--dry-run", action="store_true", help="Simulate without actual transfer")
#
#     args = parser.parse_args()
#
#     if args.dry_run:
#         console_logger.info("DRY RUN MODE - No data will be transferred")
#         # TODO: Implement dry run logic
#
#     transfer_by_date(resume=not args.no_resume)
###########################################################
import requests
import time
import logging
import os
import json
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/pickup-delivery-items-date-range/insert-multi-with-mysql")
START_DATE = os.getenv("START_DATE", "2026-01-02T00:00:00")
END_DATE = os.getenv("END_DATE", "2026-01-05T23:59:59")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
SLEEP_BETWEEN_BATCHES = int(os.getenv("SLEEP_BETWEEN_BATCHES", "2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "600"))  # 10 minutes
LOG_DIR = Path("logs")
SUCCESS_LOG_FILE = LOG_DIR / "success-pickup-delivery-items-live.log"
FAILED_LOG_FILE = LOG_DIR / "error-pickup-delivery-items-live.log"
# PROGRESS_FILE = LOG_DIR / "transfer-progress.json"
# ------------------ LOGGING SETUP ------------------
# Create log directory
LOG_DIR.mkdir(exist_ok=True)
# Configure formatters
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# Success logger
success_logger = logging.getLogger("success_logger")
success_logger.setLevel(logging.INFO)
success_logger.propagate = False  # Prevent duplicate logs
success_handler = logging.FileHandler(SUCCESS_LOG_FILE)
success_handler.setFormatter(formatter)
success_logger.addHandler(success_handler)
# Failed logger
failed_logger = logging.getLogger("failed_logger")
failed_logger.setLevel(logging.ERROR)
failed_logger.propagate = False
failed_handler = logging.FileHandler(FAILED_LOG_FILE)
failed_handler.setFormatter(formatter)
failed_logger.addHandler(failed_handler)
# Console logger
console_logger = logging.getLogger("console")
console_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_logger.addHandler(console_handler)


# ------------------ SIGNAL HANDLING ------------------
shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    console_logger.warning("Shutdown signal received. Finishing current batch...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ------------------ HELPER FUNCTIONS ------------------
def daterange(start_date: datetime, end_date: datetime):
    """Generate date range day by day."""
    current = start_date
    while current < end_date:
        yield current
        current += timedelta(days=1)


def should_retry(status_code: int) -> bool:
    """Determine if request should be retried based on status code."""
    # Retry on server errors and rate limiting
    return status_code >= 500 or status_code == 429


def exponential_backoff(attempt: int, base_delay: int = 5) -> int:
    """Calculate exponential backoff delay."""
    return min(base_delay * (2 ** (attempt - 1)), 60)  # Max 60 seconds


# ------------------ MAIN FUNCTION ------------------
def transfer_by_date(resume: bool = True):
    """
    Transfer data by date range with retry logic and progress tracking.

    Args:
        resume: If True, resume from last successful date
    """
    session = requests.Session()

    try:
        start_dt = datetime.fromisoformat(START_DATE)
        end_dt = datetime.fromisoformat(END_DATE)

        batch_no = 1
        success_batches = 0
        failed_batches = 0
        total_rows_written = 0

        console_logger.info(f"Starting transfer: {start_dt.date()} → {end_dt.date()}")

        for day in daterange(start_dt, end_dt):
            if shutdown_requested:
                console_logger.warning("Shutdown requested. Stopping...")
                break

            batch_start = day.strftime("%Y-%m-%d 00:00:00")
            batch_end = (day + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

            console_logger.info(f"Processing Batch {batch_no} | {batch_start} → {batch_end}")

            success = False
            rows_written = 0

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
                        timeout=REQUEST_TIMEOUT
                    )

                    if response.status_code == 200:
                        elapsed = round(time.time() - t0, 2)

                        # Parse response data
                        try:
                            result = response.json()
                            rows_written = result.get("rows_written", 0)
                            total_rows_written += rows_written
                        except json.JSONDecodeError:
                            rows_written = 0

                        success_logger.info(
                            f"Batch {batch_no} Success | {batch_start} → {batch_end} | "
                            f"Rows: {rows_written} | Time: {elapsed}s"
                        )
                        console_logger.info(
                            f"✓ Batch {batch_no} Completed | Rows: {rows_written} | {elapsed}s"
                        )

                        success = True
                        success_batches += 1

                        break

                    elif should_retry(response.status_code):
                        # Retryable error
                        delay = exponential_backoff(attempt)
                        failed_logger.warning(
                            f"Batch {batch_no} HTTP {response.status_code} | "
                            f"Attempt {attempt}/{MAX_RETRIES} | Retrying in {delay}s"
                        )
                        time.sleep(delay)

                    else:
                        # Non-retryable error (4xx)
                        error_msg = response.text[:500]  # Truncate
                        failed_logger.error(
                            f"Batch {batch_no} Failed | HTTP {response.status_code} | "
                            f"Non-retryable error: {error_msg}"
                        )
                        break  # Don't retry

                except requests.exceptions.Timeout:
                    delay = exponential_backoff(attempt)
                    failed_logger.error(
                        f"Batch {batch_no} Timeout | Attempt {attempt}/{MAX_RETRIES} | "
                        f"Retrying in {delay}s"
                    )
                    time.sleep(delay)

                except requests.exceptions.RequestException as e:
                    delay = exponential_backoff(attempt)
                    failed_logger.error(
                        f"Batch {batch_no} Network Error | Attempt {attempt}/{MAX_RETRIES} | "
                        f"{str(e)} | Retrying in {delay}s"
                    )
                    time.sleep(delay)

                except Exception as e:
                    failed_logger.error(
                        f"Batch {batch_no} Unexpected Error | Attempt {attempt} | {str(e)}"
                    )
                    time.sleep(exponential_backoff(attempt))

            if not success:
                failed_batches += 1
                failed_logger.error(
                    f"Batch {batch_no} permanently failed after {MAX_RETRIES} retries | "
                    f"{batch_start} → {batch_end}"
                )
                console_logger.error(f"✗ Batch {batch_no} Failed")

            batch_no += 1
            time.sleep(SLEEP_BETWEEN_BATCHES)

        # Final summary
        summary = (
            f"Transfer Completed | Success: {success_batches} | Failed: {failed_batches} | "
            f"Total Rows: {total_rows_written:,}"
        )
        console_logger.info(summary)
        success_logger.info(summary)

        if failed_batches > 0:
            failed_logger.info(summary)
            sys.exit(1)  # Exit with error code if any failures

    finally:
        session.close()


# ------------------ EXECUTE ------------------
if __name__ == "__main__":
    transfer_by_date()