import logging
import time
from jobs import masterorders,masterorders_w, orderlineitems, pickup_delivery_items

# logging.basicConfig(
#     filename="/var/log/ingestion_all.log",
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )

JOBS = [
    # ("masterorders", masterorders.run),
    ("masterorders_w", masterorders_w.run),
    # ("orderlineitems", orderlineitems.run),
    # ("pickup_delivery_items", pickup_delivery_items.run),
]

if __name__ == "__main__":
    for name, job in JOBS:
        try:
            logging.info(f"JOB START | {name}")
            result = job()
            logging.info(f"JOB SUCCESS | {name} | {result}")
        except Exception as e:
            logging.exception(f"JOB FAILED | {name} | {str(e)}")