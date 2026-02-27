import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "job_id",
    "job_start_on",
    "created_on",
    "created_at",
]

TIMESTAMP_FIELDS = [
    "job_start_on",
    "created_on",
    "updated_on",
    "created_at",
    "updated_at",
]
DATE_FIELDS = []
BOOLEAN_FIELDS = []

INTEGER_FIELDS = []

VARCHAR_FIELDS = [
    "job_id",
    "job_status",
    "job_type",
    "time_taken",
    "branch_code",
    "invoice_no",
    "error_msg",
    "created_by",
    "updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "job_id": (StringType(), pa.string(), True),

    # ⚙ Job metadata
    "job_status": (StringType(), pa.string(), False),
    "job_type": (StringType(), pa.string(), False),
    "time_taken": (StringType(), pa.string(), False),
    "branch_code": (StringType(), pa.string(), False),
    "invoice_no": (StringType(), pa.string(), False),

    # ⏱ Job timing
    "job_start_on": (TimestampType(), pa.timestamp("ms"), True),

    # 📦 Job payload / IO (JSON & URLs)
    "query_obj": (StringType(), pa.string(), False),
    "input_gcp_url": (StringType(), pa.string(), False),
    "output_gcp_url": (StringType(), pa.string(), False),
    "error_gcp_url": (StringType(), pa.string(), False),

    # ❌ Error info
    "error_msg": (StringType(), pa.string(), False),

    # 🕒 Audit timestamps
    "created_on": (TimestampType(), pa.timestamp("ms"), True),
    "updated_on": (TimestampType(), pa.timestamp("ms"), False),

    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),

    # 👤 Audit users
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}
