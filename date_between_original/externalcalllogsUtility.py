import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
JSON_FIELDS = ["res_obj"]
TIMESTAMP_FIELDS = ["created_at"]
REQUIRED_FIELDS = ["id"]
BOOLEAN_FIELDS = []
DATE_FIELDS = []

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys / Required
    "id": (StringType(), pa.string(), True),

    # Nullable Strings/Varchar
    "ext_log_id": (StringType(), pa.string(), False),
    "short_code": (StringType(), pa.string(), False),
    "invoice_no": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),

    # JSON fields (mapped to String)
    "res_obj": (StringType(), pa.string(), False),

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
}
