import pyarrow as pa
from pyiceberg.types import *


REQUIRED_FIELDS = [
    "id",
]

TIMESTAMP_FIELDS = [
    "created_at",
]
DATE_FIELDS = []
BOOLEAN_FIELDS = []

INTEGER_FIELDS = [
    "affected_rows",
]

VARCHAR_FIELDS = [
    "id",
    "operation_type",
    "status",
]

FIELD_OVERRIDES = {

    # 🔑 Primary Key
    "id": (StringType(), pa.string(), True),

    # ⚙ Operation details
    "operation_type": (StringType(), pa.string(), False),
    "affected_rows": (IntegerType(), pa.int32(), False),
    "status": (StringType(), pa.string(), False),

    # 📝 Message / Error details
    "message": (StringType(), pa.string(), False),

    # 🕒 Audit
    "created_at": (TimestampType(), pa.timestamp("ms"), False),
}