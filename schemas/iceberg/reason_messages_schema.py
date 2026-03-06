import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "id",
    "created_at",
]
TIMESTAMP_FIELDS = [
    "created_at",
]

BOOLEAN_FIELDS = []

INTEGER_FIELDS = [
    "id",
]

VARCHAR_FIELDS = [
    "time_sorted_id",
    "status",
    "channel",
    "type",
]
DATE_FIELDS = []
FIELD_OVERRIDES = {

    # 🔑 Primary Key
    "id": (IntegerType(), pa.int32(), True),

    # 🧾 Event / Status Info
    "time_sorted_id": (StringType(), pa.string(), False),
    "status": (StringType(), pa.string(), False),
    "channel": (StringType(), pa.string(), False),
    "type": (StringType(), pa.string(), False),

    # 📦 Message payload (JSON)
    "msg": (StringType(), pa.string(), False),

    # 🕒 Audit
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
}
