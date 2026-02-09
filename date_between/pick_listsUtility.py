import pyarrow as pa
from pyiceberg.types import  *

REQUIRED_FIELDS = [
    "id",
    "picklist_id",
    "items",
    "pickers",
    "ispickup_active",
    "picklist_status",
    "created_on",
    "created_at",
]
DATE_FIELDS = []
TIMESTAMP_FIELDS = [
    "created_on",
    "updated_on",
    "created_at",
    "updated_at",
]

BOOLEAN_FIELDS = [
    "ispickup_active",
]

INTEGER_FIELDS = [
    "items_total",
    "pickers_total",
]

VARCHAR_FIELDS = [
    "id",
    "picklist_id",
    "allocate_picker_by",
    "branch_code",
    "picklist_status",
    "created_by",
    "updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "id": (StringType(), pa.string(), True),
    "picklist_id": (StringType(), pa.string(), True),

    # 📦 Allocation
    "allocate_picker_by": (StringType(), pa.string(), False),
    "branch_code": (StringType(), pa.string(), False),

    # 📋 JSON payloads
    "items": (StringType(), pa.string(), True),
    "pickers": (StringType(), pa.string(), True),

    # 🔢 Counts
    "items_total": (IntegerType(), pa.int32(), False),
    "pickers_total": (IntegerType(), pa.int32(), False),

    # ⚙ Status / Flags
    "ispickup_active": (BooleanType(), pa.bool_(), True),
    "picklist_status": (StringType(), pa.string(), True),

    # 🕒 Audit timestamps
    "created_on": (TimestampType(), pa.timestamp("ms"), True),
    "updated_on": (TimestampType(), pa.timestamp("ms"), False),
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),

    # 👤 Audit users
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}
