import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "manifest_id",
    "time_sorted_id",
    "shipment_id",
    "items_total",
    "createdAt",
    "created_at",
]

TIMESTAMP_FIELDS = [
    "createdAt",
    "updatedAt",
    "created_at",
    "updated_at",
]
DATE_FIELDS = []

BOOLEAN_FIELDS = []

INTEGER_FIELDS = [
    "items_total",
]

VARCHAR_FIELDS = [
    "manifest_id",
    "time_sorted_id",
    "shipment_id",
    "manifest_type",
    "created_by",
    "updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "manifest_id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),
    "shipment_id": (StringType(), pa.string(), True),

    # 📦 Manifest details
    "manifest_type": (StringType(), pa.string(), False),
    "items_total": (IntegerType(), pa.int32(), True),

    # 👤 Audit users
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # 🕒 Audit timestamps (camelCase)
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), False),

    # 🕒 Audit timestamps (snake_case)
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
}
