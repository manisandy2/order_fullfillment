import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "code",
    "time_sorted_id",
    "permissions",
    "created_on",
    "created_at",
    "createdAt",
    "updatedAt",
]

TIMESTAMP_FIELDS = [
    "created_on",
    "updated_on",
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
]

BOOLEAN_FIELDS = [
    "isEditable",
    "isActive",
    "is_multi_branch_role",
]

INTEGER_FIELDS = []

VARCHAR_FIELDS = [
    "time_sorted_id",
    "code",
    "name",
    "created_by",
    "updated_by",
    "role_type",
]
DATE_FIELDS = []

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "code": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),

    # 🧑 Role details
    "name": (StringType(), pa.string(), False),
    "role_type": (StringType(), pa.string(), False),

    # 🔐 Permissions (JSON → String)
    "permissions": (StringType(), pa.string(), True),

    # ⚙ Flags
    "isEditable": (BooleanType(), pa.bool_(), False),
    "isActive": (BooleanType(), pa.bool_(), False),
    "is_multi_branch_role": (BooleanType(), pa.bool_(), False),

    # 🕒 Audit timestamps
    "created_on": (TimestampType(), pa.timestamp("ms"), True),
    "updated_on": (TimestampType(), pa.timestamp("ms"), False),

    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),

    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), True),

    # 👤 Audit users
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}

