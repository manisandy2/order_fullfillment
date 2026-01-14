import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)

REQUIRED_FIELDS = [
    "id",
    "time_sorted_id",
    "firstname",
    "lastname",
    "password",
    "location_type",
    "isActive",
    "created_at",
    "createdAt",
    "created_on",
    "updatedAt",
]

BOOLEAN_FIELDS = [
    "email_verified",
    "isActive",
]

TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
    "created_on",
    "updated_on",
]
VARCHAR_FIELDS = [
    # 🔑 Identifiers
    "id",
    "time_sorted_id",

    # 👤 User identity
    "email",
    "firstname",
    "lastname",
    "password",
    "mobile_no",

    # 📍 Location
    "location_type",
    "warehouse",
    "store",

    # 🖼 Profile
    "picture",

    # 🧾 Audit
    "created_by",
    "updated_by",
]

JSON_FIELDS = [
    "role",
    "other_data",
    "branch_code_list",
    "branch_list",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),

    # 👤 User identity
    "email": (StringType(), pa.string(), False),
    "firstname": (StringType(), pa.string(), True),
    "lastname": (StringType(), pa.string(), False),
    "password": (StringType(), pa.string(), True),
    "mobile_no": (StringType(), pa.string(), False),

    # 🧭 Location
    "location_type": (StringType(), pa.string(), True),
    "warehouse": (StringType(), pa.string(), False),
    "store": (StringType(), pa.string(), False),

    # 🖼 Profile
    "picture": (StringType(), pa.string(), False),

    # ⚙ Flags
    "email_verified": (BooleanType(), pa.bool_(), False),
    "isActive": (BooleanType(), pa.bool_(), True),

    # 🧾 JSON fields (stored as string)
    "role": (StringType(), pa.string(), False),
    "other_data": (StringType(), pa.string(), False),
    "branch_code_list": (StringType(), pa.string(), False),
    "branch_list": (StringType(), pa.string(), False),

    # 🕒 Audit timestamps
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), True),
    "created_on": (TimestampType(), pa.timestamp("ms"), True),
    "updated_on": (TimestampType(), pa.timestamp("ms"), False),

    # 👮 Audit users
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}