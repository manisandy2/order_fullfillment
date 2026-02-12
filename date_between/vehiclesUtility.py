import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)

REQUIRED_FIELDS = [
    "id",
    "time_sorted_id",
    "type_of_vehicle",
    "truck_name",
    "truck_reg_no",
    "capacity",
    "vehicle_from",
    "engine_type",
    "created_at",
    "createdAt",
]
DATE_FIELDS = []
TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
]


INTEGER_FIELDS = []

BOOLEAN_FIELDS = [
    "is_available",
]

VARCHAR_FIELDS = [
    # 🔑 Identifiers
    "id",
    "time_sorted_id",

    # 🚚 Vehicle details
    "type_of_vehicle",
    "truck_name",
    "truck_reg_no",
    "capacity",
    "ratings",

    # 🧭 Classification
    "vehicle_from",
    "engine_type",

    # 👤 Driver
    "default_driver_name",
    "default_driver_id",

    # 🧾 Audit
    "created_by",
    "updated_by",

    # 📦 JSON stored as string
    "vehicle_image",
    "documents",
    "branch_code_list",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),

    # 🚚 Vehicle details
    "type_of_vehicle": (StringType(), pa.string(), True),
    "truck_name": (StringType(), pa.string(), True),
    "truck_reg_no": (StringType(), pa.string(), True),
    "capacity": (StringType(), pa.string(), True),
    "ratings": (StringType(), pa.string(), False),

    # ⚙ Availability
    "is_available": (BooleanType(), pa.bool_(), False),

    # 🧭 Classification
    "vehicle_from": (StringType(), pa.string(), True),
    "engine_type": (StringType(), pa.string(), True),

    # 🖼 Media / Documents (JSON → String)
    "vehicle_image": (StringType(), pa.string(), False),
    "documents": (StringType(), pa.string(), False),
    "branch_code_list": (StringType(), pa.string(), False),

    # 👤 Default driver
    "default_driver_name": (StringType(), pa.string(), False),
    "default_driver_id": (StringType(), pa.string(), False),

    # 🕒 Audit timestamps
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), False),

    # 👮 Audit users
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}