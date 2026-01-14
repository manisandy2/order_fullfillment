import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)

REQUIRED_FIELDS = [
    "shipment_id",
    "time_sorted_id",
    "provider",
    "status",
    "created_at",
    "createdAt",
]


TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
    "ewaybilldate",
]


INTEGER_FIELDS = [
    "dimension_length",
    "dimension_height",
    "dimension_width",
]

BOOLEAN_FIELDS = []

VARCHAR_FIELDS = [
    # 🔑 Identifiers
    "shipment_id",
    "time_sorted_id",

    # 🚚 Provider / Status
    "provider",
    "status",
    "fulfiled_by",
    "branch_code",

    # 🚗 Vehicle
    "vehicle_no",
    "vehicle_type",
    "vehicle_image",
    "vehicle_name",

    # 🧑 Driver
    "driver_code",
    "driver_name",
    "driver_image",
    "driver_contact",

    # 🧑‍🤝‍🧑 Assistants
    "assistant_code",
    "assistant_name",
    "assistant_pic",
    "assistant_contact",
    "secondary_assistant_code",
    "secondary_assistant_name",
    "secondary_assistant_pic",
    "secondary_assistant_contact",

    # 📦 Tracking / Courier
    "tracking_id",
    "tracking_ref_suffix",
    "tracking_url",
    "courier_name",
    "courier_code",

    # 📐 Dimensions
    "dimension_units",

    # 🧾 Compliance
    "permit",
    "ewaybillno",

    # 🕒 Audit (string-only audit fields)
    "created_by",
    "updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary
    "shipment_id": (StringType(), pa.string(), True),

    # ⏱ Sorting / Status
    "time_sorted_id": (StringType(), pa.string(), True),
    "status": (StringType(), pa.string(), True),

    # 🚚 Provider / Branch
    "provider": (StringType(), pa.string(), True),
    "fulfiled_by": (StringType(), pa.string(), False),
    "branch_code": (StringType(), pa.string(), False),

    # 🚗 Vehicle
    "vehicle_no": (StringType(), pa.string(), False),
    "vehicle_type": (StringType(), pa.string(), False),
    "vehicle_image": (StringType(), pa.string(), False),
    "vehicle_name": (StringType(), pa.string(), False),

    # 🧑 Driver
    "driver_code": (StringType(), pa.string(), False),
    "driver_name": (StringType(), pa.string(), False),
    "driver_image": (StringType(), pa.string(), False),
    "driver_contact": (StringType(), pa.string(), False),

    # 🧑‍🤝‍🧑 Assistants
    "assistant_code": (StringType(), pa.string(), False),
    "assistant_name": (StringType(), pa.string(), False),
    "assistant_pic": (StringType(), pa.string(), False),
    "assistant_contact": (StringType(), pa.string(), False),

    "secondary_assistant_code": (StringType(), pa.string(), False),
    "secondary_assistant_name": (StringType(), pa.string(), False),
    "secondary_assistant_pic": (StringType(), pa.string(), False),
    "secondary_assistant_contact": (StringType(), pa.string(), False),

    # 📦 Tracking
    "tracking_id": (StringType(), pa.string(), False),
    "tracking_ref_suffix": (StringType(), pa.string(), False),
    "tracking_url": (StringType(), pa.string(), False),
    "courier_name": (StringType(), pa.string(), False),
    "courier_code": (StringType(), pa.string(), False),

    # 📐 Dimensions
    "dimension_length": (IntegerType(), pa.int32(), False),
    "dimension_height": (IntegerType(), pa.int32(), False),
    "dimension_width": (IntegerType(), pa.int32(), False),
    "dimension_units": (StringType(), pa.string(), False),

    # 🧾 Compliance
    "permit": (StringType(), pa.string(), False),
    "ewaybillno": (StringType(), pa.string(), False),
    "ewaybilldate": (TimestampType(), pa.timestamp("ms"), False),

    # 🕒 Audit (MySQL duplicates preserved)
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), False),

    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}