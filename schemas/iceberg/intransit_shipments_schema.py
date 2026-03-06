import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = ['t_shipment_id', 'time_sorted_id', 'shipment_type', 'provider', 'collection_branch_code', 'status', 'created_at', 'createdAt']
TIMESTAMP_FIELDS = ['created_at', 'updated_at', 'createdAt', 'updatedAt']
BOOLEAN_FIELDS = []
INTEGER_FIELDS = ['dimension_length', 'dimension_height', 'dimension_width']
VARCHAR_FIELDS = ['t_shipment_id', 'time_sorted_id', 'shipment_type', 'fulfiled_by',
                  'provider', 'branch_code', 'collection_branch_code', 'status',
                  'vehicle_no', 'vehicle_type', 'vehicle_image', 'vehicle_name',
                  'driver_code', 'driver_name', 'driver_image', 'driver_contact',
                  'assistant_code', 'assistant_name', 'assistant_pic',
                  'assistant_contact', 'secondary_assistant_code', 'secondary_assistant_name',
                  'secondary_assistant_pic', 'secondary_assistant_contact',
                  'tracking_id', 'tracking_url', 'courier_name', 'dimension_units',
                  'permit', 'created_by', 'updated_by', 'courier_code']

DATE_FIELDS = []
FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "t_shipment_id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),

    # 📦 Shipment Details
    "shipment_type": (StringType(), pa.string(), True),
    "fulfiled_by": (StringType(), pa.string(), False),
    "provider": (StringType(), pa.string(), True),

    # 🏬 Branch
    "branch_code": (StringType(), pa.string(), False),
    "collection_branch_code": (StringType(), pa.string(), True),

    # 📌 Status
    "status": (StringType(), pa.string(), True),

    # 🚗 Vehicle
    "vehicle_no": (StringType(), pa.string(), False),
    "vehicle_type": (StringType(), pa.string(), False),
    "vehicle_name": (StringType(), pa.string(), False),
    "vehicle_image": (StringType(), pa.string(), False),

    # 🧑 Driver
    "driver_code": (StringType(), pa.string(), False),
    "driver_name": (StringType(), pa.string(), False),
    "driver_image": (StringType(), pa.string(), False),
    "driver_contact": (StringType(), pa.string(), False),

    # 🧑 Assistant
    "assistant_code": (StringType(), pa.string(), False),
    "assistant_name": (StringType(), pa.string(), False),
    "assistant_pic": (StringType(), pa.string(), False),
    "assistant_contact": (StringType(), pa.string(), False),

    # 🧑 Secondary Assistant
    "secondary_assistant_code": (StringType(), pa.string(), False),
    "secondary_assistant_name": (StringType(), pa.string(), False),
    "secondary_assistant_pic": (StringType(), pa.string(), False),
    "secondary_assistant_contact": (StringType(), pa.string(), False),

    # 📦 Tracking
    "tracking_id": (StringType(), pa.string(), False),
    "tracking_url": (StringType(), pa.string(), False),
    "courier_name": (StringType(), pa.string(), False),
    "courier_code": (StringType(), pa.string(), False),

    # 📐 Dimensions
    "dimension_length": (IntegerType(), pa.int32(), False),
    "dimension_height": (IntegerType(), pa.int32(), False),
    "dimension_width": (IntegerType(), pa.int32(), False),
    "dimension_units": (StringType(), pa.string(), False),

    # 📄 Permit
    "permit": (StringType(), pa.string(), False),

    # 🕒 Audit (snake_case)
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),

    # 🕒 Audit (camelCase)
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), False),

    # 👤 Audit User
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}