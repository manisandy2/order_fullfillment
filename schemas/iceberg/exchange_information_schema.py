import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
INTEGER_FIELDS = []  # No explicit integer fields in the provided schema, all look like varchar or text except created_at
BOOLEAN_FIELDS = []  # No explicit boolean fields
JSON_FIELDS = [
    "customer_info", 
    "product_info", 
    "exchange_quote_details", 
    "device_evaluation", 
    "device_images"
]
TIMESTAMP_FIELDS = ["created_at"]
REQUIRED_FIELDS = ["order_id"]
DATE_FIELDS = []
# Field type overrides based on MySQL schema
# name: (IcebergType, ArrowType, Required)
FIELD_OVERRIDES = {
    # varchar & Text
    "order_id": (StringType(), pa.string(), True),  # UNI, not null
    "quote_id": (StringType(), pa.string(), False),
    "mobile_no": (StringType(), pa.string(), False),
    "name": (StringType(), pa.string(), False),
    "imei": (StringType(), pa.string(), False),
    "item_code": (StringType(), pa.string(), False),
    "status": (StringType(), pa.string(), False),
    "customer_id": (StringType(), pa.string(), False),
    "category": (StringType(), pa.string(), False),
    "brand": (StringType(), pa.string(), False),
    "branch_code": (StringType(), pa.string(), False),
    "confirm_extra_value": (StringType(), pa.string(), False),
    "actual_product_value": (StringType(), pa.string(), False),
    "actual_exchange_value": (StringType(), pa.string(), False),
    "extra_value_comments": (StringType(), pa.string(), False),
    "extra_value": (StringType(), pa.string(), False),
    "designation": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "vendor_id": (StringType(), pa.string(), False),
    "vendor_name": (StringType(), pa.string(), False),
    "device_condition": (StringType(), pa.string(), False),

    # JSON fields
    "customer_info": (StringType(), pa.string(), False),
    "product_info": (StringType(), pa.string(), False),
    "exchange_quote_details": (StringType(), pa.string(), False),
    "device_evaluation": (StringType(), pa.string(), False),
    "device_images": (StringType(), pa.string(), False),

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), False), # Schema says nullable: true, key: MUL
}
