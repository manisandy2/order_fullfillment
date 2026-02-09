import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
TIMESTAMP_FIELDS = ['created_at']
BOOLEAN_FIELDS = []
DATE_FIELDS = []
VARCHAR_FIELDS = ['id', 'service_type', 'order_id', 'item_code', 'item_image', 'item_name',
                  'item_serial_no', 'invoice_no', 'installation_status', 'installation_sub_status',
                  'installation_rating', 'installation_remarks', 'installation_address',
                  'installation_call_booking_date', 'installation_completed_date',
                  'installation_call_booking_number', 'created_by']


REQUIRED_FIELDS = ['id', 'service_type', 'order_id', 'item_code', 'item_image', 'item_name',
                   'item_serial_no', 'invoice_no', 'installation_status',
                   'installation_sub_status', 'created_at']

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Primary / required identifiers
    "id": (StringType(), pa.string(), True),
    "service_type": (StringType(), pa.string(), True),  # enum
    "order_id": (StringType(), pa.string(), False),
    "item_code": (StringType(), pa.string(), True),
    "item_image": (StringType(), pa.string(), True),
    "item_name": (StringType(), pa.string(), True),
    "item_serial_no": (StringType(), pa.string(), True),
    "invoice_no": (StringType(), pa.string(), True),

    # Status enums (stored as STRING in Iceberg)
    "installation_status": (StringType(), pa.string(), True),       # enum
    "installation_sub_status": (StringType(), pa.string(), True),   # enum

    # Nullable text fields
    "installation_rating": (StringType(), pa.string(), False),
    "installation_remarks": (StringType(), pa.string(), False),
    "installation_address": (StringType(), pa.string(), False),
    "installation_call_booking_date": (StringType(), pa.string(), False),
    "installation_completed_date": (StringType(), pa.string(), False),
    "installation_call_booking_number": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),

    # Timestamp
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
}