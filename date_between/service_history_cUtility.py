import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "id",
    "service_id",
    "order_id",
    "invoice_no",
    "item_code",
    "item_serial_no",
    "status",
    "created_at",
    "updated_at",
]

TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
]

BOOLEAN_FIELDS = []

VARCHAR_FIELDS = [
    "id",
    "service_id",
    "order_id",
    "invoice_no",
    "item_code",
    "item_serial_no",
    "status",
    "remarks",
    "additional_info",
    "created_by",
    "updated_by",
    "service_type",
]
DATE_FIELDS = []

FIELD_OVERRIDES = {

    # 🔑 Primary Key
    "id": (StringType(), pa.string(), True),

    # 🔗 Service / Order Info
    "service_id": (StringType(), pa.string(), True),
    "order_id": (StringType(), pa.string(), True),
    "invoice_no": (StringType(), pa.string(), True),
    "item_code": (StringType(), pa.string(), True),
    "item_serial_no": (StringType(), pa.string(), True),

    # 📌 Status
    "status": (StringType(), pa.string(), True),
    "service_type": (StringType(), pa.string(), False),

    # 📝 Optional Info
    "remarks": (StringType(), pa.string(), False),
    "additional_info": (StringType(), pa.string(), False),  # JSON → String

    # 🕒 Audit Fields
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), True),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}
