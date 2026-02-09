import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "pickup_delivery_req_id",
]
DATE_FIELDS = []

TIMESTAMP_FIELDS = [
    "invoice_date",
    "expected",
    "row_added_dttm",
    "row_updated_dttm",
    "updated_at_new",
]

BOOLEAN_FIELDS = [
    "is_accepted",
]

INTEGER_FIELDS = [
    "oms_data_migration_status",
]

VARCHAR_FIELDS = [
    "pickup_delivery_req_id",
    "order_id",
    "sale_order_id",
    "invoice_no",
    "invoice_reff_no",
    "request_type",
    "invoice_status",
    "order_type",
    "order_inv_status",
    "row_added_by",
    "row_updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary Key
    "pickup_delivery_req_id": (StringType(), pa.string(), True),

    # 🧾 Order / Invoice
    "order_id": (StringType(), pa.string(), False),
    "sale_order_id": (StringType(), pa.string(), False),
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_date": (TimestampType(), pa.timestamp("ms"), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),
    "invoice_status": (StringType(), pa.string(), False),

    # 📦 Request Details
    "request_type": (StringType(), pa.string(), False),
    "inventory_details": (StringType(), pa.string(), False),
    "return_details": (StringType(), pa.string(), False),
    "collection_details": (StringType(), pa.string(), False),
    "to_details": (StringType(), pa.string(), False),
    "shipment_details": (StringType(), pa.string(), False),
    "order_details": (StringType(), pa.string(), False),

    # ⏱ Expected / Status
    "expected": (TimestampType(), pa.timestamp("ms"), False),
    "is_accepted": (IntegerType(), pa.int32(), False),
    "rejection_reason": (StringType(), pa.string(), False),

    # 🌍 Geo
    "latitude": (DoubleType(), pa.float64(), False),
    "longitude": (DoubleType(), pa.float64(), False),

    # 📊 Order State
    "order_type": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),

    # 🕒 Audit
    "row_added_dttm": (TimestampType(), pa.timestamp("ms"), False),
    "row_updated_dttm": (TimestampType(), pa.timestamp("ms"), False),
    "updated_at_new": (TimestampType(), pa.timestamp("ms"), False),
    "row_added_by": (StringType(), pa.string(), False),
    "row_updated_by": (StringType(), pa.string(), False),

    # ⚙ Migration / System
    "oms_data_migration_status": (IntegerType(), pa.int32(), False),
}