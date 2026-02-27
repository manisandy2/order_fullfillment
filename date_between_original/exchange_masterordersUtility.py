import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
JSON_FIELDS = [
    "order_tag", "customer_address", "shipment_address", "billing_address",
    "payment_details", "refund_details", "voucher_details", 
    "employee_sale_details", "order_summary_details", "other_details",
    "lineitems", "lineitem_status", "service_details"
]

TIMESTAMP_FIELDS = ["invoice_date", "created_at", "updated_at"]

DECIMAL_FIELDS = ["latitude", "longitude"]
BOOLEAN_FIELDS = []
REQUIRED_FIELDS = ["order_id", "sale_order_id"]
DATE_FIELDS = []
# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys / Required
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # Nullable Strings/Varchar
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False), # text in MySQL
    "channel": (StringType(), pa.string(), False), # enum
    "channel_medium": (StringType(), pa.string(), False), # enum
    "order_status": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "order_type": (StringType(), pa.string(), False),
    "delivery_from": (StringType(), pa.string(), False), # enum
    "delivery_from_branchcode": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),
    "cust_id": (StringType(), pa.string(), False),
    "cust_primary_email": (StringType(), pa.string(), False),
    "cust_primary_contact": (StringType(), pa.string(), False),
    "cust_mobile": (StringType(), pa.string(), False),
    "invoice_pdf": (StringType(), pa.string(), False), # text in MySQL
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # JSON fields (mapped to String)
    "order_tag": (StringType(), pa.string(), False),
    "customer_address": (StringType(), pa.string(), False),
    "shipment_address": (StringType(), pa.string(), False),
    "billing_address": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),
    "refund_details": (StringType(), pa.string(), False),
    "voucher_details": (StringType(), pa.string(), False),
    "employee_sale_details": (StringType(), pa.string(), False),
    "order_summary_details": (StringType(), pa.string(), False),
    "other_details": (StringType(), pa.string(), False),
    "lineitems": (StringType(), pa.string(), False),
    "lineitem_status": (StringType(), pa.string(), False),
    "service_details": (StringType(), pa.string(), False),

    # Decimal fields (mapped to Double/Float64)
    "latitude": (DoubleType(), pa.float64(), False),
    "longitude": (DoubleType(), pa.float64(), False),

    # Timestamp fields
    "invoice_date": (TimestampType(), pa.timestamp('ms'), False),
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
}
