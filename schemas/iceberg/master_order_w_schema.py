import pyarrow as pa
from pyiceberg.types import *
from datetime import datetime, date
from pyiceberg.schema import Schema



FIELD_OVERRIDES = {
    # Required keys
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # Integer fields
    # "oms_data_migration_status": (IntegerType(), pa.int32(), False),
    # "cust_id_update": (LongType(), pa.int32(), False),
    "cust_id_update": (StringType(), pa.string(), False),
    # Float fields
    # "latitude": (DoubleType(), pa.float64(), False),
    # "longitude": (DoubleType(), pa.float64(), False),
    # "latitude": (StringType(), pa.string(), False),
    # "longitude": (StringType(), pa.string(), False),
    "latitude": (StringType(), pa.string(), False),
    "longitude": (StringType(), pa.string(), False),

    # Date fields
    # "invoice_date": (DateType(), pa.timestamp("ms"), False),
    # "updated_at_new": (DateType(), pa.timestamp("ms"), False),

    # Timestamp fields
    "invoice_date": (TimestampType(), pa.timestamp('ms'), False),
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at_new": (TimestampType(), pa.timestamp('ms'), False),

    # Other explicit string fields
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),
    "channel": (StringType(), pa.string(), False),
    "channel_medium": (StringType(), pa.string(), False),
    "order_status": (StringType(), pa.string(), False),
    "order_tag": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "order_type": (StringType(), pa.string(), False),
    "delivery_from": (StringType(), pa.string(), False),
    "delivery_from_branchcode": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),
    "cust_id": (StringType(), pa.string(), False),
    "cust_primary_email": (StringType(), pa.string(), False),
    "cust_primary_contact": (StringType(), pa.string(), False),
    "cust_mobile": (StringType(), pa.string(), False),
    "customer_address": (StringType(), pa.string(), False),
    "shipment_address": (StringType(), pa.string(), False),
    "billing_address": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),
    "refund_details": (StringType(), pa.string(), False),
    "voucher_details": (StringType(), pa.string(), False),
    "employee_sale_details": (StringType(), pa.string(), False),
    "order_summary_details": (StringType(), pa.string(), False),
    "other_details": (StringType(), pa.string(), False),
    "service_details": (StringType(), pa.string(), False),
    "invoice_pdf": (StringType(), pa.string(), False),
    "lineitems": (StringType(), pa.string(), False),
    "lineitem_status": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
    "multi_invoice": (StringType(), pa.string(), False),
}



BOOLEAN_FIELDS = []
FLOAT_FIELDS = ["latitude","longitude"]
INTEGER_FIELDS = []
DATE_FIELDS = []
# DOUBLE_FIELDS = ["latitude","longitude"]
TIMESTAMP_FIELDS = ["created_at", "updated_at", "invoice_date", "updated_at_new"]
STRING_FIELDS = ["order_id", "sale_order_id", "invoice_no", "invoice_reff_no", "invoice_reff_date",
                 "channel", "channel_medium", "order_status", "order_tag", "order_inv_status", "order_type",
                 "delivery_from", "delivery_from_branchcode", "billing_branch_code", "cust_id",
                 "cust_primary_email",
                 "cust_primary_contact", "cust_mobile", "customer_address", "shipment_address", "billing_address",
                 "payment_details", "refund_details", "voucher_details", "employee_sale_details",
                 "order_summary_details",
                 "other_details", "service_details", "invoice_pdf", "lineitems", "lineitem_status", "created_by",
                 "updated_by",
                 "multi_invoice"]


