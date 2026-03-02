import logging
import json
from typing import Dict, List, Tuple, Any
import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)
from datetime import datetime, date
from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "order_id",
    "sale_order_id",
]

TIMESTAMP_FIELDS = [
    "invoice_date",
    "created_at",
    "updated_at",
]
DATE_FIELDS = []

BOOLEAN_FIELDS = []

INTEGER_FIELDS = []

VARCHAR_FIELDS = [
    "order_id",
    "sale_order_id",
    "invoice_no",
    "invoice_reff_no",
    "channel",
    "channel_medium",
    "order_status",
    "order_inv_status",
    "order_type",
    "delivery_from",
    "delivery_from_branchcode",
    "billing_branch_code",
    "cust_id",
    "cust_primary_email",
    "cust_primary_contact",
    "cust_mobile",
    "invoice_pdf",
    "created_by",
    "updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # 🧾 Invoice
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_date": (TimestampType(), pa.timestamp("ms"), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),

    # 📦 Channel / Order state
    "channel": (StringType(), pa.string(), False),
    "channel_medium": (StringType(), pa.string(), False),
    "order_status": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "order_type": (StringType(), pa.string(), False),

    # 🚚 Delivery / Billing
    "delivery_from": (StringType(), pa.string(), False),
    "delivery_from_branchcode": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),

    # 👤 Customer
    "cust_id": (StringType(), pa.string(), False),
    "cust_primary_email": (StringType(), pa.string(), False),
    "cust_primary_contact": (StringType(), pa.string(), False),
    "cust_mobile": (StringType(), pa.string(), False),

    # 📍 Addresses (JSON → String)
    "customer_address": (StringType(), pa.string(), False),
    "shipment_address": (StringType(), pa.string(), False),
    "billing_address": (StringType(), pa.string(), False),

    # 🌍 Geo
    "latitude": (DoubleType(), pa.float64(), False),
    "longitude": (DoubleType(), pa.float64(), False),

    # 📦 Line items & financials (ALL JSON)
    "lineitems": (StringType(), pa.string(), False),
    "lineitem_status": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),
    "refund_details": (StringType(), pa.string(), False),
    "voucher_details": (StringType(), pa.string(), False),
    "employee_sale_details": (StringType(), pa.string(), False),
    "order_summary_details": (StringType(), pa.string(), False),
    "other_details": (StringType(), pa.string(), False),
    "service_details": (StringType(), pa.string(), False),
    "multi_invoice": (StringType(), pa.string(), False),
    "order_tag": (StringType(), pa.string(), False),

    # 📄 Documents
    "invoice_pdf": (StringType(), pa.string(), False),

    # 🕒 Audit
    "created_at": (TimestampType(), pa.timestamp("ms"), False),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}

