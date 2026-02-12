import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)

REQUIRED_FIELDS = [
    "service_id",
    "service_type",
    "order_id",
    "invoice_no",
    "item_code",
    "item_image",
    "item_name",
    "item_serial_no",
    "status",
    "sub_status",
    "customer_id",
    "customer_name",
    "customer_address",
    "created_at",
    "updated_at",
]

TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
]

DATE_FIELDS = [
    "call_booking_date",
]

BOOLEAN_FIELDS = []

VARCHAR_FIELDS = [
    "service_id",
    "service_type",
    "order_id",
    "invoice_no",
    "item_code",
    "item_image",
    "item_name",
    "item_serial_no",
    "status",
    "sub_status",
    "amount",
    "service_agent_id",
    "service_agent_name",
    "service_agent_mobile_no",
    "additional_service_agent",
    "rating",
    "remarks",
    "payment_status",
    "payment_details",
    "customer_id",
    "customer_name",
    "customer_address",
    "mobile_no",
    "completed_date",
    "call_booking_number",
    "created_by",
    "updated_by",
    "additional_info",
    "branch_code",
    "branch_name",
]
FIELD_OVERRIDES = {

    # 🔑 Primary / Core
    "service_id": (StringType(), pa.string(), True),
    "service_type": (StringType(), pa.string(), True),

    # 🧾 Order / Item
    "order_id": (StringType(), pa.string(), True),
    "invoice_no": (StringType(), pa.string(), True),
    "item_code": (StringType(), pa.string(), True),
    "item_image": (StringType(), pa.string(), True),
    "item_name": (StringType(), pa.string(), True),
    "item_serial_no": (StringType(), pa.string(), True),

    # 📌 Status
    "status": (StringType(), pa.string(), True),
    "sub_status": (StringType(), pa.string(), False),

    # 💰 Service / Payment
    "amount": (StringType(), pa.string(), False),
    "payment_status": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),  # JSON → String

    # 🧑‍🔧 Service Agent
    "service_agent_id": (StringType(), pa.string(), False),
    "service_agent_name": (StringType(), pa.string(), False),
    "service_agent_mobile_no": (StringType(), pa.string(), False),
    "additional_service_agent": (StringType(), pa.string(), False),  # JSON

    # ⭐ Feedback
    "rating": (StringType(), pa.string(), False),
    "remarks": (StringType(), pa.string(), False),

    # 👤 Customer
    "customer_id": (StringType(), pa.string(), True),
    "customer_name": (StringType(), pa.string(), True),
    "customer_address": (StringType(), pa.string(), True),  # JSON
    "mobile_no": (StringType(), pa.string(), False),         # JSON

    # 📅 Dates
    "call_booking_date": (DateType(), pa.date32(), False),
    "completed_date": (StringType(), pa.string(), False),
    "call_booking_number": (StringType(), pa.string(), False),

    # 🏢 Branch
    "branch_code": (StringType(), pa.string(), False),
    "branch_name": (StringType(), pa.string(), False),

    # 🕒 Audit
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), True),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # 🧩 Extra
    "additional_info": (StringType(), pa.string(), False),  # JSON
}