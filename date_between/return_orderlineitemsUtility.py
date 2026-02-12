import pyarrow as pa
from pyiceberg.types import *

REQUIRED_FIELDS = [
    "line_item_id",
    "order_line_item_id",
    "master_order_id",
    "master_sale_order_id",
]

TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
]
DATE_FIELDS =[]
BOOLEAN_FIELDS = []

INTEGER_FIELDS = [
    "quantity",
    "special_price",
]

VARCHAR_FIELDS = [
    "line_item_id",
    "order_line_item_id",
    "master_order_id",
    "master_sale_order_id",
    "delivery_from",
    "customer_status",
    "inventory_status",
    "internal_status",
    "shipping_status",
    "category_code",
    "category_name",
    "item_qty_label",
    "exg_invo_no",
    "exg_invo_date",
    "home_pickup",
    "order_inv_status",
    "slug",
    "product_name",
    "model",
    "erp_item_code",
    "type_of_order",
    "product_hsn",
    "options",
    "delivery_charges",
    "price",
    "brand_code",
    "brand_name",
    "created_by",
    "updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "line_item_id": (StringType(), pa.string(), True),
    "order_line_item_id": (StringType(), pa.string(), True),
    "master_order_id": (StringType(), pa.string(), True),
    "master_sale_order_id": (StringType(), pa.string(), True),

    # 📦 Status / Flow
    "delivery_from": (StringType(), pa.string(), False),
    "customer_status": (StringType(), pa.string(), False),
    "inventory_status": (StringType(), pa.string(), False),
    "internal_status": (StringType(), pa.string(), False),
    "shipping_status": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "home_pickup": (StringType(), pa.string(), False),

    # 🧾 Category / Product
    "category_code": (StringType(), pa.string(), False),
    "category_name": (StringType(), pa.string(), False),
    "product_name": (StringType(), pa.string(), False),
    "model": (StringType(), pa.string(), False),
    "erp_item_code": (StringType(), pa.string(), False),
    "type_of_order": (StringType(), pa.string(), False),
    "product_hsn": (StringType(), pa.string(), False),
    "slug": (StringType(), pa.string(), False),
    "image": (StringType(), pa.string(), False),
    "options": (StringType(), pa.string(), False),

    # 🔢 Quantity / Pricing
    "quantity": (IntegerType(), pa.int32(), False),
    "special_price": (IntegerType(), pa.int32(), False),
    "item_qty_label": (StringType(), pa.string(), False),
    "delivery_charges": (StringType(), pa.string(), False),
    "price": (StringType(), pa.string(), False),

    # 🧾 Invoice / Exchange
    "exg_invo_no": (StringType(), pa.string(), False),
    "exg_invo_date": (StringType(), pa.string(), False),

    # 🏷 Brand
    "brand_code": (StringType(), pa.string(), False),
    "brand_name": (StringType(), pa.string(), False),

    # 📦 JSON payloads (ALL JSON → String)
    "product_policy": (StringType(), pa.string(), False),
    "billed_details": (StringType(), pa.string(), False),
    "delivery_details": (StringType(), pa.string(), False),
    "invoice_details": (StringType(), pa.string(), False),
    "tax_details": (StringType(), pa.string(), False),
    "insurance_details": (StringType(), pa.string(), False),
    "preorder_response": (StringType(), pa.string(), False),
    "seller_details": (StringType(), pa.string(), False),
    "shipment_details": (StringType(), pa.string(), False),
    "return_details": (StringType(), pa.string(), False),
    "return_refund_details": (StringType(), pa.string(), False),
    "return_replace_details": (StringType(), pa.string(), False),
    "return_exchange_details": (StringType(), pa.string(), False),

    # 🔐 Serial / Extra
    "serial_no": (StringType(), pa.string(), False),

    # 🕒 Audit
    "created_at": (TimestampType(), pa.timestamp("ms"), False),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
}

