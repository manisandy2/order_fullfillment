import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
INTEGER_FIELDS = ["quantity", "special_price"]
BOOLEAN_FIELDS = []
JSON_FIELDS = [
    "product_policy",
    "billed_details",
    "delivery_details",
    "invoice_details",
    "tax_details",
    "insurance_details",
    "preorder_response",
    "seller_details",
    "shipment_details",
    "return_details",
    "return_refund_details",
    "return_replace_details",
    "return_exchange_details"
]
DATE_FIELDS = []
TIMESTAMP_FIELDS = ["created_at", "updated_at"]
REQUIRED_FIELDS = [
    "line_item_id", 
    "order_line_item_id", 
    "master_order_id", 
    "master_sale_order_id"
]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys & Varchars
    "line_item_id": (StringType(), pa.string(), True), # PRI, not null
    "order_line_item_id": (StringType(), pa.string(), True),
    "master_order_id": (StringType(), pa.string(), True),
    "master_sale_order_id": (StringType(), pa.string(), True),
    "delivery_from": (StringType(), pa.string(), False),
    "customer_status": (StringType(), pa.string(), False),
    "inventory_status": (StringType(), pa.string(), False),
    "internal_status": (StringType(), pa.string(), False),
    "shipping_status": (StringType(), pa.string(), False),
    "category_code": (StringType(), pa.string(), False),
    "category_name": (StringType(), pa.string(), False),
    "item_qty_label": (StringType(), pa.string(), False),
    "exg_invo_no": (StringType(), pa.string(), False),
    "exg_invo_date": (StringType(), pa.string(), False),
    "home_pickup": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "slug": (StringType(), pa.string(), False),
    "product_name": (StringType(), pa.string(), False),
    "model": (StringType(), pa.string(), False),
    "erp_item_code": (StringType(), pa.string(), False),
    "type_of_order": (StringType(), pa.string(), False),
    "product_hsn": (StringType(), pa.string(), False),
    "image": (StringType(), pa.string(), False),
    "options": (StringType(), pa.string(), False),
    "delivery_charges": (StringType(), pa.string(), False),
    "price": (StringType(), pa.string(), False),
    "brand_code": (StringType(), pa.string(), False),
    "brand_name": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
    "serial_no": (StringType(), pa.string(), False),

    # Integers (int(11))
    "quantity": (LongType(), pa.int32(), False),
    "special_price": (LongType(), pa.int32(), False),

    # JSON fields
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

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
}
