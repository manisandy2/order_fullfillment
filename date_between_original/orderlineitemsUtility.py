import pyarrow as pa
from pyiceberg.types import *
from datetime import datetime, date
from pyiceberg.schema import Schema


FIELD_OVERRIDES = {

    # PRIMARY KEY
    "line_item_id": (StringType(), pa.string(), True),

    # INTEGER FIELDS
    "quantity": (IntegerType(), pa.int32(), False),
    "special_price": (IntegerType(), pa.int32(), False),
    "oms_data_migration_status": (IntegerType(), pa.int32(), False),

    # TIMESTAMP FIELDS
    "created_at": (TimestampType(), pa.timestamp("ms"), False),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),

    # JSON FIELDS (store as STRING for Iceberg safety)
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

    # STRING FIELDS (varchar/text)
    "order_line_item_id": (StringType(), pa.string(), False),
    "master_order_id": (StringType(), pa.string(), False),
    "master_sale_order_id": (StringType(), pa.string(), False),
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
    "offer_type": (StringType(), pa.string(), False),
    # "delivery_type": (StringType(), pa.string(), False),
}




BOOLEAN_FIELDS = []
FLOAT_FIELDS = ["latitude","longitude"]
INTEGER_FIELDS = ["quantity", "special_price", "oms_data_migration_status"]
DATE_FIELDS = []
# DOUBLE_FIELDS = ["latitude","longitude"]
TIMESTAMP_FIELDS = ["created_at", "updated_at"]
STRING_FIELDS = [
        "line_item_id", "order_line_item_id", "master_order_id", "master_sale_order_id",
        "delivery_from", "customer_status", "inventory_status", "internal_status", "shipping_status",
        "category_code", "category_name", "item_qty_label", "exg_invo_no", "exg_invo_date",
        "home_pickup", "order_inv_status", "slug", "product_name", "model", "erp_item_code",
        "product_hsn", "image", "options", "delivery_charges", "price", "brand_code", "brand_name",
        "created_by", "updated_by", "serial_no", "offer_type",

        # JSON fields stored as string
        "product_policy", "billed_details", "delivery_details", "invoice_details",
        "tax_details", "insurance_details", "preorder_response", "seller_details",
        "shipment_details", "return_details", "return_refund_details", "return_replace_details",
        "return_exchange_details"
    ]



