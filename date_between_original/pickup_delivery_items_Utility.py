import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)

REQUIRED_FIELDS = [

    # -----------------------------
    # Primary / Core Identifiers
    # -----------------------------
    "pickup_delivery_req_item_id",
    "pickup_delivery_req_id",
    "order_id",
    "sale_order_id",

    # -----------------------------
    # Dates / Timeline
    # -----------------------------
    "expect_delivery_pickup_dt",

    # -----------------------------
    # Status (NOT NULL)
    # -----------------------------
    "internal_status",
    "inventory_status",
    "shipment_status",

    # -----------------------------
    # Inventory / Location
    # -----------------------------
    "inventory_location_code",
    "inventory_location",

    # -----------------------------
    # Item details
    # -----------------------------
    "item_code",
    "item_name",
    "item_image",
    "item_weight",
    # "item_qty",

    # -----------------------------
    # Order / Pick / Ship
    # -----------------------------
    "order_line_item_id",
    "picklist_id",
    "picker_id",
    "manifest_id",
    "shipment_id",

    # -----------------------------
    # Driver
    # -----------------------------
    "driver_name",

    # -----------------------------
    # Compliance / Ops
    # -----------------------------
    "permit",

    # -----------------------------
    # Flags (tinyint NOT NULL)
    # -----------------------------
    "ispickup_active",

    # -----------------------------
    # Destination
    # -----------------------------
    "to_location_pincode",
    "to_location_lat_long",

    # -----------------------------
    # Audit
    # -----------------------------
    "row_added_dt",
    "row_added_by",
    "row_updated_by",

    # -----------------------------
    # Business rule
    # -----------------------------
    "is_item_installable",
]

VARCHAR_FIELDS = [

    # -----------------------------
    # IDs / Keys
    # -----------------------------
    "pickup_delivery_req_item_id",
    "pickup_delivery_req_id",
    "order_id",
    "sale_order_id",
    "invoice_no",
    "invoice_reff_no",
    "order_line_item_id",
    "picklist_id",
    "picker_id",
    "manifest_id",
    "shipment_id",
    "driver_code",
    "assistant_code",
    "secondary_assistant_code",
    "pickup_label_id",

    # -----------------------------
    # Status / Type fields
    # -----------------------------
    "customer_status",
    "internal_status",
    "inventory_status",
    "shipment_status",
    "inward_exchange_status",
    "order_type",
    "type_of_order",
    "order_inv_status",
    "invoice_status",
    "shipment_type",
    "home_pickup",
    "offer_type",
    "delivery_type",
    "payment_status",
    "payment_code",

    # -----------------------------
    # Item / Product
    # -----------------------------
    "item_code",
    "item_name",
    "item_image",
    "item_weight",
    "category_code",
    "category_name",
    "item_price",
    "item_qty_label",
    "item_dimension",
    "item_serial_no",
    "brand",
    "selling_price",
    "unit_price",
    "discount_price",
    "plant",
    "vendor_name",
    "vendor_id",
    "seller_item_code",
    "seller_id",
    "seller_order_id",

    # -----------------------------
    # Location / Inventory
    # -----------------------------
    "inventory_location_code",
    "inventory_location",
    "inventory_location_branchname",
    "inventory_location_mobileno",
    "inventory_location_address",

    "collection_location_type",
    "collection_location_code",
    "collection_location_branchname",
    "collection_location_address",
    "collection_location_mobileno",
    "collection_docking_area",
    "collection_docking_area_code",

    "to_location_email",
    "to_location_mobileno",
    "to_location_address",
    "to_location_lat_long",
    "to_location_fullname",

    "shipping_pincode",
    "shipping_state",

    # -----------------------------
    # Driver / Vehicle
    # -----------------------------
    "driver_name",
    "driver_image",
    "driver_contact",
    "assistant_name",
    "assistant_pic",
    "assistant_contact",
    "secondary_assistant_name",
    "secondary_assistant_pic",
    "secondary_assistant_contact",

    "vehicle_no",
    "vehicle_type",
    "vehicle_name",
    "vehicle_image",

    # -----------------------------
    # Courier / Tracking
    # -----------------------------
    "courier_name",
    "courier_code",
    "courier_details",
    "tracking_id",
    "tracking_url",
    "shipment_tracking_id",
    "shipping_label_url",

    # -----------------------------
    # Billing / Payment
    # -----------------------------
    "billing_branch_code",
    "billing_from_branch_code",
    "billing_gst_no",
    "billed_at_branch_name",

    "payment_email",
    "customer_email",
    "customer_name",
    "customer_mobile",

    # -----------------------------
    # ERP / Seller
    # -----------------------------
    "erp_po_createdAt",
    "erp_po_no",
    "seller_order_createdAt",
    "seller_apob_code",
    "seller_branch_code",

    # -----------------------------
    # Misc / Textual
    # -----------------------------
    "invoice_reff_date",
    "exchange_invo_no",
    "exchange_invoice_url",
    "exchange_collected_amount",
    "capture_do_url",
    "permit",
    "line_itemid",
    "quote_id",
    "mapping_domain",
    "invoice_url",
]

TIMESTAMP_FIELDS = [
    "invoice_date",
    "expect_delivery_pickup_dt",
    "picker_added_dt",
    "manifest_added_dt",
    "exchange_invo_date",
    "goods_received_date",
    "row_added_dt",
    "row_updated_dt",
]

BOOLEAN_FIELDS = [
    "ispickup_active",
    "otp_verified_status",
    "payment_verification",
    "dropship_flag",
    "preorder",
]

# FIELD_OVERRIDES = {
#
#     # -----------------------------
#     # BOOLEAN / FLAG (tinyint / enum)
#     # -----------------------------
#     "ispickup_active": IntegerType(),
#     "otp_verified_status": IntegerType(),
#     "payment_verification": IntegerType(),
#     "dropship_flag": IntegerType(),
#     "preorder": IntegerType(),
#     "is_item_installable": StringType(),  # enum YES/NO
#
#     # -----------------------------
#     # DATE / TIMESTAMP
#     # -----------------------------
#     "invoice_date": TimestampType(),
#     "expect_delivery_pickup_dt": TimestampType(),
#     "picker_added_dt": TimestampType(),
#     "manifest_added_dt": TimestampType(),
#     "exchange_invo_date": TimestampType(),
#     "goods_received_date": TimestampType(),
#     "row_added_dt": TimestampType(),
#     "row_updated_dt": TimestampType(),
#     "sale_invoice_date": DateType(),
#
#     # -----------------------------
#     # GEO / DECIMAL
#     # -----------------------------
#     "latitude": DoubleType(),    # decimal(10,6)
#     "longitude": DoubleType(),   # decimal(10,6)
#
#     # -----------------------------
#     # NUMERIC
#     # -----------------------------
#     "item_qty": IntegerType(),
#     "dimension_length": IntegerType(),
#     "dimension_height": IntegerType(),
#     "dimension_width": IntegerType(),
#     "reattempt_count": IntegerType(),
#     "delivery_charge": IntegerType(),
#     "adld_charges": IntegerType(),
#     "ew_charges": IntegerType(),
#     "to_location_pincode": IntegerType(),
#
#     # -----------------------------
#     # JSON → STRING (Iceberg-safe)
#     # -----------------------------
#     "picker_details": StringType(),
#     "order_tag": StringType(),
#     "status_notification": StringType(),
#     "shipment_transfer_info": StringType(),
#     "vendor_details": StringType(),
#     "tracking_details": StringType(),
#     "invoiced_item_detail": StringType(),
#     "billing_from_branch_details": StringType(),
#     "delivery_from_branch_details": StringType(),
#     "reject_remarks": StringType(),
#
#     # -----------------------------
#     # TEXT (force string)
#     # -----------------------------
#     "invoice_reff_date": StringType(),
#     "permit": StringType(),
#     "courier_details": StringType(),
#     "inventory_location_address": StringType(),
#     "tracking_url": StringType(),
#     "exchange_invoice_url": StringType(),
#     "shipping_label_url": StringType(),
#     "invoice_url": StringType(),
# }

FIELD_OVERRIDES = {

    # -----------------------------
    # BOOLEAN / FLAG (tinyint / enum)
    # -----------------------------
    "ispickup_active": (BooleanType(), pa.bool_(), False),
    "otp_verified_status": (BooleanType(), pa.bool_(), False),
    "payment_verification": (BooleanType(), pa.bool_(), False),
    "dropship_flag": (BooleanType(), pa.bool_(), False),
    "preorder": (BooleanType(), pa.bool_(), False),
    "is_item_installable": (StringType(), pa.string(), False),  # enum YES/NO

    # -----------------------------
    # DATE / TIMESTAMP
    # -----------------------------
    "invoice_date": (TimestampType(), pa.timestamp("ms"), False),
    "expect_delivery_pickup_dt": (TimestampType(), pa.timestamp("ms"), False),
    "picker_added_dt": (TimestampType(), pa.timestamp("ms"), False),
    "manifest_added_dt": (TimestampType(), pa.timestamp("ms"), False),
    "exchange_invo_date": (TimestampType(), pa.timestamp("ms"), False),
    "goods_received_date": (TimestampType(), pa.timestamp("ms"), False),
    "row_added_dt": (TimestampType(), pa.timestamp("ms"), False),
    "row_updated_dt": (TimestampType(), pa.timestamp("ms"), False),
    "sale_invoice_date": (StringType(), pa.string(), False),  # inconsistent format → keep string

    # -----------------------------
    # GEO / DECIMAL
    # -----------------------------
    "latitude": (StringType(), pa.string(), False),    # decimal(10,6)
    "longitude": (StringType(), pa.string(), False),   # decimal(10,6)

    # -----------------------------
    # NUMERIC
    # -----------------------------
    "item_qty": (StringType(), pa.string(), False),
    "dimension_length": (StringType(), pa.string(), False),
    "dimension_height": (StringType(), pa.string(), False),
    "dimension_width": (StringType(), pa.string(), False),
    "reattempt_count": (StringType(), pa.string(), False),
    "delivery_charge": (StringType(), pa.string(), False),
    "adld_charges": (IntegerType(), pa.int32(), False),
    "ew_charges": (IntegerType(), pa.int32(), False),
    "to_location_pincode": (StringType(), pa.string(), False),

    # -----------------------------
    # JSON → STRING (Iceberg-safe)
    # -----------------------------
    "picker_details": (StringType(), pa.string(), False),
    "order_tag": (StringType(), pa.string(), False),
    "status_notification": (StringType(), pa.string(), False),
    "shipment_transfer_info": (StringType(), pa.string(), False),
    "vendor_details": (StringType(), pa.string(), False),
    "tracking_details": (StringType(), pa.string(), False),
    "invoiced_item_detail": (StringType(), pa.string(), False),
    "billing_from_branch_details": (StringType(), pa.string(), False),
    "delivery_from_branch_details": (StringType(), pa.string(), False),
    "reject_remarks": (StringType(), pa.string(), False),

    # -----------------------------
    # TEXT (force string)
    # -----------------------------
    "invoice_reff_date": (StringType(), pa.string(), False),
    "permit": (StringType(), pa.string(), False),
    "courier_details": (StringType(), pa.string(), False),
    "inventory_location_address": (StringType(), pa.string(), False),
    "tracking_url": (StringType(), pa.string(), False),
    "exchange_invoice_url": (StringType(), pa.string(), False),
    "shipping_label_url": (StringType(), pa.string(), False),
    "invoice_url": (StringType(), pa.string(), False),

# -----------------------------
    # Missing fields (string-safe)
    # -----------------------------
    "plant": (StringType(), pa.string(), False),
    "seller_branch_code": (StringType(), pa.string(), False),
    "tracking_details": (StringType(), pa.string(), False),
    "invoiced_item_detail": (StringType(), pa.string(), False),
    "invoice_url": (StringType(), pa.string(), False),

    "billing_from_branch_code": (StringType(), pa.string(), False),
    "billing_from_branch_details": (StringType(), pa.string(), False),
    "billing_gst_no": (StringType(), pa.string(), False),

    "shipping_pincode": (StringType(), pa.string(), False),
    "shipping_state": (StringType(), pa.string(), False),

    "customer_email": (StringType(), pa.string(), False),
    "mapping_domain": (StringType(), pa.string(), False),
    "payment_email": (StringType(), pa.string(), False),

    "customer_name": (StringType(), pa.string(), False),
    "customer_mobile": (StringType(), pa.string(), False),

    "payment_code": (StringType(), pa.string(), False),
    "payment_status": (StringType(), pa.string(), False),

    "delivery_from_branch_details": (StringType(), pa.string(), False),
    "delivery_from_branch_code": (StringType(), pa.string(), False),

    "adld_charges": (StringType(), pa.string(), False),
    "ew_charges": (StringType(), pa.string(), False),

    "reject_remarks": (StringType(), pa.string(), False),
}