import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)

REQUIRED_FIELDS = [
    "pickup_delivery_req_item_id",
    "pickup_delivery_req_id",
    "order_id",
    "sale_order_id",
    "expect_delivery_pickup_dt",
    "internal_status",
    "inventory_status",
    "shipment_status",
    "inventory_location_code",
    "item_code",
    "item_name",
    "item_image",
    "item_weight",
    "item_qty",
    "inventory_location",
    "order_line_item_id",
    "picklist_id",
    "picker_id",
    "manifest_id",
    "shipment_id",
    "driver_name",
    "permit",
    "to_location_pincode",
    "to_location_lat_long",
    "row_added_dt",
    "row_added_by",
    "row_updated_by",
    "is_item_installable",
]

VARCHAR_FIELDS = [
    "pickup_delivery_req_item_id",
    "pickup_delivery_req_id",
    "order_id",
    "sale_order_id",
    "invoice_no",
    "invoice_reff_no",
    "invoice_reff_date",
    "customer_status",
    "internal_status",
    "inventory_status",
    "shipment_status",
    "inward_exchange_status",
    "inventory_location_code",
    "docking_area",
    "docking_area_code",
    "type_of_order",
    "item_code",
    "item_name",
    "item_image",
    "item_weight",
    "category_code",
    "category_name",
    "inventory_location",
    "inventory_location_branchname",
    "inventory_location_mobileno",
    "order_type",
    "order_line_item_id",
    "picklist_id",
    "picker_id",
    "manifest_id",
    "shipment_id",
    "driver_code",
    "driver_name",
    "driver_image",
    "driver_contact",
    "assistant_code",
    "assistant_name",
    "assistant_pic",
    "assistant_contact",
    "vehicle_no",
    "vehicle_type",
    "vehicle_name",
    "vehicle_image",
    "order_inv_status",
    "tracking_id",
    "tracking_url",
    "permit",
    "courier_name",
    "courier_details",
    "dimension_units",
    "inventory_location_address",
    "item_price",
    "item_qty_label",
    "item_dimension",
    "line_itemid",
    "invoice_status",
    "exchange_invo_no",
    "exchange_invoice_url",
    "exchange_collected_amount",
    "brand",
    "capture_do_url",
    "shipment_tracking_id",
    "shipping_label_url",
    "secondary_assistant_code",
    "secondary_assistant_name",
    "secondary_assistant_pic",
    "secondary_assistant_contact",
    "pickup_label_id",
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
    "shipment_type",
    "home_pickup",
    "to_location_fullname",
    "courier_code",
    "billing_branch_code",
    "item_serial_no",
    "selling_price",
    "vendor_name",
    "vendor_id",
    "sale_invoice_no",
    "dismantle_label",
    "quote_id",
    "billed_at_branch_name",
    "offer_type",
    "delivery_type",
    "erp_po_createdAt",
    "erp_po_no",
    "seller_order_id",
    "seller_item_code",
    "seller_id",
    "seller_order_createdAt",
    "seller_apob_code",
    "unit_price",
    "discount_price",
    "plant",
    "seller_branch_code",
    "invoice_url",
    "billing_from_branch_code",
    "billing_gst_no",
    "shipping_pincode",
    "shipping_state",
    "customer_email",
    "mapping_domain",
    "payment_email",
    "customer_name",
    "customer_mobile",
    "payment_code",
    "payment_status",
    "delivery_from_branch_code",
]

TIMESTAMP_FIELDS = [
    "invoice_date",
    "expect_delivery_pickup_dt",
    "picker_added_dt",
    "manifest_added_dt",
    "exchange_invo_date",
    "row_added_dt",
    "row_updated_dt",
    "goods_received_date",
    "sale_invoice_date",
]

BOOLEAN_FIELDS = [
    "ispickup_active",
    "otp_verified_status",
    "dropship_flag",
    "preorder",
    "payment_verification",
    "is_item_installable",
]


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