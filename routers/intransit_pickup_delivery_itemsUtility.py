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


#
REQUIRED_FIELDS = ['intransit_pickupdelivery_id', 'pickup_delivery_req_item_id', 'pickup_delivery_req_id',
                   'order_id', 'sale_order_id', 'expect_delivery_pickup_dt', 'internal_status',
                   'inventory_status', 'shipment_status', 'inventory_location_code',
                   "permit","ispickup_active",
                   'item_code', 'item_name', 'item_image', 'item_weight', 'item_qty',
                   'inventory_location','picker_id',
                   't_manifest_id', 't_shipment_id', 'driver_name', 'ispickup_active',
                   'to_location_pincode', 'to_location_lat_long', 'row_added_dt', 'row_added_by']

TIMESTAMP_FIELDS = ['invoice_date', 'expect_delivery_pickup_dt', 'picker_added_dt',
                    'manifest_added_dt', 'exchange_invo_date', 'row_added_dt', 'row_updated_dt',
                    'goods_received_date']
BOOLEAN_FIELDS = ["ispickup_active","otp_verified_status"]
INTEGER_FIELDS = ['item_qty', 'dimension_length', 'dimension_height', 'dimension_width',
                  'reattempt_count', 'to_location_pincode']

VARCHAR_FIELDS = ['intransit_pickupdelivery_id', 'pickup_delivery_req_item_id',
                  'pickup_delivery_req_id', 'order_id', 'sale_order_id', 'invoice_no',
                  'invoice_reff_no', 'invoice_reff_date', 'customer_status', 'internal_status',
                  'inventory_status', 'shipment_status', 'inventory_location_code', 'docking_area',
                  'docking_area_code', 'type_of_order', 'item_code', 'item_name', 'item_image',
                  'item_weight', 'category_code', 'category_name', 'inventory_location',
                  'inventory_location_branchname', 'inventory_location_mobileno', 'order_type',
                  'order_line_item_id', 'picklist_id', 'picker_id', 't_manifest_id',
                  't_shipment_id', 'driver_code', 'driver_name', 'driver_image',
                  'driver_contact', 'assistant_code', 'assistant_name', 'assistant_pic',
                  'assistant_contact', 'vehicle_no', 'vehicle_type', 'vehicle_name',
                  'vehicle_image', 'order_inv_status', 'tracking_id', 'tracking_url',
                  'permit', 'courier_name', 'courier_details', 'dimension_units',
                  'inventory_location_address', 'item_price', 'item_qty_label',
                  'item_dimension', 'line_itemid', 'invoice_status', 'exchange_invo_no',
                  'exchange_invoice_url', 'exchange_collected_amount', 'brand',
                  'capture_do_url', 'shipment_tracking_id', 'shipping_label_url',
                  'secondary_assistant_code', 'secondary_assistant_name',
                  'secondary_assistant_pic', 'secondary_assistant_contact',
                  'pickup_label_id', 'collection_location_type', 'collection_location_code',
                  'collection_location_branchname', 'collection_location_address',
                  'collection_location_mobileno', 'collection_docking_area',
                  'collection_docking_area_code', 'to_location_fullname',
                  'to_location_email', 'to_location_mobileno', 'to_location_address',
                  'to_location_lat_long', 'shipment_type', 'home_pickup', 'row_added_by',
                  'row_updated_by', 'courier_code', 'billing_branch_code', 'item_serial_no',
                  'selling_price', 'vendor_name', 'vendor_id', 'sale_invoice_no', 'quote_id']



FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "intransit_pickupdelivery_id": (StringType(), pa.string(), True),
    "pickup_delivery_req_item_id": (StringType(), pa.string(), True),
    "pickup_delivery_req_id": (StringType(), pa.string(), True),
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # 📄 Invoice
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_date": (TimestampType(), pa.timestamp("ms"), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),
    "invoice_status": (StringType(), pa.string(), False),

    # 📦 Delivery / Status
    "expect_delivery_pickup_dt": (TimestampType(), pa.timestamp("ms"), True),
    "customer_status": (StringType(), pa.string(), False),
    "internal_status": (StringType(), pa.string(), True),
    "inventory_status": (StringType(), pa.string(), False),
    "shipment_status": (StringType(), pa.string(), True),
    "shipment_type": (StringType(), pa.string(), False),

    # 🏬 Inventory Location
    "inventory_location_code": (StringType(), pa.string(), True),
    "inventory_location": (StringType(), pa.string(), True),
    "inventory_location_branchname": (StringType(), pa.string(), False),
    "inventory_location_mobileno": (StringType(), pa.string(), False),
    "inventory_location_address": (StringType(), pa.string(), False),
    "order_line_item_id": (StringType(), pa.string(), False),
    "picklist_id": (StringType(), pa.string(), False),
    "permit": (StringType(), pa.string(), True),
    # 🚚 Docking / Collection
    "docking_area": (StringType(), pa.string(), False),
    "docking_area_code": (StringType(), pa.string(), False),
    "collection_location_type": (StringType(), pa.string(), False),
    "collection_location_code": (StringType(), pa.string(), False),
    "collection_location_branchname": (StringType(), pa.string(), False),
    "collection_location_address": (StringType(), pa.string(), False),
    "collection_location_mobileno": (StringType(), pa.string(), False),
    "collection_docking_area": (StringType(), pa.string(), False),
    "collection_docking_area_code": (StringType(), pa.string(), False),

    # 📍 Destination
    "to_location_fullname": (StringType(), pa.string(), False),
    "to_location_email": (StringType(), pa.string(), False),
    "to_location_mobileno": (StringType(), pa.string(), False),
    "to_location_address": (StringType(), pa.string(), False),
    "to_location_pincode": (IntegerType(), pa.int32(), True),
    "to_location_lat_long": (StringType(), pa.string(), False),

    # 🧾 Item Details
    "item_code": (StringType(), pa.string(), True),
    "item_name": (StringType(), pa.string(), True),
    "item_image": (StringType(), pa.string(), True),
    "item_weight": (StringType(), pa.string(), True),
    "item_qty": (IntegerType(), pa.int32(), True),
    "item_price": (StringType(), pa.string(), False),
    "item_dimension": (StringType(), pa.string(), False),
    "item_qty_label": (StringType(), pa.string(), False),
    "item_serial_no": (StringType(), pa.string(), False),

    # 📦 Category
    "category_code": (StringType(), pa.string(), False),
    "category_name": (StringType(), pa.string(), False),

    # 🧑‍🔧 Picker / Driver
    "picker_id": (StringType(), pa.string(), True),
    "picker_details": (StringType(), pa.string(), False),
    "picker_added_dt": (TimestampType(), pa.timestamp("ms"), False),
    "driver_code": (StringType(), pa.string(), False),
    "driver_name": (StringType(), pa.string(), False),
    "driver_contact": (StringType(), pa.string(), False),

    # 🚗 Vehicle
    "vehicle_no": (StringType(), pa.string(), False),
    "vehicle_type": (StringType(), pa.string(), False),
    "vehicle_name": (StringType(), pa.string(), False),
    "vehicle_image": (StringType(), pa.string(), False),

    # 🧾 Manifest / Shipment
    "t_manifest_id": (StringType(), pa.string(), True),
    "manifest_added_dt": (TimestampType(), pa.timestamp("ms"), False),
    "t_shipment_id": (StringType(), pa.string(), True),
    "shipment_tracking_id": (StringType(), pa.string(), False),
    "tracking_id": (StringType(), pa.string(), False),
    "tracking_url": (StringType(), pa.string(), False),
    "tracking_details": (StringType(), pa.string(), False),

    # 📐 Dimensions / Geo
    "dimension_length": (IntegerType(), pa.int32(), False),
    "dimension_height": (IntegerType(), pa.int32(), False),
    "dimension_width": (IntegerType(), pa.int32(), False),
    "dimension_units": (StringType(), pa.string(), False),
    "latitude": (FloatType(), pa.float64(), False),
    "longitude": (FloatType(), pa.float64(), False),

    # 🏷 Vendor / Billing
    "vendor_name": (StringType(), pa.string(), False),
    "vendor_id": (StringType(), pa.string(), False),
    "vendor_details": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),

    # 🕒 Audit
    "row_added_dt": (TimestampType(), pa.timestamp("ms"), True),
    "row_updated_dt": (TimestampType(), pa.timestamp("ms"), False),
    "row_added_by": (StringType(), pa.string(), True),
    "row_updated_by": (StringType(), pa.string(), False),

    # 💰 Sale / Exchange
    "sale_invoice_no": (StringType(), pa.string(), False),
    "sale_invoice_date": (DateType(), pa.date32(), False),
    "exchange_invo_no": (StringType(), pa.string(), False),
    "exchange_invo_date": (TimestampType(), pa.timestamp("ms"), False),
    "exchange_invoice_url": (StringType(), pa.string(), False),
    "exchange_collected_amount": (StringType(), pa.string(), False),

    # ⚙ Flags
    "otp_verified_status": (BooleanType(), pa.bool_(), False),
    "ispickup_active": (BooleanType(), pa.bool_(), True),
    "reattempt_count": (IntegerType(), pa.int32(), False),
}



def schema(record: Dict[str, Any]) -> Tuple[Schema, pa.Schema]:
    """
    Generate Iceberg and Arrow schemas for installation_services table.
    
    Args:
        record: Sample record dictionary
        
    Returns:
        Tuple of (Iceberg Schema, Arrow Schema)
        
    Raises:
        ValueError: If required fields are missing
    """
    # Validate required fields
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    
    iceberg_fields = []
    arrow_fields = []

    # Sort for deterministic field IDs
    sorted_items = sorted(record.items())
    
    for idx, (name, value) in enumerate(sorted_items, start=1):
        if name in FIELD_OVERRIDES:
            ice_type, arrow_type, required = FIELD_OVERRIDES[name]
        else:
            required = False
            
            # Boolean
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()
            
            # Integer
            elif isinstance(value, int):
                ice_type, arrow_type = LongType(), pa.int64()
            
            # Float
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()
            
            # Date only
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()
            
            # Timestamp
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp("ms")
            
            # String (default)
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema


def clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for hub_masters schema compliance.
    
    Args:
        rows: List of row dictionaries
        
    Returns:
        Cleaned list of row dictionaries
    """
    dt_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
    ]

    for row in rows:
        # 1. Boolean Fields
        for f in BOOLEAN_FIELDS:
            val = row.get(f)
            if val is None:
                # Schema says NOT NULL, so default to False or True?
                # Usually False (0) is a safe default for flags.
                logger.warning(f"Required boolean field {f} is None, defaulting to False")
                row[f] = False
            elif isinstance(val, bool):
                row[f] = val
            elif isinstance(val, int):
                row[f] = bool(val)
            elif isinstance(val, str):
                row[f] = val.lower() in ("1", "true", "yes", "on")
            else:
                row[f] = False

        # 2. Timestamp Fields
        for f in TIMESTAMP_FIELDS:
            val = row.get(f)
            
            if val is None or val == "":
                # Timestamps are NOT NULL with DEFAULT CURRENT_TIMESTAMP in schema.
                # So we must provide a value if missing.
                logger.info(f"Required timestamp {f} is None, using current timestamp")
                row[f] = datetime.now()
                continue

            if isinstance(val, datetime):
                continue

            # Try multiple formats
            parsed = None
            for fmt in dt_formats:
                try:
                    parsed = datetime.strptime(val, fmt)
                    break
                except (ValueError, TypeError):
                    pass

            if parsed is None:
                logger.warning(f"Failed to parse timestamp {f}: {val}, using current timestamp")
                row[f] = datetime.now()
            else:
                row[f] = parsed

        # 3. String Fields (Everything else)
        for key, val in row.items():
            if key not in BOOLEAN_FIELDS + TIMESTAMP_FIELDS:
                 # Check if this field override exists and is required
                if key in FIELD_OVERRIDES:
                    _, _, is_required = FIELD_OVERRIDES[key]
                    if val is None:
                        if is_required:
                            logger.warning(f"Required string field {key} is None, defaulting to empty string")
                            row[key] = ""
                        else:
                            row[key] = None
                    else:
                        row[key] = str(val)
                else:
                    # Generic handling for non-overridden fields
                    row[key] = str(val) if val is not None else None

    return rows
