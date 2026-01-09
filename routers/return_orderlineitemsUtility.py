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
    "line_item_id",
    "order_line_item_id",
    "master_order_id",
    "master_sale_order_id",
]

TIMESTAMP_FIELDS = [
    "created_at",
    "updated_at",
]

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
