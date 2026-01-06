import logging
import json
from typing import Dict, List, Tuple, Any
import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField
)
from datetime import datetime, date
from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

# Module-level constants
JSON_FIELDS = [
    "product_policy", "billed_details", "delivery_details",
    "invoice_details", "tax_details", "insurance_details",
    "preorder_response", "seller_details", "shipment_details",
    "return_details", "return_refund_details", "return_replace_details",
    "return_exchange_details"
]

TIMESTAMP_FIELDS = ["created_at", "updated_at"]

INTEGER_FIELDS = ["quantity", "special_price"]

REQUIRED_FIELDS = ["line_item_id", "order_line_item_id", "master_order_id", "master_sale_order_id"]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys / Required
    "line_item_id": (StringType(), pa.string(), True),
    "order_line_item_id": (StringType(), pa.string(), True),
    "master_order_id": (StringType(), pa.string(), True),
    "master_sale_order_id": (StringType(), pa.string(), True),

    # Nullable Strings/Varchar
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
    "image": (StringType(), pa.string(), False), # text
    "options": (StringType(), pa.string(), False),
    "delivery_charges": (StringType(), pa.string(), False), # varchar(50)
    "price": (StringType(), pa.string(), False), # varchar(50)
    "brand_code": (StringType(), pa.string(), False),
    "brand_name": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
    "serial_no": (StringType(), pa.string(), False), # text

    # Integer fields
    "quantity": (LongType(), pa.int64(), False), # int(11)
    "special_price": (LongType(), pa.int64(), False), # int(11)

    # JSON fields (mapped to String)
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


def exchange_orderlineitems_schema(record: Dict[str, Any]) -> Tuple[Schema, pa.Schema]:
    """
    Generate Iceberg and Arrow schemas for exchange_orderlineitems table.
    
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


def exchange_orderlineitems_clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for exchange_orderlineitems schema compliance.
    
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
        # 1. Integer Fields
        for f in INTEGER_FIELDS:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    logger.warning(f"Invalid integer value for {f}: {val}, defaulting to 0")
                    row[f] = 0
            elif val is None:
                row[f] = 0 # Default to 0 for int(11) nullable? Logic varies, but usually 0 is safer.
                # However, schema says nullable=True.
                # If we want to preserve NULLs for analysis:
                row[f] = None
        
        # 2. JSON Fields
        for f in JSON_FIELDS:
            val = row.get(f)
            if val is None:
                row[f] = None
            elif isinstance(val, (dict, list)):
                try:
                    row[f] = json.dumps(val)
                except TypeError:
                    logger.warning(f"Failed to serialize JSON for {f}, using string representation")
                    row[f] = str(val)
            else:
                row[f] = str(val)

        # 3. Timestamp Fields
        for f in TIMESTAMP_FIELDS:
            val = row.get(f)
            
            if val is None or val == "":
                row[f] = None
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
                logger.warning(f"Failed to parse timestamp {f}: {val}")
                row[f] = None
            else:
                row[f] = parsed

        # 4. String Fields (Everything else)
        for key, val in row.items():
            if key not in JSON_FIELDS + TIMESTAMP_FIELDS + INTEGER_FIELDS:
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
