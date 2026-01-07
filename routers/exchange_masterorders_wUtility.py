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
    "order_tag", "customer_address", "shipment_address", "billing_address",
    "payment_details", "refund_details", "voucher_details", 
    "employee_sale_details", "order_summary_details", "other_details",
    "lineitems", "lineitem_status", "service_details"
]

TIMESTAMP_FIELDS = ["invoice_date", "created_at", "updated_at"]

DECIMAL_FIELDS = ["latitude", "longitude"]

REQUIRED_FIELDS = ["order_id", "sale_order_id"]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys / Required
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # Nullable Strings/Varchar
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False), # text in MySQL
    "channel": (StringType(), pa.string(), False), # enum
    "channel_medium": (StringType(), pa.string(), False), # enum
    "order_status": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "order_type": (StringType(), pa.string(), False),
    "delivery_from": (StringType(), pa.string(), False), # enum
    "delivery_from_branchcode": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),
    "cust_id": (StringType(), pa.string(), False),
    "cust_primary_email": (StringType(), pa.string(), False),
    "cust_primary_contact": (StringType(), pa.string(), False),
    "cust_mobile": (StringType(), pa.string(), False),
    "invoice_pdf": (StringType(), pa.string(), False), # text in MySQL
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # JSON fields (mapped to String)
    "order_tag": (StringType(), pa.string(), False),
    "customer_address": (StringType(), pa.string(), False),
    "shipment_address": (StringType(), pa.string(), False),
    "billing_address": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),
    "refund_details": (StringType(), pa.string(), False),
    "voucher_details": (StringType(), pa.string(), False),
    "employee_sale_details": (StringType(), pa.string(), False),
    "order_summary_details": (StringType(), pa.string(), False),
    "other_details": (StringType(), pa.string(), False),
    "lineitems": (StringType(), pa.string(), False),
    "lineitem_status": (StringType(), pa.string(), False),
    "service_details": (StringType(), pa.string(), False),

    # Decimal fields (mapped to Double/Float64)
    "latitude": (DoubleType(), pa.float64(), False),
    "longitude": (DoubleType(), pa.float64(), False),

    # Timestamp fields
    "invoice_date": (TimestampType(), pa.timestamp('ms'), False),
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
}


def exchange_masterorders_w_schema(record: Dict[str, Any]) -> Tuple[Schema, pa.Schema]:
    """
    Generate Iceberg and Arrow schemas for exchange_masterorders_w table.
    
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


def exchange_masterorders_w_clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for exchange_masterorders_w schema compliance.
    
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
        # 1. Decimal/Float Fields
        for f in DECIMAL_FIELDS:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = float(val)
                except ValueError:
                    logger.warning(f"Invalid float value for {f}: {val}, defaulting to 0.0")
                    row[f] = 0.0
            elif val is None:
                row[f] = 0.0 # Default to 0.0 or None? Schema says Nullable=True, but overrides say False?
                # Actually, in overrides I set required=False for clean generation, but cleaning logic
                # might often want defaults for numeric fields to avoid downstream issues.
                # Let's keep nullable=True in schema (overrides say False required), allowing None.
                # So if None, we can keep None if compatible.
                # However, previous utilities used defaults. Let's stick to None if nullable is true.
                # But looking at FIELD_OVERRIDES, I set required=False.
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
            if key not in JSON_FIELDS + TIMESTAMP_FIELDS + DECIMAL_FIELDS:
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
