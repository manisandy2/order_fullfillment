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
    "pickup_delivery_req_id",
]

TIMESTAMP_FIELDS = [
    "invoice_date",
    "expected",
    "row_added_dttm",
    "row_updated_dttm",
    "updated_at_new",
]

BOOLEAN_FIELDS = [
    "is_accepted",
]

INTEGER_FIELDS = [
    "oms_data_migration_status",
]

VARCHAR_FIELDS = [
    "pickup_delivery_req_id",
    "order_id",
    "sale_order_id",
    "invoice_no",
    "invoice_reff_no",
    "request_type",
    "invoice_status",
    "order_type",
    "order_inv_status",
    "row_added_by",
    "row_updated_by",
]

FIELD_OVERRIDES = {

    # 🔑 Primary Key
    "pickup_delivery_req_id": (StringType(), pa.string(), True),

    # 🧾 Order / Invoice
    "order_id": (StringType(), pa.string(), False),
    "sale_order_id": (StringType(), pa.string(), False),
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_date": (TimestampType(), pa.timestamp("ms"), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),
    "invoice_status": (StringType(), pa.string(), False),

    # 📦 Request Details
    "request_type": (StringType(), pa.string(), False),
    "inventory_details": (StringType(), pa.string(), False),
    "return_details": (StringType(), pa.string(), False),
    "collection_details": (StringType(), pa.string(), False),
    "to_details": (StringType(), pa.string(), False),
    "shipment_details": (StringType(), pa.string(), False),
    "order_details": (StringType(), pa.string(), False),

    # ⏱ Expected / Status
    "expected": (TimestampType(), pa.timestamp("ms"), False),
    "is_accepted": (IntegerType(), pa.int32(), False),
    "rejection_reason": (StringType(), pa.string(), False),

    # 🌍 Geo
    "latitude": (DoubleType(), pa.float64(), False),
    "longitude": (DoubleType(), pa.float64(), False),

    # 📊 Order State
    "order_type": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),

    # 🕒 Audit
    "row_added_dttm": (TimestampType(), pa.timestamp("ms"), False),
    "row_updated_dttm": (TimestampType(), pa.timestamp("ms"), False),
    "updated_at_new": (TimestampType(), pa.timestamp("ms"), False),
    "row_added_by": (StringType(), pa.string(), False),
    "row_updated_by": (StringType(), pa.string(), False),

    # ⚙ Migration / System
    "oms_data_migration_status": (IntegerType(), pa.int32(), False),
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
