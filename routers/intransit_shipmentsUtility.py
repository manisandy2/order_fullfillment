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
REQUIRED_FIELDS = ['t_shipment_id', 'time_sorted_id', 'shipment_type', 'provider', 'collection_branch_code', 'status', 'created_at', 'createdAt']
TIMESTAMP_FIELDS = ['created_at', 'updated_at', 'createdAt', 'updatedAt']
BOOLEAN_FIELDS = []
INTEGER_FIELDS = ['dimension_length', 'dimension_height', 'dimension_width']
VARCHAR_FIELDS = ['t_shipment_id', 'time_sorted_id', 'shipment_type', 'fulfiled_by',
                  'provider', 'branch_code', 'collection_branch_code', 'status',
                  'vehicle_no', 'vehicle_type', 'vehicle_image', 'vehicle_name',
                  'driver_code', 'driver_name', 'driver_image', 'driver_contact',
                  'assistant_code', 'assistant_name', 'assistant_pic',
                  'assistant_contact', 'secondary_assistant_code', 'secondary_assistant_name',
                  'secondary_assistant_pic', 'secondary_assistant_contact',
                  'tracking_id', 'tracking_url', 'courier_name', 'dimension_units',
                  'permit', 'created_by', 'updated_by', 'courier_code']


FIELD_OVERRIDES = {

    # 🔑 Primary / Identifiers
    "t_shipment_id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),

    # 📦 Shipment Details
    "shipment_type": (StringType(), pa.string(), True),
    "fulfiled_by": (StringType(), pa.string(), False),
    "provider": (StringType(), pa.string(), True),

    # 🏬 Branch
    "branch_code": (StringType(), pa.string(), False),
    "collection_branch_code": (StringType(), pa.string(), True),

    # 📌 Status
    "status": (StringType(), pa.string(), True),

    # 🚗 Vehicle
    "vehicle_no": (StringType(), pa.string(), False),
    "vehicle_type": (StringType(), pa.string(), False),
    "vehicle_name": (StringType(), pa.string(), False),
    "vehicle_image": (StringType(), pa.string(), False),

    # 🧑 Driver
    "driver_code": (StringType(), pa.string(), False),
    "driver_name": (StringType(), pa.string(), False),
    "driver_image": (StringType(), pa.string(), False),
    "driver_contact": (StringType(), pa.string(), False),

    # 🧑 Assistant
    "assistant_code": (StringType(), pa.string(), False),
    "assistant_name": (StringType(), pa.string(), False),
    "assistant_pic": (StringType(), pa.string(), False),
    "assistant_contact": (StringType(), pa.string(), False),

    # 🧑 Secondary Assistant
    "secondary_assistant_code": (StringType(), pa.string(), False),
    "secondary_assistant_name": (StringType(), pa.string(), False),
    "secondary_assistant_pic": (StringType(), pa.string(), False),
    "secondary_assistant_contact": (StringType(), pa.string(), False),

    # 📦 Tracking
    "tracking_id": (StringType(), pa.string(), False),
    "tracking_url": (StringType(), pa.string(), False),
    "courier_name": (StringType(), pa.string(), False),
    "courier_code": (StringType(), pa.string(), False),

    # 📐 Dimensions
    "dimension_length": (IntegerType(), pa.int32(), False),
    "dimension_height": (IntegerType(), pa.int32(), False),
    "dimension_width": (IntegerType(), pa.int32(), False),
    "dimension_units": (StringType(), pa.string(), False),

    # 📄 Permit
    "permit": (StringType(), pa.string(), False),

    # 🕒 Audit (snake_case)
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),

    # 🕒 Audit (camelCase)
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), False),

    # 👤 Audit User
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
