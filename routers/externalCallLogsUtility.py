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
JSON_FIELDS = ["res_obj"]

TIMESTAMP_FIELDS = ["created_at"]

REQUIRED_FIELDS = ["id"]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys / Required
    "id": (StringType(), pa.string(), True),

    # Nullable Strings/Varchar
    "ext_log_id": (StringType(), pa.string(), False),
    "short_code": (StringType(), pa.string(), False),
    "invoice_no": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),

    # JSON fields (mapped to String)
    "res_obj": (StringType(), pa.string(), False),

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
}


def external_call_logs_schema(record: Dict[str, Any]) -> Tuple[Schema, pa.Schema]:
    """
    Generate Iceberg and Arrow schemas for externalcalllogs table.
    
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


def external_call_logs_clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for externalcalllogs schema compliance.
    
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
        # 1. JSON Fields
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

        # 2. Timestamp Fields
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

        # 3. String Fields (Everything else)
        for key, val in row.items():
            if key not in JSON_FIELDS + TIMESTAMP_FIELDS:
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
