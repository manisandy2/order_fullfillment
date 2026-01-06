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
INTEGER_FIELDS = []
BOOLEAN_FIELDS = ["isactive", "isEditable", "isApiIntegrated"]
JSON_FIELDS = ["branch_list", "branch_code_list"]
TIMESTAMP_FIELDS = ["created_at", "updated_at"]
REQUIRED_FIELDS = [
    "code", "created_at", "isEditable", "isApiIntegrated"
]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys & Varchars
    "code": (StringType(), pa.string(), True),
    "name": (StringType(), pa.string(), False),
    "type": (StringType(), pa.string(), False),
    "image": (StringType(), pa.string(), False),
    "description": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # Boolean fields (tinyint(1))
    "isactive": (BooleanType(), pa.bool_(), False),
    "isEditable": (BooleanType(), pa.bool_(), True),
    "isApiIntegrated": (BooleanType(), pa.bool_(), True),

    # JSON fields
    "branch_list": (StringType(), pa.string(), False),
    "branch_code_list": (StringType(), pa.string(), False),

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), True),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
}


def courier_masters_schema(record: Dict[str, Any]) -> Tuple[Schema, pa.Schema]:
    """
    Generate Iceberg and Arrow schemas for courier_masters table.
    
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


def courier_masters_clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for courier_masters schema compliance.
    
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
                # Pincode is required
                if f == "pincode":
                    logger.error(f"Required field {f} is None, defaulting to 0")
                    row[f] = 0
                else:
                    row[f] = None

        # 2. Boolean Fields (tinyint(1))
        for f in BOOLEAN_FIELDS:
            val = row.get(f)
            if val is None:
                row[f] = False # Default 0 as per schema
            elif isinstance(val, bool):
                row[f] = val
            elif isinstance(val, int):
                row[f] = bool(val)
            elif isinstance(val, str):
                row[f] = val.lower() in ("1", "true", "yes", "on")
            else:
                row[f] = False

        # 3. JSON Fields
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

        # 4. Timestamp Fields
        for f in TIMESTAMP_FIELDS:
            val = row.get(f)
            
            # Check if required (e.g., created_at)
            is_required = f in ["created_at"]

            if val is None or val == "":
                if is_required:
                    # Provide default current timestamp for creation times
                    row[f] = datetime.now()
                    logger.info(f"Required timestamp {f} is None, using current timestamp")
                else:
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
                if is_required:
                    row[f] = datetime.now()
                else:
                    row[f] = None
            else:
                row[f] = parsed

        # 5. String Fields (Everything else)
        # We need to handle required/nullable handling for strings explicitly or generically
        # Getting all keys from row to handle
        for key, val in row.items():
            if key not in INTEGER_FIELDS + BOOLEAN_FIELDS + JSON_FIELDS + TIMESTAMP_FIELDS:
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
