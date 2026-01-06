import logging
from typing import Dict, List, Tuple, Any
import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, 
    TimestampType, StringType, NestedField
)
from datetime import datetime, date
from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

# Module-level constants
BIGINT_FIELDS = ["id"]
VARCHAR_FIELDS = [
    "cpincode", "cpindesc", "city", "bdsc", "state", 
    "created_by", "updated_by", "cscrcd"
]
TEXT_FIELDS = ["carea", "cecomzn"]
TIMESTAMP_FIELDS = ["created_at", "updated_at"]
REQUIRED_FIELDS = ["id"]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Primary key - bigint(20) unsigned, auto_increment, NOT NULL
    "id": (LongType(), pa.int64(), True),
    
    # VARCHAR fields - NOT NULL
    "cpincode": (StringType(), pa.string(), True),
    "cpindesc": (StringType(), pa.string(), True),
    "city": (StringType(), pa.string(), True),
    "bdsc": (StringType(), pa.string(), True),
    "state": (StringType(), pa.string(), True),
    
    # TEXT fields - NOT NULL
    "carea": (StringType(), pa.string(), True),
    "cecomzn": (StringType(), pa.string(), True),
    "region": (StringType(), pa.string(), True),
    
    # Timestamp fields - NOT NULL with defaults
    "created_at": (TimestampType(), pa.timestamp('ms'), True),
    "updated_at": (TimestampType(), pa.timestamp('ms'), True),
    
    # VARCHAR fields - NULLABLE
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
    "cscrcd": (StringType(), pa.string(), False),
}


def bluedart_zone_masters_schema(record: Dict[str, Any]) -> Tuple[Schema, pa.Schema]:
    """
    Generate Iceberg and Arrow schemas for bluedart_zone_masters table.
    
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


def bluedart_zone_masters_clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for bluedart_zone_masters schema compliance.
    
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
        # 1. Bigint Fields (ID)
        for f in BIGINT_FIELDS:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    logger.warning(f"Invalid integer value for {f}: {val}, defaulting to 0")
                    row[f] = 0
            elif val is None:
                # ID is required, should not be None
                logger.error(f"Required field {f} is None")
                row[f] = 0

        # 2. VARCHAR Fields (NOT NULL)
        for f in VARCHAR_FIELDS:
            val = row.get(f)
            # Nullable fields
            if f in ["created_by", "updated_by", "cscrcd"]:
                if val is None:
                    row[f] = None
                else:
                    row[f] = str(val)
            # NOT NULL fields
            else:
                if val is None:
                    logger.warning(f"NOT NULL field {f} is None, defaulting to empty string")
                    row[f] = ""
                else:
                    row[f] = str(val)

        # 3. TEXT Fields (NOT NULL)
        for f in TEXT_FIELDS:
            val = row.get(f)
            if val is None:
                logger.warning(f"NOT NULL field {f} is None, defaulting to empty string")
                row[f] = ""
            else:
                row[f] = str(val)
        
        # Handle region separately (NOT NULL varchar)
        val = row.get("region")
        if val is None:
            logger.warning("NOT NULL field region is None, defaulting to empty string")
            row["region"] = ""
        else:
            row["region"] = str(val)

        # 4. Timestamp Fields (NOT NULL with defaults)
        for f in TIMESTAMP_FIELDS:
            val = row.get(f)

            # If None or empty, use current timestamp (simulating CURRENT_TIMESTAMP default)
            if val is None or val == "":
                row[f] = datetime.now()
                logger.info(f"Field {f} is None, using current timestamp")
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

    return rows
