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
BOOLEAN_FIELDS = []
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

