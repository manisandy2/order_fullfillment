"""
Complete schema definitions for bluedart_zone_masters table.
Includes both PyIceberg and PyArrow schemas.
"""

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    TimestampType,
)

# ============================================================================
# PYARROW SCHEMA
# ============================================================================

bluedart_zone_masters_arrow_schema = pa.schema([
    pa.field("id", pa.uint64()),
    pa.field("cpincode", pa.string()),
    pa.field("cpindesc", pa.string()),
    pa.field("city", pa.string()),
    pa.field("bdsc", pa.string()),
    pa.field("state", pa.string()),
    pa.field("carea", pa.string()),
    pa.field("cecomzn", pa.string()),
    pa.field("region", pa.string()),
    pa.field("created_at", pa.timestamp('us')),
    pa.field("updated_at", pa.timestamp('us')),
    pa.field("created_by", pa.string(), nullable=True),
    pa.field("updated_by", pa.string(), nullable=True),
    pa.field("cscrcd", pa.string(), nullable=True),
])

# ============================================================================
# PYICEBERG SCHEMA
# ============================================================================

bluedart_zone_masters_iceberg_schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "cpincode", StringType(), required=True),
    NestedField(3, "cpindesc", StringType(), required=True),
    NestedField(4, "city", StringType(), required=True),
    NestedField(5, "bdsc", StringType(), required=True),
    NestedField(6, "state", StringType(), required=True),
    NestedField(7, "carea", StringType(), required=True),
    NestedField(8, "cecomzn", StringType(), required=True),
    NestedField(9, "region", StringType(), required=True),
    NestedField(10, "created_at", TimestampType(), required=True),
    NestedField(11, "updated_at", TimestampType(), required=True),
    NestedField(12, "created_by", StringType(), required=False),
    NestedField(13, "updated_by", StringType(), required=False),
    NestedField(14, "cscrcd", StringType(), required=False),
)

# ============================================================================
# COLUMN DEFINITIONS (for reference)
# ============================================================================

COLUMN_DEFINITIONS = {
    "id": {
        "mysql_type": "bigint(20) unsigned",
        "arrow_type": "uint64",
        "iceberg_type": "long",
        "nullable": False,
        "description": "Primary key, auto-increment ID"
    },
    "cpincode": {
        "mysql_type": "varchar(100)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "Customer pincode"
    },
    "cpindesc": {
        "mysql_type": "varchar(100)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "Pincode description"
    },
    "city": {
        "mysql_type": "varchar(100)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "City name"
    },
    "bdsc": {
        "mysql_type": "varchar(100)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "Bluedart service code"
    },
    "state": {
        "mysql_type": "varchar(100)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "State name"
    },
    "carea": {
        "mysql_type": "text",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "Customer area"
    },
    "cecomzn": {
        "mysql_type": "text",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "E-commerce zone"
    },
    "region": {
        "mysql_type": "varchar(100)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": False,
        "description": "Region name"
    },
    "created_at": {
        "mysql_type": "timestamp",
        "arrow_type": "timestamp[us]",
        "iceberg_type": "timestamp",
        "nullable": False,
        "description": "Record creation timestamp"
    },
    "updated_at": {
        "mysql_type": "timestamp",
        "arrow_type": "timestamp[us]",
        "iceberg_type": "timestamp",
        "nullable": False,
        "description": "Record update timestamp"
    },
    "created_by": {
        "mysql_type": "varchar(200)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": True,
        "description": "User who created the record"
    },
    "updated_by": {
        "mysql_type": "varchar(200)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": True,
        "description": "User who last updated the record"
    },
    "cscrcd": {
        "mysql_type": "varchar(255)",
        "arrow_type": "string",
        "iceberg_type": "string",
        "nullable": True,
        "description": "Customer service code"
    },
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_arrow_schema():
    """Get the PyArrow schema."""
    return bluedart_zone_masters_arrow_schema


def get_iceberg_schema():
    """Get the PyIceberg schema."""
    return bluedart_zone_masters_iceberg_schema


def print_schema_info():
    """Print schema information."""
    print("=" * 80)
    print("BLUEDART ZONE MASTERS - SCHEMA INFORMATION")
    print("=" * 80)
    print()
    
    print("PyArrow Schema:")
    print("-" * 80)
    print(bluedart_zone_masters_arrow_schema)
    print()
    
    print("PyIceberg Schema:")
    print("-" * 80)
    print(bluedart_zone_masters_iceberg_schema)
    print()
    
    print("Column Definitions:")
    print("-" * 80)
    for col_name, col_info in COLUMN_DEFINITIONS.items():
        print(f"{col_name:15s} | MySQL: {col_info['mysql_type']:20s} | "
              f"Arrow: {col_info['arrow_type']:15s} | "
              f"Iceberg: {col_info['iceberg_type']:10s} | "
              f"Nullable: {col_info['nullable']}")
    print()


if __name__ == "__main__":
    print_schema_info()
