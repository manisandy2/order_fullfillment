# Bluedart Zone Masters Schema Conversion

## Overview
This document describes the schema conversion for the [bluedart_zone_masters](file:///Users/mac-1/Desktop/order_fulfillment/routers/bluedartZoneMastersUtility.py#51-113) table from MySQL to PyIceberg and PyArrow formats.

---

## MySQL Schema

```json
[
  {"name":"id","type":"bigint(20) unsigned","nullable":false,"key":"PRI","default":null,"extra":"auto_increment"},
  {"name":"cpincode","type":"varchar(100)","nullable":false,"key":"","default":null,"extra":""},
  {"name":"cpindesc","type":"varchar(100)","nullable":false,"key":"","default":null,"extra":""},
  {"name":"city","type":"varchar(100)","nullable":false,"key":"","default":null,"extra":""},
  {"name":"bdsc","type":"varchar(100)","nullable":false,"key":"","default":null,"extra":""},
  {"name":"state","type":"varchar(100)","nullable":false,"key":"","default":null,"extra":""},
  {"name":"carea","type":"text","nullable":false,"key":"","default":null,"extra":""},
  {"name":"cecomzn","type":"text","nullable":false,"key":"","default":null,"extra":""},
  {"name":"region","type":"varchar(100)","nullable":false,"key":"","default":null,"extra":""},
  {"name":"created_at","type":"timestamp","nullable":false,"key":"","default":"CURRENT_TIMESTAMP","extra":""},
  {"name":"updated_at","type":"timestamp","nullable":false,"key":"","default":"CURRENT_TIMESTAMP","extra":"on update CURRENT_TIMESTAMP"},
  {"name":"created_by","type":"varchar(200)","nullable":true,"key":"","default":null,"extra":""},
  {"name":"updated_by","type":"varchar(200)","nullable":true,"key":"","default":null,"extra":""},
  {"name":"cscrcd","type":"varchar(255)","nullable":true,"key":"","default":null,"extra":""}
]
```

---

## Type Mappings

| MySQL Type | PyIceberg Type | PyArrow Type | Nullable | Notes |
|------------|----------------|--------------|----------|-------|
| `bigint(20) unsigned` | `LongType()` | `pa.int64()` | ❌ | Primary key, auto_increment |
| `varchar(100)` | `StringType()` | `pa.string()` | ❌ (most) | NOT NULL fields |
| `varchar(200)` | `StringType()` | `pa.string()` | ✅ | `created_by`, `updated_by` |
| `varchar(255)` | `StringType()` | `pa.string()` | ✅ | `cscrcd` |
| `text` | `StringType()` | `pa.string()` | ❌ | `carea`, `cecomzn` |
| `timestamp` | `TimestampType()` | `pa.timestamp('ms')` | ❌ | `created_at`, `updated_at` |

---

## Field Categories

### Required Fields (NOT NULL)
- **Primary Key**: `id`
- **VARCHAR**: `cpincode`, `cpindesc`, `city`, `bdsc`, `state`, `region`
- **TEXT**: `carea`, `cecomzn`
- **TIMESTAMP**: `created_at`, `updated_at`

### Nullable Fields
- `created_by` (varchar 200)
- `updated_by` (varchar 200)
- `cscrcd` (varchar 255)

---

## Key Differences from Master Order Schema

### 1. **Stricter NULL Handling**
Most fields are NOT NULL in this schema, unlike master order which has mostly nullable fields.

```python
# Master Order (mostly nullable)
"invoice_no": (StringType(), pa.string(), False)  # nullable=True

# Bluedart Zone Masters (mostly required)
"cpincode": (StringType(), pa.string(), True)  # nullable=False
```

### 2. **Timestamp Defaults**
Both timestamp fields have `CURRENT_TIMESTAMP` defaults:

```python
# In cleaning function
if val is None or val == "":
    row[f] = datetime.now()  # Simulate CURRENT_TIMESTAMP
```

### 3. **No Float/Double Fields**
This schema only has integers and strings, no geographic coordinates.

### 4. **Smaller Field Count**
14 fields vs 40+ in master order schema.

---

## Usage Examples

### 1. Generate Schema

```python
from routers.bluedartZoneMastersUtility import bluedart_zone_masters_schema

# Sample record
sample_record = {
    "id": 1,
    "cpincode": "400001",
    "cpindesc": "Mumbai Central",
    "city": "Mumbai",
    "bdsc": "MUM",
    "state": "Maharashtra",
    "carea": "Central Mumbai",
    "cecomzn": "West Zone",
    "region": "Western",
    "created_at": "2024-01-01 10:00:00",
    "updated_at": "2024-01-01 10:00:00",
    "created_by": "admin",
    "updated_by": None,
    "cscrcd": "MH001"
}

# Generate schemas
iceberg_schema, arrow_schema = bluedart_zone_masters_schema(sample_record)

print(iceberg_schema)
print(arrow_schema)
```

### 2. Clean Data Rows

```python
from routers.bluedartZoneMastersUtility import bluedart_zone_masters_clean_rows

# Raw data from MySQL
raw_rows = [
    {
        "id": "1",  # String needs conversion
        "cpincode": "400001",
        "cpindesc": "Mumbai Central",
        "city": "Mumbai",
        "bdsc": "MUM",
        "state": "Maharashtra",
        "carea": "Central Mumbai",
        "cecomzn": "West Zone",
        "region": "Western",
        "created_at": "2024-01-01 10:00:00",
        "updated_at": None,  # Will use current timestamp
        "created_by": "admin",
        "updated_by": None,  # Nullable, stays None
        "cscrcd": None  # Nullable, stays None
    }
]

# Clean rows
cleaned_rows = bluedart_zone_masters_clean_rows(raw_rows)
```

### 3. Integration with FastAPI Endpoint

```python
from fastapi import APIRouter, HTTPException
from routers.bluedartZoneMastersUtility import (
    bluedart_zone_masters_schema,
    bluedart_zone_masters_clean_rows
)

router = APIRouter()

@router.post("/bluedart-zone-masters/insert-multi")
async def insert_bluedart_zone_masters(data: dict):
    try:
        # Fetch data from MySQL
        rows = fetch_from_mysql(data)
        
        # Clean rows
        cleaned_rows = bluedart_zone_masters_clean_rows(rows)
        
        # Generate schema from first row
        if cleaned_rows:
            iceberg_schema, arrow_schema = bluedart_zone_masters_schema(cleaned_rows[0])
            
            # Convert to Arrow table
            arrow_table = pa.Table.from_pylist(cleaned_rows, schema=arrow_schema)
            
            # Append to Iceberg table
            # ... append logic here
            
        return {"status": "success", "rows_inserted": len(cleaned_rows)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

---

## Data Cleaning Rules

### 1. **ID Field (bigint)**
- Convert string to int
- Default to 0 if invalid (with error log)
- Should never be None (required field)

### 2. **VARCHAR Fields (NOT NULL)**
- Convert to string
- Default to empty string if None (with warning log)
- Examples: `cpincode`, `cpindesc`, `city`, `bdsc`, `state`, `region`

### 3. **VARCHAR Fields (NULLABLE)**
- Convert to string if not None
- Keep as None if None
- Examples: `created_by`, `updated_by`, `cscrcd`

### 4. **TEXT Fields (NOT NULL)**
- Convert to string
- Default to empty string if None (with warning log)
- Examples: `carea`, `cecomzn`

### 5. **Timestamp Fields (NOT NULL)**
- Parse from multiple formats:
  - `%Y-%m-%d %H:%M:%S`
  - `%Y-%m-%dT%H:%M:%S`
  - `%d/%m/%Y %H:%M:%S`
  - `%Y-%m-%d`
- Default to `datetime.now()` if None or parse fails
- Simulates MySQL `CURRENT_TIMESTAMP` default

---

## Validation

### Required Field Check
```python
REQUIRED_FIELDS = ["id"]

missing = [f for f in REQUIRED_FIELDS if f not in record]
if missing:
    raise ValueError(f"Missing required fields: {missing}")
```

### Logging Strategy
```python
import logging
logger = logging.getLogger(__name__)

# Error: Critical issues (required field is None)
logger.error(f"Required field {f} is None")

# Warning: Data quality issues (parse failures, NULL in NOT NULL field)
logger.warning(f"NOT NULL field {f} is None, defaulting to empty string")

# Info: Normal operations (using default timestamp)
logger.info(f"Field {f} is None, using current timestamp")
```

---

## Best Practices Applied

✅ **No duplicate dictionary keys** (unlike original masterOrderUtility)  
✅ **Consistent type mappings** (Iceberg and Arrow types match)  
✅ **Specific exception handling** (no bare `except:`)  
✅ **Comprehensive logging** (errors, warnings, info)  
✅ **Type hints** (all functions annotated)  
✅ **Module-level constants** (field lists defined once)  
✅ **Deterministic field IDs** (sorted iteration)  
✅ **Proper NULL handling** (respects MySQL nullable constraints)  

---

## File Location

**Utility File**: [bluedartZoneMastersUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/bluedartZoneMastersUtility.py)

**Functions**:
1. [bluedart_zone_masters_schema(record)](file:///Users/mac-1/Desktop/order_fulfillment/routers/bluedartZoneMastersUtility.py#51-113) - Generate Iceberg and Arrow schemas
2. [bluedart_zone_masters_clean_rows(rows)](file:///Users/mac-1/Desktop/order_fulfillment/routers/bluedartZoneMastersUtility.py#115-210) - Clean and normalize data

---

## Summary

This schema conversion properly handles:
- ✅ MySQL type mappings to Iceberg/Arrow
- ✅ NOT NULL constraints (most fields required)
- ✅ Nullable fields (created_by, updated_by, cscrcd)
- ✅ Timestamp defaults (CURRENT_TIMESTAMP simulation)
- ✅ Primary key handling (auto_increment bigint)
- ✅ Data validation and cleaning
- ✅ Comprehensive error logging
- ✅ Type safety with hints
