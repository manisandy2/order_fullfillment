# External Call Logs Schema Conversion

## Overview
This document describes the schema conversion for the `externalcalllogs` table from MySQL to PyIceberg and PyArrow formats.

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `id` | `varchar(100)` | `StringType()` | `pa.string()` | Unique Key (Required) |
| `res_obj` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `created_at` | `timestamp` | `TimestampType()` | `pa.timestamp('ms')` | |

## Special Handling

### 1. JSON Fields
The `res_obj` field is stored as a JSON string.
```python
# In cleaning function
if isinstance(val, (dict, list)):
    row[f] = json.dumps(val)
```

### 2. Required Fields
- `id` is the only strictly required field.
- All others are nullable strings or timestamps.

## Usage Examples

### 1. Generate Schema

```python
from routers.externalCallLogsUtility import external_call_logs_schema

sample_record = {
    "id": "LOG-001",
    "ext_log_id": "EXT-999",
    "res_obj": {"status": "success", "code": 200},
    "created_at": "2024-01-01 10:00:00"
}

iceberg_schema, arrow_schema = external_call_logs_schema(sample_record)
```

### 2. Clean Data

```python
from routers.externalCallLogsUtility import external_call_logs_clean_rows

raw_rows = [
    {
        "id": "LOG-001",
        "res_obj": {"status": "success"} # Dict to JSON string
    }
]

cleaned_rows = external_call_logs_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [externalCallLogsUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/externalCallLogsUtility.py)
