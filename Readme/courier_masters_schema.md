# Courier Masters Schema Conversion

## Overview
This document describes the schema conversion for the [courier_masters](file:///Users/mac-1/Desktop/order_fulfillment/routers/courierMastersUtility.py#98-160) table from MySQL to PyIceberg and PyArrow formats.

## MySQL Schema Highlights

- **Varchar Primary Keys**: `id` is `varchar(100)`, not integer.
- **JSON Fields**: `branch_code_list`, `branchcode`, `roles` are stored as JSON in MySQL.
- **Boolean Fields**: `isActive` is `tinyint(1)`.
- **Timestamps**: Multiple creation/update timestamps (`created_on`, `created_at`, `createdAt`, etc.).

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `id` | `varchar(100)` | `StringType()` | `pa.string()` | Primary Key |
| `pincode` | `int(11)` | `IntegerType()` | `pa.int32()` | |
| `isActive` | `tinyint(1)` | `BooleanType()` | `pa.bool_()` | Converted to boolean |
| `branch_code_list` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `roles` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `dob` | `timestamp` | `TimestampType()` | `pa.timestamp('ms')` | Required field |

## Special Handling

### 1. JSON Fields
Iceberg and Arrow do not have native JSON types. These fields are serialized to JSON strings:
- `branch_code_list`
- `branchcode`
- `roles`

```python
# In cleaning function
if isinstance(val, (dict, list)):
    row[f] = json.dumps(val)
```

### 2. Boolean Handling
MySQL `tinyint(1)` values (0/1) are converted to Python booleans:
- `1`, `"1"`, `"true"` -> `True`
- `0`, `"0"`, `"false"`, `None` -> `False`

### 3. Required vs Nullable
The schema enforces strict nullability based on the MySQL definition.
- **Required Strings**: properties like `firstname`, `lastname`, `city`, `state`, etc. default to `""` if missing.
- **Required Timestamps**: `dob`, `created_at` etc. default to `datetime.now()` if missing to satisfy NOT NULL.

## Usage Examples

### 1. Generate Schema

```python
from routers.courierMastersUtility import courier_masters_schema

sample_record = {
    "id": "EMP001",
    "firstname": "John",
    "lastname": "Doe",
    "pincode": 400001,
    "isActive": 1,
    "roles": ["driver", "loader"]
    # ... other fields
}

iceberg_schema, arrow_schema = courier_masters_schema(sample_record)
```

### 2. Clean Data

```python
from routers.courierMastersUtility import courier_masters_clean_rows

raw_rows = [
    {
        "id": "EMP001",
        "pincode": "400001",  # String to Int
        "isActive": 1,        # Int to Bool
        "roles": ["driver"],  # List to JSON string
        "created_at": None    # Defaults to now()
    }
]

cleaned_rows = courier_masters_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [courierMastersUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/courierMastersUtility.py)
