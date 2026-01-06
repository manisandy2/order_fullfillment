# Exchange Information Schema Conversion

## Overview
This document describes the schema conversion for the `exchange_informations` table from MySQL to PyIceberg and PyArrow formats.

## MySQL Schema Highlights

- **Required Fields**: Only `order_id` is strictly required and unique.
- **JSON Fields**: Extensive use of JSON for nested data:
  - `customer_info`
  - `product_info`
  - `exchange_quote_details`
  - `device_evaluation`
  - `device_images`
- **Nullable Strings**: Most fields are nullable strings or text.
- **Timestamps**: Single `created_at` timestamp.

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `order_id` | `varchar(50)` | `StringType()` | `pa.string()` | Unique Key (Required) |
| `customer_info` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `product_info` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `created_at` | `timestamp` | `TimestampType()` | `pa.timestamp('ms')` | Nullable |

## Special Handling

### 1. JSON Fields
Iceberg and Arrow do not have native JSON types. These fields are serialized to JSON strings:
```python
# In cleaning function
if isinstance(val, (dict, list)):
    row[f] = json.dumps(val)
```

### 2. Value Fields as Strings
Several fields that look numeric (`actual_product_value`, `confirm_extra_value`, etc.) are defined as `varchar(20)` in the source schema. They are mapped to `StringType()` to preserve the source fidelity, unless specific numeric conversion is requested.

### 3. Null Handling
Most fields are nullable.
- If a value is `None` in the source, it remains `None` in the output.
- `order_id` is required; if missing, the row will fail schema validation if not caught earlier, or be defaulted to empty string if cleaning logic forces it (currently validation raises explicit error).

## Usage Examples

### 1. Generate Schema

```python
from routers.exchangeInfoUtility import exchange_info_schema

sample_record = {
    "order_id": "ORD123456",
    "customer_info": {"name": "Alice", "phone": "1234567890"},
    "product_info": {"model": "iPhone 13", "storage": "128GB"},
    "status": "Quote Generated"
}

iceberg_schema, arrow_schema = exchange_info_schema(sample_record)
```

### 2. Clean Data

```python
from routers.exchangeInfoUtility import exchange_info_clean_rows

raw_rows = [
    {
        "order_id": "ORD123456",
        "customer_info": {"name": "Alice"},  # Dict to JSON string
        "created_at": "2024-01-01 10:00:00"
    }
]

cleaned_rows = exchange_info_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [exchangeInfoUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/exchangeInfoUtility.py)
