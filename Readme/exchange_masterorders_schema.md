# Exchange Master Orders Schema Conversion

## Overview
This document describes the schema conversion for the `exchange_masterorders_w` table from MySQL to PyIceberg and PyArrow formats.

## MySQL Schema Highlights

- **Complex Types**: Extensive usage of `enum`, `decimal`, and `JSON` types.
- **Enums**: Fields like `channel`, `channel_medium`, `delivery_from`.
- **Decimals**: `latitude` and `longitude` are `decimal(10,6)`.
- **JSON**: 13 fields storing varied structured data.

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `channel` | `enum(...)` | `StringType()` | `pa.string()` | Enums treated as strings |
| `latitude` | `decimal(10,6)` | `DoubleType()` | `pa.float64()` | Mapped to double for utility |
| `order_tag` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `invoice_date`| `timestamp` | `TimestampType()` | `pa.timestamp('ms')` | |

## Special Handling

### 1. Decimal Fields
`latitude` and `longitude` are converted to `float` (DoubleType) for easier processing in analytics, rather than strict Decimal support which can be verbose in Arrow/Iceberg if precision isn't critical for money-like calculation.
```python
# In cleaning function
try:
    row[f] = float(val)
except ValueError:
    row[f] = 0.0
```

### 2. Enum Fields
MySQL Enums are simply treated as Strings. No validation against the enum values is currently performed during cleaning; values are passed through.

### 3. JSON Fields
Serialized to string format:
```python
if isinstance(val, (dict, list)):
    row[f] = json.dumps(val)
```

## Usage Examples

### 1. Generate Schema

```python
from routers.exchangeMasterOrdersUtility import exchange_masterorders_schema

sample_record = {
    "order_id": "EX-ORD-001",
    "sale_order_id": "SO-001",
    "channel": "ONLINE",
    "latitude": 19.0760,
    "longitude": 72.8777,
    "lineitems": [{"sku": "A", "qty": 1}]
}

iceberg_schema, arrow_schema = exchange_masterorders_schema(sample_record)
```

### 2. Clean Data

```python
from routers.exchangeMasterOrdersUtility import exchange_masterorders_clean_rows

raw_rows = [
    {
        "order_id": "EX-ORD-001",
        "latitude": "19.0760",  # String to float
        "lineitems": [{"sku": "A"}] # List to JSON string
    }
]

cleaned_rows = exchange_masterorders_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [exchangeMasterOrdersUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/exchangeMasterOrdersUtility.py)
