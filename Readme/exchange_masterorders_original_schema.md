# Exchange Master Orders (Original) Schema Conversion

## Overview
This document describes the schema conversion for the [exchange_masterorders](file:///Users/mac-1/Desktop/order_fulfillment/routers/exchangeMasterOrdersUtility.py#80-142) table. Note that this is distinct from `exchange_masterorders_w`.

## Key Differences from `_w` Version

- **Default Values**: `latitude` and `longitude` defaults to `0.000000` instead of `NULL`.
- **Field Defaults**: `order_inv_status` defaults to `'NEW'`.
- **Timestamps**: `created_at` and `updated_at` have explicit `CURRENT_TIMESTAMP` defaults.

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `latitude` | `decimal(10,6)` | `DoubleType()` | `pa.float64()` | Defaults to 0.0 |
| `order_tag` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |
| `service_details` | `JSON` | `StringType()` | `pa.string()` | Serialized to JSON string |

## Usage Examples

### 1. Generate Schema

```python
from routers.exchangeMasterOrdersOriginalUtility import exchange_masterorders_original_schema

sample_record = {
    "order_id": "ORD-ORIG-001",
    "sale_order_id": "SO-001",
    "latitude": 0.0,
    "created_at": "2024-01-01 10:00:00"
}

iceberg_schema, arrow_schema = exchange_masterorders_original_schema(sample_record)
```

### 2. Clean Data

```python
from routers.exchangeMasterOrdersOriginalUtility import exchange_masterorders_original_clean_rows

raw_rows = [
    {
        "order_id": "ORD-ORIG-001",
        "latitude": None,  # Defaults to 0.0
        "order_tag": {"tag": "VIP"} # Dict to JSON string
    }
]

cleaned_rows = exchange_masterorders_original_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [exchangeMasterOrdersOriginalUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/exchangeMasterOrdersOriginalUtility.py)
