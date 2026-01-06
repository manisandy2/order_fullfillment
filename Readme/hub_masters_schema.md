# Hub Masters Schema Conversion

## Overview
This document describes the schema conversion for the [hub_masters](file:///Users/mac-1/Desktop/order_fulfillment/routers/hubMastersUtility.py#56-118) table from MySQL to PyIceberg and PyArrow formats.

## MySQL Schema Highlights

- **Strict Requirements**: Most fields are `NOT NULL` in the source schema.
- **Boolean fields**: `isactive` is `tinyint(1)` corresponding to boolean.
- **Text fields**: Extensive use of `text` type for addresses and codes, mapped to Strings.

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `isactive` | `tinyint(1)` | `BooleanType()` | `pa.bool_()` | Mapped to Boolean |
| `pincode` | `text` | `StringType()` | `pa.string()` | Mapped to String |
| `store_address` | `text` | `StringType()` | `pa.string()` | Mapped to String |

## Special Handling

### 1. Boolean Fields
`isactive` is converted from `0/1` integers or strings to boolean `False/True`.
Default is `False` if missing.

### 2. Required Fields
Almost all fields are required (NOT NULL) in this schema.
- Missing string fields default to `""` (empty string) with a warning log.
- Missing timestamps default to `datetime.now()` with an info log (simulating `CURRENT_TIMESTAMP`).

## Usage Examples

### 1. Generate Schema

```python
from routers.hubMastersUtility import hub_masters_schema

sample_record = {
    "id": "HUB-001",
    "store_name": "Main Hub",
    "isactive": 1,
    "created_at": "2024-01-01 10:00:00"
}

iceberg_schema, arrow_schema = hub_masters_schema(sample_record)
```

### 2. Clean Data

```python
from routers.hubMastersUtility import hub_masters_clean_rows

raw_rows = [
    {
        "id": "HUB-001",
        "isactive": "1",  # String to boolean
        "store_address": "123 Main St"
    }
]

cleaned_rows = hub_masters_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [hubMastersUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/hubMastersUtility.py)
