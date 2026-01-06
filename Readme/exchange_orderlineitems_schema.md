# Exchange Order Line Items Schema Conversion

## Overview
This document describes the schema conversion for the [exchange_orderlineitems](file:///Users/mac-1/Desktop/order_fulfillment/routers/exchangeOrderLineItemsUtility.py#91-153) table from MySQL to PyIceberg and PyArrow formats.

## MySQL Schema Highlights

- **JSON Fields**: 13 fields storing varied structured data (e.g., `product_policy`, `delivery_details`).
- **Integer Fields**: `quantity` and `special_price`.
- **Primary Keys**: `line_item_id` is the primary key.

## Type Mappings

| MySQL Field | MySQL Type | PyIceberg Type | PyArrow Type | Notes |
|-------------|------------|----------------|--------------|-------|
| `quantity` | `int(11)` | `LongType()` | `pa.int64()` | Nullable |
| `special_price` | `int(11)` | `LongType()` | `pa.int64()` | Nullable |
| `image` | `text` | `StringType()` | `pa.string()` | Mapped to String |
| `serial_no` | `text` | `StringType()` | `pa.string()` | Mapped to String |

## Special Handling

### 1. JSON Fields
Iceberg and Arrow do not have native JSON types. These fields are serialized to JSON strings:
```python
# In cleaning function
if isinstance(val, (dict, list)):
    row[f] = json.dumps(val)
```

### 2. Nullable Integers
Integers like `quantity` are nullable in the source.
- If `None`, they remain `None`.
- Invalid strings are defaulted to `0` with a warning log.

## Usage Examples

### 1. Generate Schema

```python
from routers.exchangeOrderLineItemsUtility import exchange_orderlineitems_schema

sample_record = {
    "line_item_id": "LITEM-001",
    "order_line_item_id": "OLITEM-001",
    "master_order_id": "MORD-001",
    "master_sale_order_id": "MSO-001",
    "quantity": 1,
    "product_name": "Widget"
}

iceberg_schema, arrow_schema = exchange_orderlineitems_schema(sample_record)
```

### 2. Clean Data

```python
from routers.exchangeOrderLineItemsUtility import exchange_orderlineitems_clean_rows

raw_rows = [
    {
        "line_item_id": "LITEM-001",
        "quantity": "5",  # String to int
        "product_policy": {"policy": "standard"} # Dict to JSON string
    }
]

cleaned_rows = exchange_orderlineitems_clean_rows(raw_rows)
```

## File Location
- **Utility File**: [exchangeOrderLineItemsUtility.py](file:///Users/mac-1/Desktop/order_fulfillment/routers/exchangeOrderLineItemsUtility.py)
