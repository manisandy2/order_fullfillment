import sys
import os
import json
from datetime import datetime
import pyarrow as pa

# Add proper path to sys.path
sys.path.append("/Users/mac-1/Desktop/order_fulfillment")

from routers.exchangeInformationsUtility import (
    exchange_informations_schema,
    exchange_informations_clean_rows
)

from routers.exchange_orderlineitemsUtility import (
    exchange_orderlineitems_schema,
    exchange_orderlineitems_clean_rows
)

def verify_exchange_info():
    print("--- Verifying Exchange Information ---")
    sample_record = {
        "order_id": "ORD123",
        "created_at": datetime.now(),
        "customer_info": '{"name": "John"}',
        "mobile_no": "1234567890"
    }
    
    ice_schema, arrow_schema = exchange_informations_schema(sample_record)
    
    arrow_names = arrow_schema.names
    assert "order_id" in arrow_names, "order_id missing"
    assert "created_at" in arrow_names, "created_at missing"
    assert "customer_info" in arrow_names, "customer_info missing"
    
    idx_order = arrow_names.index("order_id")
    assert pa.types.is_string(arrow_schema.types[idx_order]), "order_id should be string"
    
    print("Schema Verification Passed!")

    rows = [
        {
            "order_id": "ORD001",
            "created_at": "2023-10-27 10:00:00",
            "customer_info": {"name": "Test User"},
            "mobile_no": 9876543210
        }
    ]
    
    cleaned = exchange_informations_clean_rows(rows)
    r1 = cleaned[0]
    assert isinstance(r1["customer_info"], str), "customer_info should be stringified JSON"
    assert isinstance(r1["created_at"], datetime), "created_at should be datetime object"
    assert isinstance(r1["mobile_no"], str), "mobile_no should be converted to string"
    print("Clean Rows Verification Passed!")


def verify_exchange_items():
    print("\n--- Verifying Exchange OrderLineItems ---")
    sample_record = {
        "line_item_id": "LI123",
        "order_line_item_id": "OLI123",
        "master_order_id": "MO123",
        "master_sale_order_id": "MSO123",
        "quantity": 2,
        "product_policy": {"policy": "standard"},
        "created_at": datetime.now()
    }

    ice_schema, arrow_schema = exchange_orderlineitems_schema(sample_record)
    arrow_names = arrow_schema.names

    assert "line_item_id" in arrow_names
    assert "quantity" in arrow_names
    
    idx_qty = arrow_names.index("quantity")
    assert pa.types.is_int64(arrow_schema.types[idx_qty]), "quantity should be int64"

    print("Schema Verification Passed!")

    rows = [
        {
            "line_item_id": "LI001",
            "quantity": "5", # String to int
            "special_price": 100.50, # Float to int
            "product_policy": {"return": "7 days"},
            "created_at": "2023-11-01 12:00:00"
        }
    ]

    cleaned = exchange_orderlineitems_clean_rows(rows)
    r1 = cleaned[0]
    
    assert r1["quantity"] == 5, "Quantity should be 5"
    assert r1["special_price"] == 100, "Special price should be 100"
    assert isinstance(r1["product_policy"], str), "product_policy should be stringified"
    assert isinstance(r1["created_at"], datetime), "created_at should be datetime"

    print("Clean Rows Verification Passed!")

if __name__ == "__main__":
    verify_exchange_info()
    verify_exchange_items()
