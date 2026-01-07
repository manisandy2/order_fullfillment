from routers.Iceberg_schema import *
# from app.iceberg.base_schema import build_schema

def masterorders_table():
    return {
        "namespace": "order_fulfillment",
        "table": "masterorders_test",
        "schema": MasterSchema("masterorders"),
        "partition": ["created_at"]
    }

def driver():
    return {
        "namespace": "order_fulfillment",
        "table": "drivers",
        "schema": Drivers_schema("drivers"),
        "partition": ["created_at"]
    }