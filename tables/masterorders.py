from routers.Iceberg_schema import MasterSchema
# from app.iceberg.base_schema import build_schema

def masterorders_table():
    return {
        "namespace": "order_fulfillment",
        "table": "masterorders_test",
        "schema": MasterSchema("masterorders"),
        "partition": ["created_at"]
    }