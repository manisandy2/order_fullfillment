TABLE_CONFIG = {
    "pickup_deliveries": {
        "namespace": "order_fulfillment",
        "date_column": "row_added_dttm",
        "mysql_method": "get_pickup_deliveries_date_between",
        "chunk_size": 1000,
    },
    "masterorders": {
        "namespace": "order_fulfillment",
        "date_column": "created_at",
        "mysql_method": "get_master_order_date_between",
        "chunk_size": 1000,
    },
    # Add remaining 40 tables here
}