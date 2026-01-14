from routers.Iceberg_schema import *



SCHEMA_REGISTRY = {
    "roles": roles,
    "schedulers": schedulers,
    "scheduler_retention_log": scheduler_retention_log,
}

TABLE_CONFIG = {
    "roles": {
        "sort": "id ASC",
        "partition_field": "created_at",
    },
    "schedulers": {
        "sort": "scheduler_id ASC",
        "partition_field": "created_at",
    },
    "scheduler_retention_log": {
        "sort": "id ASC",
        "partition_field": "created_at",
    },
}