from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType,UUIDType

IngestionTrackingSchema = [
    NestedField(1, "id", UUIDType(), required=True),  # 🔥 Unique tracking id
    NestedField(2, "table_name", StringType(), required=True),
    NestedField(3, "last_processed", TimestampType(), required=True),
    NestedField(4, "created_at", TimestampType(), required=True),
    NestedField(5, "updated_at", TimestampType(), required=True)
]