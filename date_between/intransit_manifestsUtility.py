import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
TIMESTAMP_FIELDS = ['createdAt', 'updatedAt', 'created_at', 'updated_at']

BOOLEAN_FIELDS = []
INTEGER_FIELDS = ['items_total']

VARCHAR_FIELDS = ['t_manifest_id', 't_shipment_id', 'time_sorted_id', 'manifest_type', 'created_by', 'updated_by']

REQUIRED_FIELDS = ['t_manifest_id', 't_shipment_id', 'time_sorted_id', 'items_total', 'createdAt', 'created_at']

DATE_FIELDS = []

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Primary / required identifiers
    "t_manifest_id": (StringType(), pa.string(), True),     # PRI
    "t_shipment_id": (StringType(), pa.string(), True),     # NOT NULL
    "time_sorted_id": (StringType(), pa.string(), True),    # NOT NULL

    # Nullable string fields
    "manifest_type": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # Integer fields
    "items_total": (IntegerType(), pa.int32(), True),

    # Timestamp fields (keep both naming styles)
    "createdAt": (TimestampType(), pa.timestamp("ms"), True),
    "updatedAt": (TimestampType(), pa.timestamp("ms"), False),
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), False),
}
