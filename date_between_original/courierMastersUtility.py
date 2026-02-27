import pyarrow as pa
from pyiceberg.types import  *

# Module-level constants
INTEGER_FIELDS = []
BOOLEAN_FIELDS = ["isactive", "isEditable", "isApiIntegrated"]
JSON_FIELDS = ["branch_list", "branch_code_list"]
TIMESTAMP_FIELDS = ["created_at", "updated_at"]
REQUIRED_FIELDS = [
    "code", "created_at", "isEditable", "isApiIntegrated"
]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys & Varchars
    "code": (StringType(), pa.string(), True),
    "name": (StringType(), pa.string(), False),
    "type": (StringType(), pa.string(), False),
    "image": (StringType(), pa.string(), False),
    "description": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # Boolean fields (tinyint(1))
    "isactive": (BooleanType(), pa.bool_(), False),
    "isEditable": (BooleanType(), pa.bool_(), True),
    "isApiIntegrated": (BooleanType(), pa.bool_(), True),

    # JSON fields
    "branch_list": (StringType(), pa.string(), False),
    "branch_code_list": (StringType(), pa.string(), False),

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), True),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
}
