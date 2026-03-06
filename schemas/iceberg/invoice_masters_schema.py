import pyarrow as pa
from pyiceberg.types import *


REQUIRED_FIELDS = ['id', 'invoice_code', 'invoice_branch', 'invoice_no', 'invoice_year', 'created_at', 'updated_at']
TIMESTAMP_FIELDS = ['created_at', 'updated_at']
BOOLEAN_FIELDS = []
INTEGER_FIELDS = ['invoice_no']
VARCHAR_FIELDS = ['id', 'invoice_code', 'invoice_branch', 'invoice_year', 'order_type']
DATE_FIELDS = []


FIELD_OVERRIDES = {

    # 🔑 Primary Key
    "id": (StringType(), pa.string(), True),

    # 📄 Invoice Details
    "invoice_code": (StringType(), pa.string(), True),
    "invoice_branch": (StringType(), pa.string(), True),
    "invoice_no": (IntegerType(), pa.int32(), True),
    "invoice_year": (StringType(), pa.string(), True),
    "order_type": (StringType(), pa.string(), False),

    # 🕒 Audit
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
    "updated_at": (TimestampType(), pa.timestamp("ms"), True),
}
