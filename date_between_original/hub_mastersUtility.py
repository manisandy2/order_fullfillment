import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
TIMESTAMP_FIELDS = ["created_at", "updated_at"]
BOOLEAN_FIELDS = ["isactive"]
DATE_FIELDS = []
REQUIRED_FIELDS = ["id", "store_name", "state", "district", "store_mobile_no", "store_mailid", 
                   "store_shortcode", "pincode", "customer_code", "store_address", 
                   "area_code", "login_user", "created_at", "updated_at", "isactive", 
                   "store_contact_person"]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Keys / Required
    "id": (StringType(), pa.string(), True),
    "store_name": (StringType(), pa.string(), True),
    "state": (StringType(), pa.string(), True),
    "district": (StringType(), pa.string(), True),
    "store_mobile_no": (StringType(), pa.string(), True),
    "store_mailid": (StringType(), pa.string(), True),
    "store_shortcode": (StringType(), pa.string(), True),
    "pincode": (StringType(), pa.string(), True), # text in MySQL
    "customer_code": (StringType(), pa.string(), True), # text
    "store_address": (StringType(), pa.string(), True), # text
    "store_address_line1": (StringType(), pa.string(), True), # text, NOT NULL
    "store_address_line2": (StringType(), pa.string(), True), # text, NOT NULL
    "store_address_line3": (StringType(), pa.string(), True), # text, NOT NULL
    "area_code": (StringType(), pa.string(), True), # text
    "login_user": (StringType(), pa.string(), True),
    "store_contact_person": (StringType(), pa.string(), True),

    # Boolean/Tinyint
    "isactive": (IntegerType(), pa.int32(), True),

    # Nullable Strings/Varchar
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # Timestamp fields
    "created_at": (TimestampType(), pa.timestamp('ms'), True), # NOT NULL
    "updated_at": (TimestampType(), pa.timestamp('ms'), True), # NOT NULL
}
