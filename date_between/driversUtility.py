import pyarrow as pa
from pyiceberg.types import *

# Module-level constants
INTEGER_FIELDS = ["pincode","isActive"]
# BOOLEAN_FIELDS = ["isActive"]
BOOLEAN_FIELDS = []
JSON_FIELDS = ["branch_code_list", "branchcode", "roles"]
TIMESTAMP_FIELDS = [
    "dob", "driving_license_expiry", 
    "created_on", "updated_on", 
    "created_at", "updated_at", 
    "createdAt", "updatedAt"
]
REQUIRED_FIELDS = [
    "id", "time_sorted_id", "type_of_work", "employee_from", "work_type",
    "firstname", "lastname", "dob", "primary_contact", "address_line1",
    "city", "state", "pincode", "country", "gender", "driving_license_num",
    "driving_license_url", "aadhar_card_num", "aadhar_card_url",
    "created_on", "created_at", "createdAt"
]

# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # Primary & Keys
    "id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),
    
    # Required Varchars
    "type_of_work": (StringType(), pa.string(), True),
    "employee_from": (StringType(), pa.string(), True),
    "work_type": (StringType(), pa.string(), True),
    "firstname": (StringType(), pa.string(), True),
    "lastname": (StringType(), pa.string(), True),
    "primary_contact": (StringType(), pa.string(), True),
    "address_line1": (StringType(), pa.string(), True),
    "city": (StringType(), pa.string(), True),
    "state": (StringType(), pa.string(), True),
    "country": (StringType(), pa.string(), True),
    "gender": (StringType(), pa.string(), True),
    "driving_license_num": (StringType(), pa.string(), True),
    "aadhar_card_num": (StringType(), pa.string(), True),
    
    # Nullable Varchars
    "secondary_contact": (StringType(), pa.string(), False),
    "address_line2": (StringType(), pa.string(), False),
    "area": (StringType(), pa.string(), False),
    "remarks": (StringType(), pa.string(), False),
    "ratings": (StringType(), pa.string(), False),
    "approval_status": (StringType(), pa.string(), False),
    "voter_id_num": (StringType(), pa.string(), False),
    "ration_card_num": (StringType(), pa.string(), False),
    "pancard_num": (StringType(), pa.string(), False),
    "emp_id": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # Text fields (mapped to String)
    "profile_pic_url": (StringType(), pa.string(), False), # nullable=true
    "driving_license_url": (StringType(), pa.string(), True), # nullable=false
    "aadhar_card_url": (StringType(), pa.string(), True), # nullable=false
    "voter_id_url": (StringType(), pa.string(), False),
    "ration_card_url": (StringType(), pa.string(), False),
    "pancard_url": (StringType(), pa.string(), False),

    # Integer fields
    "pincode": (IntegerType(), pa.int32(), True),
    
    # Boolean fields (tinyint(1))
    "isActive": (IntegerType(), pa.int32(), False), # default 0, nullable=true in schema

    # JSON fields (mapped to String)
    "branch_code_list": (StringType(), pa.string(), False),
    "branchcode": (StringType(), pa.string(), False),
    "roles": (StringType(), pa.string(), False),

    # Timestamp fields
    "dob": (TimestampType(), pa.timestamp('ms'), True),
    "driving_license_expiry": (TimestampType(), pa.timestamp('ms'), False),
    "created_on": (TimestampType(), pa.timestamp('ms'), True),
    "updated_on": (TimestampType(), pa.timestamp('ms'), False),
    "created_at": (TimestampType(), pa.timestamp('ms'), True),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
    "createdAt": (TimestampType(), pa.timestamp('ms'), True),
    "updatedAt": (TimestampType(), pa.timestamp('ms'), False),
}






