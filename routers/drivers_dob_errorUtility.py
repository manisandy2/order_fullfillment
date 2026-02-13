import logging
import json
from typing import Dict, List, Tuple, Any
import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField
)
from datetime import datetime, date
from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

TIMESTAMP_FIELDS = [
    "dob",
    "driving_license_expiry",
    "created_on",
    "updated_on",
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
]

JSON_FIELDS = [
    "branch_code_list",
    "branchcode",
]

BOOLEAN_FIELDS = [
    "isActive",
]

INTEGER_FIELDS = [
    "pincode",
]


REQUIRED_FIELDS = [
    "id",
    "time_sorted_id",
    "type_of_work",
    "employee_from",
    "work_type",
    "firstname",
    "lastname",
    "dob",
    "primary_contact",
    "address_line1",
    "city",
    "state",
    "pincode",
    "country",
    "gender",
    # "driving_license_num",
    "driving_license_url",
    "aadhar_card_num",
    "aadhar_card_url",
    "created_on",
    "created_at",
    "createdAt",
]


# Field type overrides based on MySQL schema
FIELD_OVERRIDES = {
    # ---------------- PRIMARY / CORE ----------------
    "id": (StringType(), pa.string(), True),
    "time_sorted_id": (StringType(), pa.string(), True),
    "type_of_work": (StringType(), pa.string(), True),
    "employee_from": (StringType(), pa.string(), True),
    "work_type": (StringType(), pa.string(), True),
    "firstname": (StringType(), pa.string(), True),
    "lastname": (StringType(), pa.string(), True),

    # ---------------- TIMESTAMP ----------------
    "dob": (TimestampType(), pa.timestamp("us"), True),

    # ---------------- CONTACT ----------------
    "primary_contact": (StringType(), pa.string(), True),
    "secondary_contact": (StringType(), pa.string(), False),

    # ---------------- ADDRESS ----------------
    "address_line1": (StringType(), pa.string(), True),
    "address_line2": (StringType(), pa.string(), False),
    "city": (StringType(), pa.string(), True),
    "state": (StringType(), pa.string(), True),
    "pincode": (IntegerType(), pa.int32(), True),
    "area": (StringType(), pa.string(), False),
    "country": (StringType(), pa.string(), True),

    # ---------------- PERSONAL ----------------
    "gender": (StringType(), pa.string(), True),
    "profile_pic_url": (StringType(), pa.string(), False),

    # ---------------- STATUS / FLAGS ----------------
    "isActive": (IntegerType(), pa.int8(), False),
    "remarks": (StringType(), pa.string(), False),
    "ratings": (StringType(), pa.string(), False),
    "approval_status": (StringType(), pa.string(), False),

    # ---------------- DOCUMENTS ----------------
    "driving_license_num": (StringType(), pa.string(), False),
    "driving_license_url": (StringType(), pa.string(), True),
    "driving_license_expiry": (TimestampType(), pa.timestamp("us"), False),

    "aadhar_card_num": (StringType(), pa.string(), True),
    "aadhar_card_url": (StringType(), pa.string(), True),

    "voter_id_num": (StringType(), pa.string(), False),
    "voter_id_url": (StringType(), pa.string(), False),

    "ration_card_num": (StringType(), pa.string(), False),
    "ration_card_url": (StringType(), pa.string(), False),

    "pancard_num": (StringType(), pa.string(), False),
    "pancard_url": (StringType(), pa.string(), False),

    # ---------------- AUDIT ----------------
    "created_on": (TimestampType(), pa.timestamp("us"), True),
    "updated_on": (TimestampType(), pa.timestamp("us"), False),
    "created_at": (TimestampType(), pa.timestamp("us"), True),
    "updated_at": (TimestampType(), pa.timestamp("us"), False),
    "createdAt": (TimestampType(), pa.timestamp("us"), True),
    "updatedAt": (TimestampType(), pa.timestamp("us"), False),

    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),

    # ---------------- JSON ----------------
    "branch_code_list": (StringType(), pa.string(), False),
    "branchcode": (StringType(), pa.string(), False),

    # ---------------- EMP ----------------
    "emp_id": (StringType(), pa.string(), False),
}



