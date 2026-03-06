import pyarrow as pa
from pyiceberg.types import *

BOOLEAN_FIELDS = []
FLOAT_FIELDS = ["latitude","longitude"]
INTEGER_FIELDS = ["oms_data_migration_status"]
DATE_FIELDS = []
# DOUBLE_FIELDS = ["latitude","longitude"]
TIMESTAMP_FIELDS = [
        "invoice_date",
        "exp_delivery_date",
        "row_added_dttm",
        "row_updated_dttm",
        # "updated_at_new",
    ]
STRING_FIELDS = [
        "status_event_id", "type", "channel", "system", "medium", "flow",
        "order_id", "inventory_status", "shipment_status", "internal_status",
        "customer_status", "from_location_code", "to_location_code",
        "pickup_delivery_req_item_id", "intransit_pickup_delivery_req_item_id",
        "line_item_id", "manifest_id", "shipment_id", "item_code",
        "invoice_no", "picklist_id", "wh_st_empid", "wh_st_empname",
        "driver_code", "driver_name", "driver_emp_id", "driver_contact",
        "ass_code", "ass_name", "ass_emp_id", "ass_contact",
        "sec_ass_code", "sec_ass_name", "sec_emp_id", "sec_contact",
        "vehicle_no", "vehicle_name", "docking_area", "type_of_order",
        "cust_name", "cust_mobile_num", "imei", "tracking_id", "comments",
        "courier_name", "action", "module_name", "notification_status",
        "order_inv_status", "additional_info", "row_added_by",
        "row_added_empid", "shipment_tracking_id",
    ]


FIELD_OVERRIDES = {

        # PRIMARY KEYS
        "status_event_id": (StringType(), pa.string(), True),
        "pickup_delivery_req_item_id": (StringType(), pa.string(), True),

        # DECIMAL
        "latitude": (FloatType(), pa.float64(), False),
        "longitude": (FloatType(), pa.float64(), False),

        # TIMESTAMP FIELDS
        "invoice_date": (TimestampType(), pa.timestamp("ms"), False),
        "exp_delivery_date": (TimestampType(), pa.timestamp("ms"), False),
        "row_added_dttm": (TimestampType(), pa.timestamp("ms"), False),
        "row_updated_dttm": (TimestampType(), pa.timestamp("ms"), False),
        # "updated_at_new": (TimestampType(), pa.timestamp("ms"), False),

        # INTEGER
        "oms_data_migration_status": (IntegerType(), pa.int32(), False),

        # JSON → STRING
        "additional_info": (StringType(), pa.string(), False),

        # Everything else is VARCHAR/TEXT → StringType
        "type": (StringType(), pa.string(), False),
        "channel": (StringType(), pa.string(), False),
        "system": (StringType(), pa.string(), False),
        "medium": (StringType(), pa.string(), False),
        "flow": (StringType(), pa.string(), False),
        "order_id": (StringType(), pa.string(), False),
        "inventory_status": (StringType(), pa.string(), False),
        "shipment_status": (StringType(), pa.string(), False),
        "internal_status": (StringType(), pa.string(), False),
        "customer_status": (StringType(), pa.string(), False),
        "from_location_code": (StringType(), pa.string(), False),
        "to_location_code": (StringType(), pa.string(), False),
        "intransit_pickup_delivery_req_item_id": (StringType(), pa.string(), False),
        "line_item_id": (StringType(), pa.string(), False),
        "manifest_id": (StringType(), pa.string(), False),
        "shipment_id": (StringType(), pa.string(), False),
        "item_code": (StringType(), pa.string(), False),
        "invoice_no": (StringType(), pa.string(), False),
        "picklist_id": (StringType(), pa.string(), False),
        "wh_st_empid": (StringType(), pa.string(), False),
        "wh_st_empname": (StringType(), pa.string(), False),
        "driver_code": (StringType(), pa.string(), False),
        "driver_name": (StringType(), pa.string(), False),
        "driver_emp_id": (StringType(), pa.string(), False),
        "driver_contact": (StringType(), pa.string(), False),
        "ass_code": (StringType(), pa.string(), False),
        "ass_name": (StringType(), pa.string(), False),
        "ass_emp_id": (StringType(), pa.string(), False),
        "ass_contact": (StringType(), pa.string(), False),
        "sec_ass_code": (StringType(), pa.string(), False),
        "sec_ass_name": (StringType(), pa.string(), False),
        "sec_emp_id": (StringType(), pa.string(), False),
        "sec_contact": (StringType(), pa.string(), False),
        "vehicle_no": (StringType(), pa.string(), False),
        "vehicle_name": (StringType(), pa.string(), False),
        "docking_area": (StringType(), pa.string(), False),
        "type_of_order": (StringType(), pa.string(), False),
        "cust_name": (StringType(), pa.string(), False),
        "cust_mobile_num": (StringType(), pa.string(), False),
        "imei": (StringType(), pa.string(), False),
        "tracking_id": (StringType(), pa.string(), False),
        "comments": (StringType(), pa.string(), False),
        "courier_name": (StringType(), pa.string(), False),
        "action": (StringType(), pa.string(), False),
        "module_name": (StringType(), pa.string(), False),
        "notification_status": (StringType(), pa.string(), False),
        "order_inv_status": (StringType(), pa.string(), False),
        "row_added_by": (StringType(), pa.string(), False),
        "row_added_empid": (StringType(), pa.string(), False),
        "shipment_tracking_id": (StringType(), pa.string(), False),
}