import pyarrow as pa
from pyiceberg.types import *
from datetime import datetime, date
from pyiceberg.schema import Schema

def masterorder_schema(record: dict):
    iceberg_fields = []
    arrow_fields = []

    # Custom field overrides (by name)
    field_overrides = {
    # Required keys
    "order_id": (StringType(), pa.string(), True),
    "sale_order_id": (StringType(), pa.string(), True),

    # Integer fields
    "oms_data_migration_status": (IntegerType(), pa.int32(), False),
    "cust_id_update": (IntegerType(), pa.int32(), False),

    # Float fields
    "latitude": (FloatType(), pa.float64(), False),
    "longitude": (FloatType(), pa.float64(), False),

    # Date fields
    "invoice_date": (DateType(), pa.timestamp("ms"), False),
    "updated_at_new": (DateType(), pa.timestamp("ms"), False),

    # Timestamp fields
    "invoice_date": (TimestampType(), pa.timestamp('ms'), False),
    "created_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at": (TimestampType(), pa.timestamp('ms'), False),
    "updated_at_new": (TimestampType(), pa.timestamp('ms'), False),

    # Other explicit string fields
    "invoice_no": (StringType(), pa.string(), False),
    "invoice_reff_no": (StringType(), pa.string(), False),
    "invoice_reff_date": (StringType(), pa.string(), False),
    "channel": (StringType(), pa.string(), False),
    "channel_medium": (StringType(), pa.string(), False),
    "order_status": (StringType(), pa.string(), False),
    "order_tag": (StringType(), pa.string(), False),
    "order_inv_status": (StringType(), pa.string(), False),
    "order_type": (StringType(), pa.string(), False),
    "delivery_from": (StringType(), pa.string(), False),
    "delivery_from_branchcode": (StringType(), pa.string(), False),
    "billing_branch_code": (StringType(), pa.string(), False),
    "cust_id": (StringType(), pa.string(), False),
    "cust_primary_email": (StringType(), pa.string(), False),
    "cust_primary_contact": (StringType(), pa.string(), False),
    "cust_mobile": (StringType(), pa.string(), False),
    "customer_address": (StringType(), pa.string(), False),
    "shipment_address": (StringType(), pa.string(), False),
    "billing_address": (StringType(), pa.string(), False),
    "payment_details": (StringType(), pa.string(), False),
    "refund_details": (StringType(), pa.string(), False),
    "voucher_details": (StringType(), pa.string(), False),
    "employee_sale_details": (StringType(), pa.string(), False),
    "order_summary_details": (StringType(), pa.string(), False),
    "other_details": (StringType(), pa.string(), False),
    "service_details": (StringType(), pa.string(), False),
    "invoice_pdf": (StringType(), pa.string(), False),
    "lineitems": (StringType(), pa.string(), False),
    "lineitem_status": (StringType(), pa.string(), False),
    "created_by": (StringType(), pa.string(), False),
    "updated_by": (StringType(), pa.string(), False),
    "multi_invoice": (StringType(), pa.string(), False),
    }

    for idx, (name, value) in enumerate(record.items(), start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False
            # Boolean
            if isinstance(value, bool):
                ice_type,arrow_type = BooleanType(),pa.bool_()

            # Integer
            elif isinstance(value, int):
                ice_type,arrow_type = LongType(),pa.int64()

            # Float
            elif isinstance(value, float):
                ice_type,arrow_type = DoubleType(),pa.float64()

            # Date only
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # Timestamp
            elif isinstance(value, datetime):
                ice_type,arrow_type = DateType(),pa.timestamp("ms")

            # String
            else:
                ice_type,arrow_type = StringType(),pa.string()
                 

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema


def masterOrder_clean_rows(rows):

    float_fields = ["latitude", "longitude"]
    integer_fields = ["cust_id_update","oms_data_migration_status"]
    # date_fields = ["invoice_date","updated_at_new"]
    timestamp_fields = ["created_at", "updated_at","invoice_date","updated_at_new"]
    string_fields = ["order_id", "sale_order_id", "invoice_no", "invoice_reff_no", "invoice_reff_date",
                     "channel", "channel_medium", "order_status", "order_tag", "order_inv_status", "order_type",
                     "delivery_from", "delivery_from_branchcode", "billing_branch_code", "cust_id", "cust_primary_email",
                     "cust_primary_contact", "cust_mobile", "customer_address", "shipment_address", "billing_address",
                     "payment_details", "refund_details", "voucher_details", "employee_sale_details", "order_summary_details",
                     "other_details", "service_details", "invoice_pdf", "lineitems", "lineitem_status", "created_by", "updated_by",
                     "multi_invoice"]            

    for row in rows:

        # 1 -------- Float Fields ----------------------------------------
        for f in float_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = float(val)
                except ValueError:
                    row[f] = 0.0
            elif val is None:
                row[f] = 0.0
                

        # 2 -------- Integer Fields --------------------------------------
        for f in integer_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    row[f] = 0
            elif val is None:
                row[f] = 0

        # 3 -------- String Fields ---------------------------------------
        for f in string_fields:
            val = row.get(f)
            if val is None:
                row[f] = ""
            else:
                row[f] = str(val)

        # 4 -------- Timestamp Fields ------------------------------------
        for f in timestamp_fields:
            val = row.get(f)

            if val is None or val == "":
                row[f] = None
                continue

            if isinstance(val, datetime):
                continue

            # try multiple formats
            parsed = None
            dt_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d",
            ]

            for fmt in dt_formats:
                try:
                    parsed = datetime.strptime(val, fmt)
                    break
                except:
                    pass

            row[f] = parsed if parsed else None
        for key, val in row.items():
            if key not in float_fields + integer_fields + timestamp_fields:
                row[key] = "" if val is None else str(val)

    return rows



