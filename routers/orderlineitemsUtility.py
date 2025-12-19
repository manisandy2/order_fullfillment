
import pyarrow as pa
from pyiceberg.types import *
from datetime import datetime, date
from pyiceberg.schema import Schema

def orderlineitems_schema(record: dict):
    iceberg_fields = []
    arrow_fields = []

    # ----------------------------------------------------------
    # CUSTOM FIELD OVERRIDES (Strict Types From MySQL Schema)
    # ----------------------------------------------------------
    field_overrides = {

        # PRIMARY KEY
        "line_item_id": (StringType(), pa.string(), True),

        # INTEGER FIELDS
        "quantity": (IntegerType(), pa.int32(), False),
        "special_price": (IntegerType(), pa.int32(), False),
        "oms_data_migration_status": (IntegerType(), pa.int32(), False),

        # TIMESTAMP FIELDS
        "created_at": (TimestampType(), pa.timestamp("ms"), False),
        "updated_at": (TimestampType(), pa.timestamp("ms"), False),

        # JSON FIELDS (store as STRING for Iceberg safety)
        "product_policy": (StringType(), pa.string(), False),
        "billed_details": (StringType(), pa.string(), False),
        "delivery_details": (StringType(), pa.string(), False),
        "invoice_details": (StringType(), pa.string(), False),
        "tax_details": (StringType(), pa.string(), False),
        "insurance_details": (StringType(), pa.string(), False),
        "preorder_response": (StringType(), pa.string(), False),
        "seller_details": (StringType(), pa.string(), False),
        "shipment_details": (StringType(), pa.string(), False),
        "return_details": (StringType(), pa.string(), False),
        "return_refund_details": (StringType(), pa.string(), False),
        "return_replace_details": (StringType(), pa.string(), False),
        "return_exchange_details": (StringType(), pa.string(), False),

        # STRING FIELDS (varchar/text)
        "order_line_item_id": (StringType(), pa.string(), False),
        "master_order_id": (StringType(), pa.string(), False),
        "master_sale_order_id": (StringType(), pa.string(), False),
        "delivery_from": (StringType(), pa.string(), False),
        "customer_status": (StringType(), pa.string(), False),
        "inventory_status": (StringType(), pa.string(), False),
        "internal_status": (StringType(), pa.string(), False),
        "shipping_status": (StringType(), pa.string(), False),
        "category_code": (StringType(), pa.string(), False),
        "category_name": (StringType(), pa.string(), False),
        "item_qty_label": (StringType(), pa.string(), False),
        "exg_invo_no": (StringType(), pa.string(), False),
        "exg_invo_date": (StringType(), pa.string(), False),
        "home_pickup": (StringType(), pa.string(), False),
        "order_inv_status": (StringType(), pa.string(), False),
        "slug": (StringType(), pa.string(), False),
        "product_name": (StringType(), pa.string(), False),
        "model": (StringType(), pa.string(), False),
        "erp_item_code": (StringType(), pa.string(), False),
        "type_of_order": (StringType(), pa.string(), False),
        "product_hsn": (StringType(), pa.string(), False),
        "image": (StringType(), pa.string(), False),
        "options": (StringType(), pa.string(), False),
        "delivery_charges": (StringType(), pa.string(), False),
        "price": (StringType(), pa.string(), False),
        "brand_code": (StringType(), pa.string(), False),
        "brand_name": (StringType(), pa.string(), False),
        "created_by": (StringType(), pa.string(), False),
        "updated_by": (StringType(), pa.string(), False),
        "serial_no": (StringType(), pa.string(), False),
        "offer_type": (StringType(), pa.string(), False),
        "delivery_type": (StringType(), pa.string(), False),
    }

    # ----------------------------------------------------------
    # AUTO TYPE DETECTION FOR NON-OVERRIDE FIELDS
    # ----------------------------------------------------------
    for idx, (name, value) in enumerate(record.items(), start=1):

        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False

            # Boolean
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()

            # Integer
            elif isinstance(value, int):
                ice_type, arrow_type = IntegerType(), pa.int32()

            # Float
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()

            # Date only (YYYY-MM-DD)
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # Timestamp
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp("ms")

            # Default: STRING
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    return iceberg_schema, arrow_schema


def orderlineitems_clean_rows(rows):

    # FLOAT fields (none in this schema)
    float_fields = []

    # INTEGER fields
    integer_fields = ["quantity", "special_price", "oms_data_migration_status"]

    # TIMESTAMP fields
    timestamp_fields = ["created_at", "updated_at"]

    # STRING + TEXT + JSON fields
    string_fields = [
        "line_item_id", "order_line_item_id", "master_order_id", "master_sale_order_id",
        "delivery_from", "customer_status", "inventory_status", "internal_status", "shipping_status",
        "category_code", "category_name", "item_qty_label", "exg_invo_no", "exg_invo_date",
        "home_pickup", "order_inv_status", "slug", "product_name", "model", "erp_item_code",
        "product_hsn", "image", "options", "delivery_charges", "price", "brand_code", "brand_name",
        "created_by", "updated_by", "serial_no", "offer_type", "delivery_type",

        # JSON fields stored as string
        "product_policy", "billed_details", "delivery_details", "invoice_details",
        "tax_details", "insurance_details", "preorder_response", "seller_details",
        "shipment_details", "return_details", "return_refund_details", "return_replace_details",
        "return_exchange_details"
    ]

    for row in rows:

        # -------- Integer Fields ----------------------------------------
        for f in integer_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    row[f] = 0
            elif val is None:
                row[f] = 0

        # -------- String Fields -----------------------------------------
        for f in string_fields:
            val = row.get(f)
            if val is None:
                row[f] = ""
            else:
                row[f] = str(val)

        # -------- Timestamp Fields --------------------------------------
        for f in timestamp_fields:
            val = row.get(f)

            if val is None or val == "":
                row[f] = None
                continue

            if isinstance(val, datetime):
                continue

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

    return rows

def orderlineitems_test_clean_rows(rows):

    # FLOAT fields (none in this schema)
    float_fields = []

    # INTEGER fields
    integer_fields = []

    # TIMESTAMP fields
    timestamp_fields = ["created_at"]

    # STRING + TEXT + JSON fields
    string_fields = [
        "line_item_id", "order_line_item_id", "master_order_id", "master_sale_order_id",
        "delivery_from","customer_status","inventory_status","erp_item_code"
    ]

    for row in rows:

        # -------- Integer Fields ----------------------------------------
        for f in integer_fields:
            val = row.get(f)
            if isinstance(val, str):
                try:
                    row[f] = int(val)
                except ValueError:
                    row[f] = 0
            elif val is None:
                row[f] = 0

        # -------- String Fields -----------------------------------------
        for f in string_fields:
            val = row.get(f)
            if val is None:
                row[f] = ""
            else:
                row[f] = str(val)

        # -------- Timestamp Fields --------------------------------------
        for f in timestamp_fields:
            val = row.get(f)

            if val is None or val == "":
                row[f] = None
                continue

            if isinstance(val, datetime):
                continue

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

    return rows



def orderlineitems_test_schema(record: dict):
    iceberg_fields = []
    arrow_fields = []

    # ----------------------------------------------------------
    # CUSTOM FIELD OVERRIDES (Strict Types From MySQL Schema)
    # ----------------------------------------------------------
    field_overrides = {

        # PRIMARY KEY
        "line_item_id": (StringType(), pa.string(), True),
        "order_line_item_id": (StringType(), pa.string(), False),
        "master_order_id": (StringType(), pa.string(), False),
        "master_sale_order_id": (StringType(), pa.string(), False),
        "delivery_from": (StringType(), pa.string(), False),
        "created_by": (StringType(), pa.string(), False),
        "customer_status":(StringType(), pa.string(), False),
        "inventory_status":(StringType(), pa.string(), False),
        "erp_item_code":(StringType(), pa.string(), False),

    }

    # ----------------------------------------------------------
    # AUTO TYPE DETECTION FOR NON-OVERRIDE FIELDS
    # ----------------------------------------------------------
    for idx, (name, value) in enumerate(record.items(), start=1):

        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False

            # Boolean
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()

            # Integer
            elif isinstance(value, int):
                ice_type, arrow_type = IntegerType(), pa.int32()

            # Float
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()

            # Date only (YYYY-MM-DD)
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # Timestamp
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp("ms")

            # Default: STRING
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    return iceberg_schema, arrow_schema