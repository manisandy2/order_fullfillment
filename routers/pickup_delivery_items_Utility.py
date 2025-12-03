import pyarrow as pa
from pyiceberg.types import *
from datetime import datetime, date
from pyiceberg.schema import Schema
from decimal import Decimal


def pickup_delivery_items_clean_rows(rows):

    # ---------------- FLOAT / DECIMAL FIELDS ----------------
    float_fields = [
        "latitude",
        "longitude"
    ]

    # ---------------- INTEGER FIELDS ----------------
    integer_fields = [
        "item_qty",
        "dimension_length",
        "dimension_height",
        "dimension_width",
        "reattempt_count",
        "delivery_charge",
        "payment_verification",
        "otp_verified_status",
        "ispickup_active",
        "oms_data_migration_status",
        "dropship_flag",
    ]

    # ---------------- TIMESTAMP / DATE FIELDS ----------------
    timestamp_fields = [
        "invoice_date",
        "expect_delivery_pickup_dt",
        "picker_added_dt",
        "manifest_added_dt",
        "exchange_invo_date",
        "row_added_dt",
        "row_updated_dt",
        "goods_received_date",
        "sale_invoice_date",
        "updated_at_new",
    ]

    # ---------------- STRING FIELDS ----------------
    string_fields = [
        # ---- varchar fields ----
        "pickup_delivery_req_item_id", "pickup_delivery_req_id",
        "order_id", "sale_order_id", "invoice_no", "invoice_reff_no",
        "customer_status", "internal_status", "inventory_status",
        "shipment_status", "inventory_location_code", "docking_area",
        "docking_area_code", "type_of_order", "item_code", "item_weight",
        "category_code", "category_name", "inventory_location",
        "inventory_location_branchname", "inventory_location_mobileno",
        "order_type", "order_line_item_id", "picklist_id", "picker_id",
        "manifest_id", "shipment_id", "driver_code", "driver_name",
        "driver_contact", "assistant_code", "assistant_name",
        "assistant_contact", "vehicle_no", "vehicle_type", "vehicle_name",
        "vehicle_image", "order_inv_status", "tracking_id", "item_price",
        "item_qty_label", "item_dimension", "line_itemid",
        "invoice_status", "exchange_invo_no", "brand", "capture_do_url",
        "shipment_tracking_id", "secondary_assistant_code",
        "secondary_assistant_name", "secondary_assistant_pic",
        "secondary_assistant_contact", "pickup_label_id",
        "collection_location_type", "collection_location_code",
        "collection_location_branchname", "to_location_email",
        "to_location_mobileno", "to_location_lat_long",
        "shipment_type", "home_pickup", "row_added_by", "row_updated_by",
        "to_location_fullname", "billing_branch_code", "selling_price",
        "vendor_id", "sale_invoice_no", "dismantle_label", "quote_id",
        "billed_at_branch_name", "offer_type", "delivery_type",
        "erp_po_createdAt", "erp_po_no", "seller_order_id",
        "seller_item_code", "seller_id", "seller_order_createdAt",
        "seller_apob_code", "unit_price", "discount_price", "plant",
        "seller_branch_code",

        # ---- text fields ----
        "invoice_reff_date", "item_name", "item_image", "driver_image",
        "assistant_pic", "tracking_url", "permit", "courier_details",
        "inventory_location_address", "exchange_invoice_url",
        "collection_location_address", "to_location_address",
        "item_serial_no", "vendor_name", "invoice_url", "courier_code",

        # ---- enum ----
        "is_item_installable",
    ]

    # ------------------------------------------------------------
    #               CLEANING LOGIC (unchanged)
    # ------------------------------------------------------------

    from datetime import datetime

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
        

        # for f in decimal_fields:
        #     val = row.get(f)
        #     if val is None or val == "":
        #         row[f] = None
        #     else:
        #         try:
        #             row[f] = Decimal(str(val))
        #         except:
        #             row[f] = None

        # 4 -------- Timestamp Fields ------------------------------------
        for f in timestamp_fields:
            val = row.get(f)

            if val is None or val == "":
                row[f] = None
                continue

            if isinstance(val, datetime):
                continue

            parsed = None
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d",
            ]

            for fm in formats:
                try:
                    parsed = datetime.strptime(val, fm)
                    break
                except:
                    pass

            row[f] = parsed if parsed else None

    return rows

def pickup_delivery_items_schema(record: dict):
    iceberg_fields = []
    arrow_fields = []

    # --------------------------------------------------------------------
    # ALL FIELD TYPE OVERRIDES (from MySQL schema)
    # --------------------------------------------------------------------
    field_overrides = {

        # ---------------- PRIMARY KEYS ----------------
        "pickup_delivery_req_item_id": (StringType(), pa.string(), True),

        # ---------------- STRING (varchar, text, enum) ----------------
        "pickup_delivery_req_id": (StringType(), pa.string(), False),
        "order_id": (StringType(), pa.string(), True),
        "sale_order_id": (StringType(), pa.string(), True),
        "invoice_no": (StringType(), pa.string(), False),
        "invoice_reff_no": (StringType(), pa.string(), False),
        "invoice_reff_date": (StringType(), pa.string(), False),
        "customer_status": (StringType(), pa.string(), False),
        "internal_status": (StringType(), pa.string(), False),
        "inventory_status": (StringType(), pa.string(), False),
        "shipment_status": (StringType(), pa.string(), False),
        "inward_exchange_status": (StringType(), pa.string(), False),
        "inventory_location_code": (StringType(), pa.string(), False),
        "docking_area": (StringType(), pa.string(), False),
        "docking_area_code": (StringType(), pa.string(), False),
        "type_of_order": (StringType(), pa.string(), False),
        "item_code": (StringType(), pa.string(), False),
        "item_name": (StringType(), pa.string(), False),
        "item_image": (StringType(), pa.string(), False),
        "item_weight": (StringType(), pa.string(), False),
        "category_code": (StringType(), pa.string(), False),
        "category_name": (StringType(), pa.string(), False),
        "inventory_location": (StringType(), pa.string(), False),
        "inventory_location_branchname": (StringType(), pa.string(), False),
        "inventory_location_mobileno": (StringType(), pa.string(), False),
        "order_type": (StringType(), pa.string(), False),
        "order_line_item_id": (StringType(), pa.string(), False),
        "picklist_id": (StringType(), pa.string(), False),
        "picker_id": (StringType(), pa.string(), False),
        "manifest_id": (StringType(), pa.string(), False),
        "shipment_id": (StringType(), pa.string(), False),
        "driver_code": (StringType(), pa.string(), False),
        "driver_name": (StringType(), pa.string(), False),
        "driver_image": (StringType(), pa.string(), False),
        "driver_contact": (StringType(), pa.string(), False),
        "assistant_code": (StringType(), pa.string(), False),
        "assistant_name": (StringType(), pa.string(), False),
        "assistant_pic": (StringType(), pa.string(), False),
        "assistant_contact": (StringType(), pa.string(), False),
        "vehicle_no": (StringType(), pa.string(), False),
        "vehicle_type": (StringType(), pa.string(), False),
        "vehicle_name": (StringType(), pa.string(), False),
        "vehicle_image": (StringType(), pa.string(), False),
        "order_inv_status": (StringType(), pa.string(), False),
        "tracking_id": (StringType(), pa.string(), False),
        "tracking_url": (StringType(), pa.string(), False),
        "permit": (StringType(), pa.string(), False),
        "courier_name": (StringType(), pa.string(), False),
        "courier_details": (StringType(), pa.string(), False),
        "inventory_location_address": (StringType(), pa.string(), False),
        "item_price": (StringType(), pa.string(), False),
        "item_qty_label": (StringType(), pa.string(), False),
        "item_dimension": (StringType(), pa.string(), False),
        "line_itemid": (StringType(), pa.string(), False),
        "invoice_status": (StringType(), pa.string(), False),
        "exchange_invo_no": (StringType(), pa.string(), False),
        "exchange_invoice_url": (StringType(), pa.string(), False),
        "exchange_collected_amount": (StringType(), pa.string(), False),
        "brand": (StringType(), pa.string(), False),
        "capture_do_url": (StringType(), pa.string(), False),
        "shipment_tracking_id": (StringType(), pa.string(), False),
        "secondary_assistant_code": (StringType(), pa.string(), False),
        "secondary_assistant_name": (StringType(), pa.string(), False),
        "secondary_assistant_pic": (StringType(), pa.string(), False),
        "secondary_assistant_contact": (StringType(), pa.string(), False),
        "pickup_label_id": (StringType(), pa.string(), False),
        "collection_location_type": (StringType(), pa.string(), False),
        "collection_location_code": (StringType(), pa.string(), False),
        "collection_location_branchname": (StringType(), pa.string(), False),
        "collection_location_address": (StringType(), pa.string(), False),
        "collection_location_mobileno": (StringType(), pa.string(), False),
        "collection_docking_area": (StringType(), pa.string(), False),
        "collection_docking_area_code": (StringType(), pa.string(), False),
        "to_location_email": (StringType(), pa.string(), False),
        "to_location_mobileno": (StringType(), pa.string(), False),
        "to_location_address": (StringType(), pa.string(), False),
        "to_location_lat_long": (StringType(), pa.string(), False),
        "shipment_type": (StringType(), pa.string(), False),
        "home_pickup": (StringType(), pa.string(), False),
        "row_added_by": (StringType(), pa.string(), False),
        "row_updated_by": (StringType(), pa.string(), False),
        "to_location_fullname": (StringType(), pa.string(), False),
        "courier_code": (StringType(), pa.string(), False),
        "item_serial_no": (StringType(), pa.string(), False),
        "selling_price": (StringType(), pa.string(), False),
        "vendor_name": (StringType(), pa.string(), False),
        "vendor_id": (StringType(), pa.string(), False),
        "sale_invoice_no": (StringType(), pa.string(), False),
        "erp_po_createdAt": (StringType(), pa.string(), False),
        "erp_po_no": (StringType(), pa.string(), False),
        "seller_order_id": (StringType(), pa.string(), False),
        "seller_item_code": (StringType(), pa.string(), False),
        "seller_id": (StringType(), pa.string(), False),
        "seller_order_createdAt": (StringType(), pa.string(), False),
        "seller_apob_code": (StringType(), pa.string(), False),
        "unit_price": (StringType(), pa.string(), False),
        "discount_price": (StringType(), pa.string(), False),
        "plant": (StringType(), pa.string(), False),
        "seller_branch_code": (StringType(), pa.string(), False),

        # ---------------- JSON Fields (store as string for Iceberg) ----------------
        "picker_details": (StringType(), pa.string(), False),
        "order_tag": (StringType(), pa.string(), False),
        "status_notification": (StringType(), pa.string(), False),
        "vendor_details": (StringType(), pa.string(), False),
        "tracking_details": (StringType(), pa.string(), False),
        "invoiced_item_detail": (StringType(), pa.string(), False),
        "shipment_transfer_info": (StringType(), pa.string(), False),

        # ---------------- INTEGER ----------------
        "item_qty": (IntegerType(), pa.int32(), False),
        "dimension_length": (IntegerType(), pa.int32(), False),
        "dimension_height": (IntegerType(), pa.int32(), False),
        "dimension_width": (IntegerType(), pa.int32(), False),
        "reattempt_count": (IntegerType(), pa.int32(), False),
        "delivery_charge": (IntegerType(), pa.int32(), False),
        "otp_verified_status": (IntegerType(), pa.int32(), False),
        "payment_verification": (IntegerType(), pa.int32(), False),
        "dropship_flag": (IntegerType(), pa.int32(), False),

        # ---------------- DECIMAL (float) ----------------
        "latitude": (FloatType(), pa.float64(), False),
        "longitude": (FloatType(), pa.float64(), False),

        # ---------------- DATE Fields ----------------
        "sale_invoice_date": (DateType(), pa.date32(), False),

        # ---------------- TIMESTAMP Fields ----------------
        "invoice_date": (TimestampType(), pa.timestamp('ms'), False),
        "expect_delivery_pickup_dt": (TimestampType(), pa.timestamp('ms'), False),
        "picker_added_dt": (TimestampType(), pa.timestamp('ms'), False),
        "manifest_added_dt": (TimestampType(), pa.timestamp('ms'), False),
        "exchange_invo_date": (TimestampType(), pa.timestamp('ms'), False),
        "row_added_dt": (TimestampType(), pa.timestamp('ms'), False),
        "row_updated_dt": (TimestampType(), pa.timestamp('ms'), False),
        "goods_received_date": (TimestampType(), pa.timestamp('ms'), False),
        "updated_at_new": (TimestampType(), pa.timestamp('ms'), False),
    }

    # --------------------------------------------------------------------
    # AUTO DETECTION FOR FIELDS NOT IN FIELD_OVERRIDES
    # --------------------------------------------------------------------
    for idx, (name, value) in enumerate(record.items(), start=1):

        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False

            # bool
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()

            # int
            elif isinstance(value, int):
                ice_type, arrow_type = IntegerType(), pa.int32()

            # float
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()

            # date
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # timestamp
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp('ms')

            # string fallback
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    return iceberg_schema, arrow_schema