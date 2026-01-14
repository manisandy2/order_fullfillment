


TABLE_LIST = [
    "masterorders",
    "masterorders_w",
    "pickup_delivery_items",
    "pickup_delivery_items_w",
    "orderlineitems",
    "status_events",
    "bluedart_zone_masters",
    "courier_masters",
    "drivers",
    "exchange_informations",
    "schedulers_w",
    "service_history_c",
    "service_history_h",
]


# def table_identifier(table_name: str) -> str:
#     namespace = "order_fulfillment"
#
#     return f"{namespace}.{table_name}"
#
#
# TABLE_CONFIG = {
#         "masterorders": {
#         "columns" : [
#         "order_id",
#         "sale_order_id",
#         "invoice_no",
#         "invoice_date",
#         "invoice_reff_no",
#         "invoice_reff_date",
#         "channel",
#         "channel_medium",
#         "order_status",
#         "order_tag",
#         "order_inv_status",
#         "order_type",
#         "delivery_from",
#         "delivery_from_branchcode",
#         "billing_branch_code",
#         "cust_id",
#         "cust_primary_email",
#         "cust_primary_contact",
#         "cust_mobile",
#         "customer_address",
#         "shipment_address",
#         "latitude",
#         "longitude",
#         "billing_address",
#         "payment_details",
#         "refund_details",
#         "voucher_details",
#         "employee_sale_details",
#         "order_summary_details",
#         "other_details",
#         "service_details",
#         "invoice_pdf",
#         "lineitems",
#         "lineitem_status",
#         "created_at",
#         "created_by",
#         "updated_by",
#         "updated_at",
#         "oms_data_migration_status",
#         "cust_id_update",
#         "multi_invoice",
#         "updated_at_new" ],
#         "order_by": "create_at",
#     },
#     "masterorders_w": {
#         "columns" : [
#
#     "order_id",
#     "sale_order_id",
#     "invoice_no",
#     "invoice_date",
#     "invoice_reff_no",
#     "invoice_reff_date",
#     "channel",
#     "channel_medium",
#     "order_status",
#     "order_tag",
#     "order_inv_status",
#     "order_type",
#     "delivery_from",
#     "delivery_from_branchcode",
#     "billing_branch_code",
#     "cust_id",
#     "cust_primary_email",
#     "cust_primary_contact",
#     "cust_mobile",
#     "customer_address",
#     "shipment_address",
#     "latitude",
#     "longitude",
#     "billing_address",
#     "payment_details",
#     "refund_details",
#     "voucher_details",
#     "employee_sale_details",
#     "order_summary_details",
#     "other_details",
#     "service_details",
#     "invoice_pdf",
#     "lineitems",
#     "lineitem_status",
#     "created_at",
#     "created_by",
#     "updated_by",
#     "updated_at",
#     "oms_data_migration_status",
#     "cust_id_update",
#     "multi_invoice",
#     "updated_at_new",
# ],
#         "order_by": "create_at",
#     },
# }

# for table_name, config in TABLE_CONFIG.items():
#     print(f"Table: {table_name}")
#     print(f"Columns: {config['columns']}")
#     print(f"Order by: {config['order_by']}")
#     print("-" * 40)