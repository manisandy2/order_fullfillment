from fastapi import APIRouter,Query,HTTPException
from core.catalog_client import get_catalog_client
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec,PartitionField
from pyiceberg.transforms import YearTransform,MonthTransform
from .Iceberg_schema import MasterSchema,Pickup_delivery_items,Status_event
from pyiceberg.types import LongType, StringType, DateType, DoubleType, NestedField, TimestampType, BooleanType, \
    IntegerType, FloatType
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError

router = APIRouter(prefix="", tags=["Tables"])


@router.get("/table/list")
def get_tables(
        namespace: str = Query(..., description="Namespace to list tables from"),

):
    try:
        catalog = get_catalog_client()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")
# original
# @router.post("/masterorders/create")
# def create(
#         # namespace: str = Query("pos_transactions"),
#         # table_name: str = Query(..., description="Table name"),
# ):
#     namespace = "order_fulfillment"
#     table_name = "master_order"
#     # table_name = "iceberg_add_range_test"
#     table_identifier = f"{namespace}.{table_name}"
#
#
#     print("Schema obj:", MasterSchema)
#     order_ff_schema = Schema(fields=MasterSchema)
#     # order_ff_schema_data = Schema(MasterSchema)
#     # Step 1: Define Iceberg schema
#     # order_ff_schema = Schema(
#     #     NestedField(1,"order_id",StringType(),required=True),
#     #     NestedField(2, "sale_order_id", StringType(),required=True),
#     #     NestedField(3, "invoice_no", StringType()),
#     #     NestedField(4, "invoice_date", TimestampType()),
#     #     NestedField(5, "invoice_reff_no", StringType()),
#     #     NestedField(6, "invoice_reff_date", StringType()),
#     #     NestedField(7, "channel", StringType()),
#     #     NestedField(8, "channel_medium", StringType()),
#     #     NestedField(9, "order_status", StringType()),
#     #     NestedField(10, "order_tag", StringType()),
#     #     NestedField(11, "order_inv_status", StringType()),
#     #     NestedField(12, "order_type", StringType()),
#     #     NestedField(13, "delivery_from", StringType()),
#     #     NestedField(14, "delivery_from_branchcode", StringType()),
#     #     NestedField(15, "billing_branch_code", StringType()),
#     #     NestedField(16, "cust_id", StringType()),
#     #     NestedField(17, "cust_primary_email", StringType()),
#     #     NestedField(18, "cust_primary_contact", StringType()),
#     #     NestedField(19, "cust_mobile", StringType()),
#     #     NestedField(20, "customer_address", StringType()),
#     #     NestedField(21, "shipment_address", StringType()),
#     #     NestedField(22, "latitude", FloatType()),
#     #     NestedField(23, "longitude", FloatType()),
#     #     NestedField(24, "billing_address", StringType()),
#     #     NestedField(25, "payment_details", StringType()),
#     #     NestedField(26, "refund_details", StringType()),
#     #     NestedField(27, "voucher_details", StringType()),
#     #     NestedField(28, "employee_sale_details", StringType()),
#     #     NestedField(29, "order_summary_details", StringType()),
#     #     NestedField(30, "other_details", StringType()),
#     #     NestedField(31, "service_details", StringType()),
#     #     NestedField(32, "invoice_pdf", StringType()),
#     #     NestedField(33, "lineitems", StringType()),
#     #     NestedField(34, "lineitem_status", StringType()),
#     #     NestedField(35, "created_at", TimestampType()),
#     #     NestedField(36, "created_by", StringType()),
#     #     NestedField(37, "updated_by", StringType()),
#     #     NestedField(38, "updated_at", TimestampType()),
#     #     NestedField(39, "oms_data_migration_status", IntegerType()),
#     #     NestedField(40, "cust_id_update", IntegerType()),
#     #     NestedField(41, "multi_invoice", StringType()),
#     #     NestedField(42, "updated_at_new", TimestampType()),
#     # )
#
#
#     # Step 2: Define partition spec
#     transaction_partition_spec = PartitionSpec(
#         PartitionField(
#             source_id=order_ff_schema.find_field("created_at").field_id,
#             field_id=2001,
#             transform=YearTransform(),
#             name="year",
#         ),
#     )
#
#     # Step 3: Connect to catalog
#     catalog = get_catalog_client()
#
#     # Step 4: Ensure namespace exists
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#     except NamespaceAlreadyExistsError:
#         pass
#
#     # Step 5: Create table
#     try:
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=order_ff_schema,
#             partition_spec=transaction_partition_spec,
#             properties={
#                 "format-version": "2",  # <-- mandatory
#                 "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
#                 "identifier-field-ids": "1", # order_id is PRIMARY KEY
#                 "write.format.default": "parquet",
#                 "write.parquet.compression-codec": "zstd",
#                 "write.partition.path-style": "hierarchical",   # hierarchical & directory
#                 # "write.sort.order": "month(Bill_Date__c) ASC, customerId,customer_mobile__c",
#                 # "write.sort.order": "customerId,customer_mobile__c",
#                 # "write.sort.order": "year ASC, order_id ASC",
#                 "write.sort.order": "year ASC, sale_order_id ASC, invoice_no ASC, invoice_date ASC",
#                 "write.target-file-size-bytes": "268435456"
#             },
#         )
#         print(f" Created Iceberg table: {table_identifier}")
#
#         # Step 6: Return confirmation
#         return {
#             "status": "created",
#             "table": table_identifier,
#             "schema_fields": [f.name for f in order_ff_schema.fields],
#             # "partitions": [f.name for f in transaction_partition_spec.fields],
#         }
#
#     except TableAlreadyExistsError:
#         return {"status": "exists", "table": table_identifier}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


@router.post("/masterorders/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "master_order"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    print("Schema obj:", MasterSchema)
    order_ff_schema = Schema(fields=MasterSchema)

    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=order_ff_schema.find_field("created_at").field_id,
            field_id=2001,
            transform=YearTransform(),
            name="year",
        ),
    )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=order_ff_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1", # order_id is PRIMARY KEY
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                # "write.sort.order": "month(Bill_Date__c) ASC, customerId,customer_mobile__c",
                # "write.sort.order": "customerId,customer_mobile__c",
                # "write.sort.order": "year ASC, order_id ASC",
                "write.sort.order": "year ASC, sale_order_id ASC, invoice_no ASC, invoice_date ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f" Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in order_ff_schema.fields],
            "partitions": [f.name for f in order_ff_schema.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/pickup_delivery_items/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "pickup_delivery_items"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema

    print("Schema obj:", Pickup_delivery_items)
    Pickup_delivery_items_schema = Schema(fields=Pickup_delivery_items)


    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=Pickup_delivery_items_schema.find_field("invoice_date").field_id,
            field_id=2001,
            transform=YearTransform(),
            name="year",
        ),
    )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=Pickup_delivery_items_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1", # order_id is PRIMARY KEY
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                # "write.sort.order": "month(Bill_Date__c) ASC, customerId,customer_mobile__c",
                # "write.sort.order": "customerId,customer_mobile__c",
                # "write.sort.order": "year ASC, order_id ASC",
                "write.sort.order": "year ASC, sale_order_id ASC, invoice_no ASC, invoice_date ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f" Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in Pickup_delivery_items_schema.fields],
            "partitions": [f.name for f in Pickup_delivery_items_schema.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/status_events/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "status_events"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema

    print("Schema obj:", MasterSchema)
    status_ff_schema = Schema(fields=MasterSchema)

    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=status_ff_schema.find_field("invoice_date").field_id,
            field_id=2001,
            transform=YearTransform(),
            name="year",
        ),
    )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=status_ff_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1", # order_id is PRIMARY KEY
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                # "write.sort.order": "month(Bill_Date__c) ASC, customerId,customer_mobile__c",
                # "write.sort.order": "customerId,customer_mobile__c",
                # "write.sort.order": "year ASC, order_id ASC",
                "write.sort.order": "invoice_date ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f" Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in status_ff_schema.fields],
            "partitions": [f.name for f in status_ff_schema.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/orderlineitems/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "orderlineitems"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema
    order_ff_schema = Schema(
        NestedField(1,"order_id",StringType(),required=True),
        NestedField(2, "sale_order_id", StringType(),required=True),
        NestedField(3, "invoice_no", StringType()),
        NestedField(4, "invoice_date", TimestampType()),
        NestedField(5, "invoice_reff_no", StringType()),
        NestedField(6, "invoice_reff_date", StringType()),
        NestedField(7, "channel", StringType()),
        NestedField(8, "channel_medium", StringType()),
        NestedField(9, "order_status", StringType()),
        NestedField(10, "order_tag", StringType()),
        NestedField(11, "order_inv_status", StringType()),
        NestedField(12, "order_type", StringType()),
        NestedField(13, "delivery_from", StringType()),
        NestedField(14, "delivery_from_branchcode", StringType()),
        NestedField(15, "billing_branch_code", StringType()),
        NestedField(16, "cust_id", StringType()),
        NestedField(17, "cust_primary_email", StringType()),
        NestedField(18, "cust_primary_contact", StringType()),
        NestedField(19, "cust_mobile", StringType()),
        NestedField(20, "customer_address", StringType()),
        NestedField(21, "shipment_address", StringType()),
        NestedField(22, "latitude", FloatType()),
        NestedField(23, "longitude", FloatType()),
        NestedField(24, "billing_address", StringType()),
        NestedField(25, "payment_details", StringType()),
        NestedField(26, "refund_details", StringType()),
        NestedField(27, "voucher_details", StringType()),
        NestedField(28, "employee_sale_details", StringType()),
        NestedField(29, "order_summary_details", StringType()),
        NestedField(30, "other_details", StringType()),
        NestedField(31, "service_details", StringType()),
        NestedField(32, "invoice_pdf", StringType()),
        NestedField(33, "lineitems", StringType()),
        NestedField(34, "lineitem_status", StringType()),
        NestedField(35, "created_at", TimestampType()),
        NestedField(36, "created_by", StringType()),
        NestedField(37, "updated_by", StringType()),
        NestedField(38, "updated_at", TimestampType()),
        NestedField(39, "oms_data_migration_status", IntegerType()),
        NestedField(40, "cust_id_update", IntegerType()),
        NestedField(41, "multi_invoice", StringType()),
        NestedField(42, "updated_at_new", TimestampType()),
    )


    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=order_ff_schema.find_field("created_at").field_id,
            field_id=2001,
            transform=YearTransform(),
            name="year",
        ),
    )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=order_ff_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1", # order_id is PRIMARY KEY
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                # "write.sort.order": "month(Bill_Date__c) ASC, customerId,customer_mobile__c",
                # "write.sort.order": "customerId,customer_mobile__c",
                # "write.sort.order": "year ASC, order_id ASC",
                "write.sort.order": "year ASC, sale_order_id ASC, invoice_no ASC, invoice_date ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f" Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in order_ff_schema.fields],
            # "partitions": [f.name for f in transaction_partition_spec.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/table/rename")
def rename_table(
    namespace: str = Query(..., description="Namespace containing the table"),
    old_table_name: str = Query(..., description="Current table name (e.g. 'transactions')"),
    new_table_name: str = Query(..., description="New table name (e.g. 'transactions_v2')"),

):

    catalog = get_catalog_client()
    try:

        old_identifier = f"{namespace}.{old_table_name}"
        new_identifier = f"{namespace}.{new_table_name}"

        catalog.rename_table(old_identifier, new_identifier)

        return {
            "status": "success",
            "message": f"Table renamed from '{old_table_name}' to '{new_table_name}' in namespace '{namespace}' successfully."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rename table '{old_table_name}' in namespace '{namespace}': {str(e)}")

    finally:
        try:
            catalog.close()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to close catalog: {str(e)}")



@router.delete("/table/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop"),

):
    catalog = get_catalog_client()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")
