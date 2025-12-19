from fastapi import APIRouter,Query,HTTPException
from core.catalog_client import get_catalog_client
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec,PartitionField
from pyiceberg.transforms import YearTransform
from .Iceberg_schema import MasterSchema, Pickup_delivery_items, Status_event, OrderLineItems, OrderLineItems_test
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError
from core.logger import get_logger
import re

logger = get_logger("table-api")
router = APIRouter(prefix="", tags=["Tables"])


def validate_namespace(namespace: str) -> None:
    """Validate namespace format"""
    if not namespace or not namespace.strip():
        raise HTTPException(
            status_code=400,
            detail="Namespace cannot be empty"
        )
    
    if not re.match(r'^[a-zA-Z0-9_]+$', namespace):
        raise HTTPException(
            status_code=400,
            detail="Namespace must contain only alphanumeric characters and underscores"
        )


@router.get("/table/list")
def get_tables(
    namespace: str = Query(..., description="Namespace to list tables from"),
):
    """
    List all tables in a given namespace
    
    Args:
        namespace: Name of the namespace to query
        
    Returns:
        Dictionary with namespace, tables list, and count
    """
    # Validate input
    validate_namespace(namespace)
    
    logger.info(f"Listing tables in namespace: {namespace}")
    
    try:
        catalog = get_catalog_client()
        tables = catalog.list_tables(namespace)

        logger.info(f"Found {len(tables)} tables in namespace '{namespace}'")
        
        return {
            "success": True,
            "namespace": namespace,
            "tables": tables,
            "count": len(tables)
        }

    except NoSuchNamespaceError:
        logger.warning(f"Namespace not found: {namespace}")
        raise HTTPException(
            status_code=404,
            detail=f"Namespace '{namespace}' does not exist"
        )
    except Exception as e:
        logger.exception(f"Failed to list tables in namespace '{namespace}'")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list tables: {str(e)}"
        )


# masterOrder
@router.post("/masterorders/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "masterorders"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    logger.info(f"Creating table: {table_identifier}")
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
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(order_ff_schema.fields),
            "partition_by": "year(created_at)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

@router.post("/masterorders-w/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "masterorders_w"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    logger.info(f"Creating table: {table_identifier}")
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
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(order_ff_schema.fields),
            "partition_by": "year(created_at)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

# pickup_delivery_items
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

    logger.info(f"Creating table: {table_identifier}")
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
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(Pickup_delivery_items_schema.fields),
            "partition_by": "year(invoice_date)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

@router.post("/pickup_delivery_items_w/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "pickup_delivery_items_w"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema

    logger.info(f"Creating table: {table_identifier}")
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
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(Pickup_delivery_items_schema.fields),
            "partition_by": "year(invoice_date)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

# status_events
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

    logger.info(f"Creating table: {table_identifier}")
    status_ff_schema = Schema(fields=Status_event)

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
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(status_ff_schema.fields),
            "partition_by": "year(invoice_date)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

# orderlineitems
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
    logger.info(f"Creating table: {table_identifier}")
    orderlineitems_ff_schema = Schema(fields=OrderLineItems)


    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=orderlineitems_ff_schema.find_field("created_at").field_id,
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
            schema=orderlineitems_ff_schema,
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
                "write.sort.order": "created_at ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(orderlineitems_ff_schema.fields),
            "partition_by": "year(created_at)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

# orderlineitems
@router.post("/orderlineitems_test/create")
def create(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "orderlineitems_test"
    # table_name = "iceberg_add_range_test"
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema
    logger.info(f"Creating table: {table_identifier}")
    orderlineitems_ff_schema = Schema(fields=OrderLineItems_test)


    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=orderlineitems_ff_schema.find_field("created_at").field_id,
            field_id=1001,
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
            schema=orderlineitems_ff_schema,
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
                "write.sort.order": "created_at ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        logger.info(f"Successfully created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "success": True,
            "status": "created",
            "table": table_identifier,
            "schema_field_count": len(orderlineitems_ff_schema.fields),
            "partition_by": "year(created_at)"
        }

    except TableAlreadyExistsError:
        logger.info(f"Table already exists: {table_identifier}")
        return {
            "success": True,
            "status": "exists",
            "table": table_identifier,
            "message": "Table already exists (idempotent)"
        }
    except Exception as e:
        logger.exception(f"Failed to create table {table_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_CREATION_FAILED",
                "message": f"Failed to create table '{table_identifier}'",
                "details": str(e),
                "table": table_identifier
            }
        )

@router.post("/table/rename")
def rename_table(
    namespace: str = Query(..., description="Namespace containing the table"),
    old_table_name: str = Query(..., description="Current table name (e.g. 'transactions')"),
    new_table_name: str = Query(..., description="New table name (e.g. 'transactions_v2')"),

):

    logger.info(f"Renaming table from {old_identifier} to {new_identifier}")
    
    catalog = get_catalog_client()
    try:
        old_identifier = f"{namespace}.{old_table_name}"
        new_identifier = f"{namespace}.{new_table_name}"

        catalog.rename_table(old_identifier, new_identifier)
        
        logger.info(f"Successfully renamed table: {old_identifier} -> {new_identifier}")

        return {
            "success": True,
            "status": "renamed",
            "old_table": old_identifier,
            "new_table": new_identifier,
            "message": f"Table renamed successfully"
        }

    except NoSuchTableError:
        logger.warning(f"Table not found for rename: {old_identifier}")
        raise HTTPException(
            status_code=404,
            detail={
                "error": "TABLE_NOT_FOUND",
                "message": f"Table '{old_identifier}' does not exist",
                "table": old_identifier
            }
        )
    except Exception as e:
        logger.exception(f"Failed to rename table {old_identifier}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_RENAME_FAILED",
                "message": f"Failed to rename table '{old_identifier}'",
                "details": str(e),
                "old_table": old_identifier,
                "new_table": new_identifier
            }
        )



@router.delete("/table/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop"),

):
    logger.info(f"Deleting table: {full_table_name}")
    
    catalog = get_catalog_client()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        
        logger.info(f"Successfully deleted table: {full_table_name}")
        
        return {
            "success": True,
            "status": "deleted",
            "table": full_table_name,
            "message": f"Table '{full_table_name}' deleted successfully"
        }

    except NoSuchTableError:
        logger.warning(f"Table not found for deletion: {full_table_name}")
        raise HTTPException(
            status_code=404,
            detail={
                "error": "TABLE_NOT_FOUND",
                "message": f"Table '{full_table_name}' does not exist",
                "table": full_table_name
            }
        )
    except Exception as e:
        logger.exception(f"Failed to delete table {full_table_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "TABLE_DELETE_FAILED",
                "message": f"Failed to delete table '{full_table_name}'",
                "details": str(e),
                "table": full_table_name
            }
        )
