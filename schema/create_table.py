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

