from fastapi import APIRouter,HTTPException
from pyiceberg.partitioning import PartitionSpec,PartitionField
from pyiceberg.schema import Schema
from pyiceberg.transforms import YearTransform

from .schema import Exchange_informations
from ..table import _create_iceberg_table

router = APIRouter(prefix="/exchange_informations", tags=["exchange_informations"])

@router.post("/create",summary="Create exchange_informations table")
def create_exchange_informations_table(
        # namespace: str = Query("pos_transactions"),
        # table_name: str = Query(..., description="Table name"),
):
    namespace = "order_fulfillment"
    table_name = "exchange_informations"

    try:
        exchange_schema = Schema(*Exchange_informations)

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=exchange_schema.find_field("created_at").field_id,
                field_id=1001,
                transform=YearTransform(),
                name="created_year",
            ),
        )

        return _create_iceberg_table(
            namespace=namespace,
            table_name=table_name,
            schema=exchange_schema,
            partition_spec=partition_spec,
            sort_order="order_id ASC"
        )
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"Failed to create Iceberg table: {str(e)}")