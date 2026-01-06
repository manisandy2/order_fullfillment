from fastapi import APIRouter, HTTPException
# from app.iceberg.registry import TABLE_REGISTRY
# from app.iceberg.catalog import get_catalog

# router = APIRouter()
#
# @router.post("/{table}/create")
# def create_table(table: str):
#     if table not in TABLE_REGISTRY:
#         raise HTTPException(404, "Table not registered")
#
#     cfg = TABLE_REGISTRY[table]()
#     catalog = get_catalog()
#
#     identifier = f"{cfg['namespace']}.{cfg['table']}"
#
#     if catalog.table_exists(identifier):
#         return {"status": "exists", "table": identifier}
#
#     catalog.create_table(
#         identifier=identifier,
#         schema=cfg["schema"],
#         partition_spec=cfg.get("partition")
#     )
#
#     return {"status": "created", "table": identifier}