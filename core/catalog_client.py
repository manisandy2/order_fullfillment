from dotenv import load_dotenv
import os
from pyiceberg.catalog.rest import RestCatalog
import logging
from fastapi import HTTPException

load_dotenv(".env")
logger = logging.getLogger(__name__)


class Creds:
    def __init__(self) -> None:
        self.CATALOG_URI: str | None = os.getenv("CATALOG_URI")
        self.WAREHOUSE: str | None = os.getenv("WAREHOUSE")
        self.TOKEN: str | None = os.getenv("TOKEN")
        self.CATALOG_NAME: str | None = os.getenv("CATALOG_NAME")

        # print(self.CATALOG_URI)
        # print(self.WAREHOUSE)
        # print(self.TOKEN)
        # print(self.CATALOG_NAME)

    def catalog_valid(self) -> RestCatalog:
        if not all([self.CATALOG_URI, self.WAREHOUSE, self.TOKEN, self.CATALOG_NAME]):
            raise ValueError("Missing environment variables. Please check CATALOG_URI, WAREHOUSE, TOKEN, or CATALOG_NAME.")

        return RestCatalog(
            name=self.CATALOG_NAME,
            warehouse=self.WAREHOUSE,
            uri=self.CATALOG_URI,
            token=self.TOKEN
        )
    
    
def get_catalog_client() -> RestCatalog:
    try:
        return Creds().catalog_valid()
    except Exception as e:
        logger.error(f"Failed to initialize Iceberg catalog client: {e}")
        raise HTTPException(status_code=500, detail=f"Iceberg catalog client initialization failed: {e}")