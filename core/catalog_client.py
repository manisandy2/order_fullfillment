"""
This module provides a function to initialize and
retrieve an Iceberg catalog client using
environment variables for configuration.
It defines a `Creds` class to manage the credentials and a
"""

import logging
import os

from dotenv import load_dotenv
from fastapi import HTTPException
from pyiceberg.catalog.rest import RestCatalog
from dataclasses import dataclass

from .config import Config

load_dotenv(".env")
logger = logging.getLogger(__name__)


class Creds:
    """
        Load and validate Iceberg catalog credentials.
    """

    def __init__(self) -> None:
        """Initializes the Creds instance by loading environment variables."""
        self.CATALOG_URI: str | None = os.getenv("CATALOG_URI")
        self.WAREHOUSE: str | None = os.getenv("WAREHOUSE")
        self.TOKEN: str | None = os.getenv("TOKEN")
        self.CATALOG_NAME: str | None = os.getenv("CATALOG_NAME")

    def load(self) -> None:
        """Load credentials from environment variables."""
        self.catalog_uri = os.getenv("CATALOG_URI")
        self.warehouse = os.getenv("WAREHOUSE")
        self.token = os.getenv("TOKEN")
        self.catalog_name = os.getenv("CATALOG_NAME")

    def validate(self) -> RestCatalog:
        """Validate credentials and return RestCatalog client."""

        if not all(
            [self.catalog_uri, self.warehouse, self.token, self.catalog_name]
        ):
            raise ValueError(
                "Missing required environment variables: "
                "CATALOG_URI, WAREHOUSE, TOKEN, CATALOG_NAME"
            )

        return RestCatalog(
            name=self.catalog_name,
            warehouse=self.warehouse,
            uri=self.catalog_uri,
            token=self.token,
        )


# def get_catalog_client() -> RestCatalog:
#     """ Initializes and retrieves an Iceberg catalog client.
#      It creates an instance of the Creds class and calls
#       the catalog_valid method to get a RestCatalog instance.
#      If any exceptions occur during this process,
#       it logs the error and raises an HTTPException with a 500 status code.
#     """
#     try:
#         return Creds().catalog_valid()
#     except Exception as e:
#         logger.error(f"Failed to initialize Iceberg catalog client: %s", {e})
#         raise HTTPException(
#             status_code=500,
#             detail=(f"Iceberg catalog client initialization failed: %s", {e})
#             )


def get_catalog_client() -> RestCatalog:
    """Initializes and retrieves an Iceberg catalog client.
    It creates an instance of the Creds class and calls 
    the catalog_valid method to get a RestCatalog instance.
    If any exceptions occur during this process, 
    it logs the error and raises an HTTPException with a 500 status code.
    """
    try:
        return RestCatalog(
            name=Config.CATALOG_NAME,
            warehouse=Config.WAREHOUSE,
            uri=Config.CATALOG_URI,
            token=Config.TOKEN,
        )
    except Exception as e:
        logger.error("Failed to initialize Iceberg catalog client: %s", {e})
        raise HTTPException(
            status_code=500,
            detail=("Iceberg catalog client initialization failed: %s", {e}),
        ) from e
