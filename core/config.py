# from pydantic import BaseModel
# import os
# from dotenv import load_dotenv
#
# load_dotenv()
#
# class Config(BaseModel):
#     """
#     A configuration class that loads environment variables for the application.
#      It uses Pydantic's BaseModel to define the expected configuration fields and their types.
#      The required environment variables are:
#      - CATALOG_URI: The URI of the Iceberg catalog.
#      - WAREHOUSE: The warehouse name for the catalog.
#      - TOKEN: The authentication token for accessing the catalog.
#      - CATALOG_NAME: The name of the catalog to be used.
#         The `Config` class will automatically load these variables from the environment when an instance is created.
#     """
#     CATALOG_URI: str
#     WAREHOUSE: str
#     TOKEN: str
#     CATALOG_NAME: str
#
#     class Config:
#         env_file = ".env"
#         env_file_encoding = "utf-8"
#
# Config = Config()  # Load configuration from environment variables