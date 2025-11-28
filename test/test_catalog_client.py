import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
import sys
import os

# Add the core directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))

from catalog_client import Creds, get_catalog_client


class TestCreds:
    """Test cases for the Creds class"""

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "test-token",
        "CATALOG_NAME": "test-catalog"
    })
    def test_creds_initialization_success(self):
        """Test successful Creds initialization with all environment variables"""
        creds = Creds()
        assert creds.CATALOG_URI == "http://localhost:8181"
        assert creds.WAREHOUSE == "s3://my-warehouse"
        assert creds.TOKEN == "test-token"
        assert creds.CATALOG_NAME == "test-catalog"

    @patch.dict("os.environ", {}, clear=True)
    def test_creds_initialization_missing_env_vars(self):
        """Test Creds initialization with missing environment variables"""
        creds = Creds()
        assert creds.CATALOG_URI is None
        assert creds.WAREHOUSE is None
        assert creds.TOKEN is None
        assert creds.CATALOG_NAME is None

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "test-token",
        "CATALOG_NAME": "test-catalog"
    })
    @patch("catalog_client.RestCatalog")
    def test_catalog_valid_success(self, mock_rest_catalog):
        """Test successful catalog validation and creation"""
        mock_catalog_instance = MagicMock()
        mock_rest_catalog.return_value = mock_catalog_instance
        
        creds = Creds()
        result = creds.catalog_valid()
        
        mock_rest_catalog.assert_called_once_with(
            name="test-catalog",
            warehouse="s3://my-warehouse",
            uri="http://localhost:8181",
            token="test-token"
        )
        assert result == mock_catalog_instance

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "",
        "CATALOG_NAME": "test-catalog"
    })
    def test_catalog_valid_missing_token(self):
        """Test catalog validation fails when TOKEN is empty"""
        creds = Creds()
        with pytest.raises(ValueError) as exc_info:
            creds.catalog_valid()
        assert "Missing environment variables" in str(exc_info.value)

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "",
        "TOKEN": "test-token",
        "CATALOG_NAME": "test-catalog"
    })
    def test_catalog_valid_missing_warehouse(self):
        """Test catalog validation fails when WAREHOUSE is empty"""
        creds = Creds()
        with pytest.raises(ValueError) as exc_info:
            creds.catalog_valid()
        assert "Missing environment variables" in str(exc_info.value)

    @patch.dict("os.environ", {
        "CATALOG_URI": "",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "test-token",
        "CATALOG_NAME": "test-catalog"
    })
    def test_catalog_valid_missing_catalog_uri(self):
        """Test catalog validation fails when CATALOG_URI is empty"""
        creds = Creds()
        with pytest.raises(ValueError) as exc_info:
            creds.catalog_valid()
        assert "Missing environment variables" in str(exc_info.value)

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "test-token",
        "CATALOG_NAME": ""
    })
    def test_catalog_valid_missing_catalog_name(self):
        """Test catalog validation fails when CATALOG_NAME is empty"""
        creds = Creds()
        with pytest.raises(ValueError) as exc_info:
            creds.catalog_valid()
        assert "Missing environment variables" in str(exc_info.value)


class TestGetCatalogClient:
    """Test cases for the get_catalog_client function"""

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "test-token",
        "CATALOG_NAME": "test-catalog"
    })
    @patch("catalog_client.RestCatalog")
    def test_get_catalog_client_success(self, mock_rest_catalog):
        """Test successful catalog client retrieval"""
        mock_catalog_instance = MagicMock()
        mock_rest_catalog.return_value = mock_catalog_instance
        
        result = get_catalog_client()
        
        assert result == mock_catalog_instance

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "",
        "CATALOG_NAME": "test-catalog"
    })
    def test_get_catalog_client_missing_env_vars(self):
        """Test get_catalog_client fails with missing environment variables"""
        with pytest.raises(HTTPException) as exc_info:
            get_catalog_client()
        assert exc_info.value.status_code == 500
        assert "Iceberg catalog client initialization failed" in exc_info.value.detail

    @patch.dict("os.environ", {}, clear=True)
    def test_get_catalog_client_all_missing(self):
        """Test get_catalog_client fails when all environment variables are missing"""
        with pytest.raises(HTTPException) as exc_info:
            get_catalog_client()
        assert exc_info.value.status_code == 500

    @patch.dict("os.environ", {
        "CATALOG_URI": "http://localhost:8181",
        "WAREHOUSE": "s3://my-warehouse",
        "TOKEN": "test-token",
        "CATALOG_NAME": "test-catalog"
    })
    @patch("catalog_client.RestCatalog")
    def test_get_catalog_client_handles_general_exception(self, mock_rest_catalog):
        """Test get_catalog_client handles general exceptions"""
        mock_rest_catalog.side_effect = Exception("Connection failed")
        
        with pytest.raises(HTTPException) as exc_info:
            get_catalog_client()
        assert exc_info.value.status_code == 500
        assert "Iceberg catalog client initialization failed" in exc_info.value.detail
        assert "Connection failed" in exc_info.value.detail
