import pytest
from unittest.mock import patch, MagicMock, Mock
from mysql.connector import Error
import sys
import os

# Add the core directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))

from mysql_client import mysql_connect, MysqlCatalog


class TestMysqlConnect:
    """Test cases for mysql_connect function"""

    @patch("mysql_client.mysql.connector.connect")
    @patch.dict("os.environ", {
        "HOST": "localhost",
        "USERNAME": "root",
        "PASSWORD": "password",
        "DATABASE": "testdb",
        "PORT": "3306"
    })
    def test_mysql_connect_success(self, mock_connect):
        """Test successful MySQL connection"""
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_connect.return_value = mock_conn
        
        conn = mysql_connect()
        
        assert conn is not None
        assert conn.is_connected()
        mock_connect.assert_called_once()

    @patch("mysql_client.mysql.connector.connect")
    @patch.dict("os.environ", {
        "HOST": "localhost",
        "USERNAME": "root",
        "PASSWORD": "password",
        "DATABASE": "testdb",
        "PORT": "3306"
    })
    def test_mysql_connect_failure(self, mock_connect):
        """Test MySQL connection failure"""
        mock_connect.side_effect = Error("Connection refused")
        
        conn = mysql_connect()
        
        assert conn is None

    @patch("mysql_client.mysql.connector.connect")
    @patch.dict("os.environ", {
        "HOST": "localhost",
        "USERNAME": "root",
        "PASSWORD": "password",
        "DATABASE": "testdb",
        "PORT": "3306"
    })
    def test_mysql_connect_not_connected(self, mock_connect):
        """Test when is_connected returns False"""
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = False
        mock_connect.return_value = mock_conn
        
        with pytest.raises(ConnectionError):
            mysql_connect()


class TestMysqlCatalogInit:
    """Test cases for MysqlCatalog initialization"""

    @patch("mysql_client.mysql_connect")
    def test_mysql_catalog_init_success(self, mock_mysql_connect):
        """Test successful MysqlCatalog initialization"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        catalog = MysqlCatalog()
        
        assert catalog.conn is not None
        assert catalog.cursor is not None
        mock_conn.cursor.assert_called_once_with(dictionary=True)

    @patch("mysql_client.mysql_connect")
    def test_mysql_catalog_init_connection_failed(self, mock_mysql_connect):
        """Test MysqlCatalog initialization when connection fails"""
        mock_mysql_connect.return_value = None
        
        with pytest.raises(AttributeError):
            catalog = MysqlCatalog()


class TestGetAllValue:
    """Test cases for get_all_value method"""

    @patch("mysql_client.mysql_connect")
    def test_get_all_value_success(self, mock_mysql_connect):
        """Test successful retrieval of all values"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Order 1"},
            {"id": 2, "name": "Order 2"}
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_all_value("masterorders")
        
        assert len(result) == 2
        mock_cursor.execute.assert_called_once_with("SELECT * FROM masterorders")

    @patch("mysql_client.mysql_connect")
    def test_get_all_value_empty(self, mock_mysql_connect):
        """Test retrieval when table is empty"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = []
        
        catalog = MysqlCatalog()
        result = catalog.get_all_value("masterorders")
        
        assert result == []


class TestGetCount:
    """Test cases for get_count method"""

    @patch("mysql_client.mysql_connect")
    def test_get_count_success(self, mock_mysql_connect):
        """Test successful count retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchone.return_value = (42,)
        
        catalog = MysqlCatalog()
        result = catalog.get_count("masterorders")
        
        assert result == 42
        mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) AS total FROM `masterorders`")

    @patch("mysql_client.mysql_connect")
    def test_get_count_zero(self, mock_mysql_connect):
        """Test count when table is empty"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchone.return_value = (0,)
        
        catalog = MysqlCatalog()
        result = catalog.get_count("masterorders")
        
        assert result == 0


class TestGetDescribe:
    """Test cases for get_describe method"""

    @patch("mysql_client.mysql_connect")
    def test_get_describe_success(self, mock_mysql_connect):
        """Test successful describe retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {"Field": "id", "Type": "INT"},
            {"Field": "name", "Type": "VARCHAR(255)"}
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_describe("masterorders")
        
        assert len(result) == 2
        mock_cursor.execute.assert_called_once_with("DESCRIBE masterorders")


class TestGetRange:
    """Test cases for get_range method"""

    @patch("mysql_client.mysql_connect")
    def test_get_range_success(self, mock_mysql_connect):
        """Test successful range retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Order 1"},
            {"id": 2, "name": "Order 2"},
            {"id": 3, "name": "Order 3"}
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_range("masterorders", 0, 3)
        
        assert len(result) == 3
        mock_cursor.execute.assert_called_once_with("SELECT * FROM masterorders LIMIT 0, 3")

    @patch("mysql_client.mysql_connect")
    def test_get_range_with_offset(self, mock_mysql_connect):
        """Test range retrieval with offset"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {"id": 11, "name": "Order 11"},
            {"id": 12, "name": "Order 12"}
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_range("masterorders", 10, 12)
        
        assert len(result) == 2
        mock_cursor.execute.assert_called_once_with("SELECT * FROM masterorders LIMIT 10, 2")


class TestGetRangePhBi:
    """Test cases for get_range_ph_bi method"""

    @patch("mysql_client.mysql_connect")
    def test_get_range_ph_bi_success(self, mock_mysql_connect):
        """Test successful get_range_ph_bi retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {
                "order_id": 1,
                "sale_order_id": "SO001",
                "invoice_no": "INV001",
                "invoice_date": "2024-01-01",
                "channel": "online",
                "cust_id": "CUST001"
            },
            {
                "order_id": 2,
                "sale_order_id": "SO002",
                "invoice_no": "INV002",
                "invoice_date": "2024-01-02",
                "channel": "retail",
                "cust_id": "CUST002"
            }
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_range_ph_bi("masterorders", 0, 2)
        
        assert len(result) == 2
        assert result[0]["order_id"] == 1
        assert result[1]["order_id"] == 2
        mock_cursor.execute.assert_called_once()

    @patch("mysql_client.mysql_connect")
    def test_get_range_ph_bi_empty(self, mock_mysql_connect):
        """Test get_range_ph_bi when no results"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = []
        
        catalog = MysqlCatalog()
        result = catalog.get_range_ph_bi("masterorders", 0, 10)
        
        assert result == []

    @patch("mysql_client.mysql_connect")
    def test_get_range_ph_bi_with_offset(self, mock_mysql_connect):
        """Test get_range_ph_bi with offset"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {"order_id": 101, "sale_order_id": "SO101"}
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_range_ph_bi("masterorders", 100, 101)
        
        assert len(result) == 1
        assert result[0]["order_id"] == 101
        # Verify that LIMIT parameters are correct
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == (100, 1)  # (start, end - start)

    @patch("mysql_client.mysql_connect")
    def test_get_range_ph_bi_exception_handling(self, mock_mysql_connect):
        """Test get_range_ph_bi exception handling"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.execute.side_effect = Exception("Database error")
        
        catalog = MysqlCatalog()
        result = catalog.get_range_ph_bi("masterorders", 0, 10)
        
        assert result == []

    @patch("mysql_client.mysql_connect")
    def test_get_range_ph_bi_query_structure(self, mock_mysql_connect):
        """Test that get_range_ph_bi constructs correct query"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = []
        
        catalog = MysqlCatalog()
        catalog.get_range_ph_bi("masterorders", 5, 15)
        
        # Verify query was called with correct parameters
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        # Check query contains key elements
        assert "SELECT" in query
        assert "order_id" in query
        assert "FROM masterorders" in query
        assert "ORDER BY order_id ASC" in query
        assert "LIMIT %s, %s" in query
        assert params == (5, 10)  # start=5, end-start=10


class TestGetSchema:
    """Test cases for get_schema method"""

    @patch("mysql_client.mysql_connect")
    def test_get_schema_success(self, mock_mysql_connect):
        """Test successful schema retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {
                "Field": "order_id",
                "Type": "INT",
                "Null": "NO",
                "Key": "PRI",
                "Default": None,
                "Extra": "auto_increment"
            },
            {
                "Field": "customer_name",
                "Type": "VARCHAR(255)",
                "Null": "YES",
                "Key": "",
                "Default": None,
                "Extra": ""
            }
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_schema("masterorders")
        
        assert len(result) == 2
        assert result[0]["name"] == "order_id"
        assert result[0]["type"] == "INT"
        assert result[0]["nullable"] == False
        assert result[0]["key"] == "PRI"
        assert result[1]["nullable"] == True

    @patch("mysql_client.mysql_connect")
    def test_get_schema_complex_types(self, mock_mysql_connect):
        """Test schema retrieval with various column types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [
            {
                "Field": "created_at",
                "Type": "TIMESTAMP",
                "Null": "NO",
                "Key": "",
                "Default": "CURRENT_TIMESTAMP",
                "Extra": ""
            },
            {
                "Field": "email",
                "Type": "VARCHAR(100)",
                "Null": "NO",
                "Key": "UNI",
                "Default": None,
                "Extra": ""
            }
        ]
        
        catalog = MysqlCatalog()
        result = catalog.get_schema("masterorders")
        
        assert result[0]["type"] == "TIMESTAMP"
        assert result[1]["key"] == "UNI"


class TestClose:
    """Test cases for close method"""

    @patch("mysql_client.mysql_connect")
    def test_close_success(self, mock_mysql_connect):
        """Test successful connection close"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        catalog = MysqlCatalog()
        catalog.close()
        
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("mysql_client.mysql_connect")
    def test_close_with_none_cursor(self, mock_mysql_connect):
        """Test close when cursor is None"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        catalog = MysqlCatalog()
        catalog.cursor = None
        catalog.close()
        
        mock_conn.close.assert_called_once()

    @patch("mysql_client.mysql_connect")
    def test_close_with_none_conn(self, mock_mysql_connect):
        """Test close when connection is None"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mysql_connect.return_value = mock_conn
        
        catalog = MysqlCatalog()
        catalog.conn = None
        catalog.close()
        
        mock_cursor.close.assert_called_once()
