import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import HTTPException, UploadFile
from fastapi.testclient import TestClient
from botocore.exceptions import ClientError, EndpointConnectionError
import sys
import os
import json

# Add the core and routers directories to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from routers.bucket import router


# Create a test client using FastAPI's TestClient
@pytest.fixture
def client():
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestListBuckets:
    """Test cases for list_buckets endpoint"""

    @patch("routers.bucket.get_r2_client")
    def test_list_buckets_success(self, mock_get_r2_client, client):
        """Test successful bucket listing"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket1"},
                {"Name": "bucket2"},
                {"Name": "bucket3"}
            ]
        }
        
        response = client.get("/Transaction/list-buckets")
        
        assert response.status_code == 200
        assert response.json() == {"buckets": ["bucket1", "bucket2", "bucket3"]}

    @patch("routers.bucket.get_r2_client")
    def test_list_buckets_empty(self, mock_get_r2_client, client):
        """Test listing when no buckets exist"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_buckets.return_value = {"Buckets": []}
        
        response = client.get("/Transaction/list-buckets")
        
        assert response.status_code == 200
        assert response.json() == {"buckets": []}

    @patch("routers.bucket.get_r2_client")
    def test_list_buckets_connection_error(self, mock_get_r2_client, client):
        """Test connection error handling"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_buckets.side_effect = EndpointConnectionError(endpoint_url="http://localhost")
        
        response = client.get("/Transaction/list-buckets")
        
        assert response.status_code == 503
        assert "Connection error" in response.json()["detail"]

    @patch("routers.bucket.get_r2_client")
    def test_list_buckets_client_error(self, mock_get_r2_client, client):
        """Test ClientError handling"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        error_response = {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access Denied"
            }
        }
        mock_r2_client.list_buckets.side_effect = ClientError(error_response, "ListBuckets")
        
        response = client.get("/Transaction/list-buckets")
        
        assert response.status_code == 400
        assert "AccessDenied" in response.json()["detail"]

    @patch("routers.bucket.get_r2_client")
    def test_list_buckets_unknown_error(self, mock_get_r2_client, client):
        """Test unknown error handling"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_buckets.side_effect = Exception("Unexpected error")
        
        response = client.get("/Transaction/list-buckets")
        
        assert response.status_code == 500
        assert "Unknown Error" in response.json()["detail"]


class TestCreateBucket:
    """Test cases for create_bucket endpoint"""

    @patch("routers.bucket.get_r2_client")
    def test_create_bucket_success(self, mock_get_r2_client, client):
        """Test successful bucket creation"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        response = client.post("/Transaction/create-bucket?bucket_name=new-bucket")
        
        assert response.status_code == 200
        assert response.json() == {"message": "Bucket 'new-bucket' created successfully"}
        mock_r2_client.create_bucket.assert_called_once_with(Bucket="new-bucket")

    @patch("routers.bucket.get_r2_client")
    def test_create_bucket_already_exists(self, mock_get_r2_client, client):
        """Test error when bucket already exists"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        error_response = {
            "Error": {
                "Code": "BucketAlreadyExists",
                "Message": "The bucket already exists"
            }
        }
        mock_r2_client.create_bucket.side_effect = ClientError(error_response, "CreateBucket")
        
        response = client.post("/Transaction/create-bucket?bucket_name=existing-bucket")
        
        assert response.status_code == 200
        assert "error" in response.json()


class TestUploadObject:
    """Test cases for upload_object endpoint"""

    @patch("routers.bucket.get_r2_client")
    def test_upload_object_success(self, mock_get_r2_client, client):
        """Test successful file upload"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        response = client.post(
            "/Transaction/upload-object?bucket_name=test-bucket&object_key=test.txt",
            files={"file": ("test.txt", b"test content")}
        )
        
        assert response.status_code == 200
        assert "uploaded" in response.json()["message"]

    @patch("routers.bucket.get_r2_client")
    def test_upload_object_client_error(self, mock_get_r2_client, client):
        """Test upload with ClientError"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist"
            }
        }
        mock_r2_client.put_object.side_effect = ClientError(error_response, "PutObject")
        
        response = client.post(
            "/Transaction/upload-object?bucket_name=nonexistent&object_key=test.txt",
            files={"file": ("test.txt", b"test content")}
        )
        
        assert response.status_code == 200


class TestSaveJson:
    """Test cases for save_json endpoint"""

    @patch("routers.bucket.get_r2_client")
    def test_save_json_success_first_file(self, mock_get_r2_client, client):
        """Test successful JSON save (first file)"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {}
        
        test_data = {"key": "value", "number": 123}
        
        response = client.post(
            "/Transaction/save?bucket_name=test-bucket&folder_path=json_store/",
            json=test_data
        )
        
        assert response.status_code == 200
        assert response.json()["id"] == 1
        assert response.json()["data"] == test_data

    @patch("routers.bucket.get_r2_client")
    def test_save_json_success_multiple_files(self, mock_get_r2_client, client):
        """Test successful JSON save (when files already exist)"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "json_store/1.json"},
                {"Key": "json_store/2.json"},
                {"Key": "json_store/3.json"}
            ]
        }
        
        test_data = {"key": "value"}
        
        response = client.post(
            "/Transaction/save?bucket_name=test-bucket&folder_path=json_store/",
            json=test_data
        )
        
        assert response.status_code == 200
        assert response.json()["id"] == 4

    @patch("routers.bucket.get_r2_client")
    def test_save_json_client_error(self, mock_get_r2_client, client):
        """Test JSON save with ClientError"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {}
        
        error_response = {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access Denied"
            }
        }
        mock_r2_client.put_object.side_effect = ClientError(error_response, "PutObject")
        
        test_data = {"key": "value"}
        
        response = client.post(
            "/Transaction/save?bucket_name=test-bucket",
            json=test_data
        )
        
        assert response.status_code == 400


class TestDeleteFolder:
    """Test cases for delete_folder endpoint"""

    @patch("routers.bucket.get_r2_client")
    def test_delete_folder_success(self, mock_get_r2_client, client):
        """Test successful folder deletion"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "folder/file1.txt"},
                {"Key": "folder/file2.txt"}
            ]
        }
        
        response = client.delete(
            "/Transaction/delete-folder?bucket_name=test-bucket&folder_path=folder/"
        )
        
        assert response.status_code == 200
        assert "2" in response.json()["message"]

    @patch("routers.bucket.get_r2_client")
    def test_delete_folder_empty(self, mock_get_r2_client, client):
        """Test deletion when folder is empty"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {}
        
        response = client.delete(
            "/Transaction/delete-folder?bucket_name=test-bucket&folder_path=empty/"
        )
        
        assert response.status_code == 200
        assert "No objects found" in response.json()["message"]

    @patch("routers.bucket.get_r2_client")
    def test_delete_folder_client_error(self, mock_get_r2_client, client):
        """Test folder deletion with ClientError"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist"
            }
        }
        mock_r2_client.list_objects_v2.side_effect = ClientError(error_response, "ListObjectsV2")
        
        response = client.delete(
            "/Transaction/delete-folder?bucket_name=nonexistent&folder_path=folder/"
        )
        
        assert response.status_code == 400


class TestDeleteBucket:
    """Test cases for delete_bucket endpoint"""

    @patch("routers.bucket.get_r2_client")
    def test_delete_bucket_success(self, mock_get_r2_client, client):
        """Test successful empty bucket deletion"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {}
        
        response = client.delete("/Transaction/delete?bucket_name=test-bucket")
        
        assert response.status_code == 200
        assert "deleted successfully" in response.json()["message"]

    @patch("routers.bucket.get_r2_client")
    def test_delete_bucket_with_force(self, mock_get_r2_client, client):
        """Test force deletion of bucket with objects"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        mock_r2_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.txt"},
                {"Key": "file2.txt"}
            ]
        }
        
        response = client.delete("/Transaction/delete?bucket_name=test-bucket&force=true")
        
        assert response.status_code == 200
        assert "deleted successfully" in response.json()["message"]
        mock_r2_client.delete_objects.assert_called_once()

    @patch("routers.bucket.get_r2_client")
    def test_delete_bucket_client_error(self, mock_get_r2_client, client):
        """Test bucket deletion with ClientError"""
        mock_r2_client = MagicMock()
        mock_get_r2_client.return_value = mock_r2_client
        
        error_response = {
            "Error": {
                "Code": "BucketNotEmpty",
                "Message": "The bucket is not empty"
            }
        }
        mock_r2_client.list_objects_v2.return_value = {}
        mock_r2_client.delete_bucket.side_effect = ClientError(error_response, "DeleteBucket")
        
        response = client.delete("/Transaction/delete?bucket_name=test-bucket")
        
        assert response.status_code == 400
