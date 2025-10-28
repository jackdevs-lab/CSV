import pytest
from unittest.mock import Mock, patch
from src.qb_client import QuickBooksClient

class TestQuickBooksClient:
    
    @pytest.fixture
    def mock_auth(self):
        auth = Mock()
        auth.get_access_token.return_value = 'test_token'
        return auth
    
    @pytest.fixture
    def qb_client(self, mock_auth):
        client = QuickBooksClient(mock_auth)
        client.realm_id = 'test_realm'
        return client
    
    def test_headers(self, qb_client):
        """Test header generation"""
        headers = qb_client._get_headers()
        assert headers['Authorization'] == 'Bearer test_token'
        assert headers['Accept'] == 'application/json'
    
    @patch('src.qb_client.requests.request')
    def test_create_customer(self, mock_request, qb_client):
        """Test customer creation"""
        mock_response = Mock()
        mock_response.json.return_value = {'Customer': {'Id': '123'}}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        customer_data = {'DisplayName': 'Test Customer'}
        response = qb_client.create_customer(customer_data)
        
        assert response['Customer']['Id'] == '123'
        mock_request.assert_called_once()