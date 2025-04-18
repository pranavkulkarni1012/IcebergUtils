import pytest
from unittest.mock import MagicMock

# Keep only fixtures, remove the sys.modules lines

@pytest.fixture(scope="session")
def aws_credentials():
    """Fixture for mock AWS credentials"""
    return {
        'aws_access_key_id': 'test_access_key',
        'aws_secret_access_key': 'test_secret_key',
        'region_name': 'us-east-1'
    }

@pytest.fixture(scope="session")
def mock_boto3_client():
    """Fixture for mock boto3 client"""
    return MagicMock() 