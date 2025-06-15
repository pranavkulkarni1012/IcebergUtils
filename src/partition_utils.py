import boto3
from datetime import datetime
from typing import List, Dict, Set
import logging
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_partition_path(s3_path: str) -> Dict[str, str]:
    """
    Parse an S3 path to extract partition values.
    
    Args:
        s3_path: Full S3 path (e.g., s3://bucket/prefix/partition1=value1/partition2=value2/file.parquet)
        
    Returns:
        Dictionary containing partition column names and their values
    """
    try:
        # Remove the file name from the path
        path_parts = s3_path.split('/')
        file_name = path_parts[-1]
        path_without_file = '/'.join(path_parts[:-1])
        
        # Extract partition values
        partitions = {}
        for part in path_without_file.split('/'):
            if '=' in part:
                key, value = part.split('=')
                partitions[key] = value
                
        return partitions
    except Exception as e:
        logger.error(f"Error parsing partition path {s3_path}: {str(e)}")
        raise

def get_partition_values(s3_path: str, partition_column: str) -> Set[str]:
    """
    Get all unique values for a specific partition column from an S3 location.
    
    Args:
        s3_path: Base S3 path (e.g., s3://bucket/prefix)
        partition_column: Name of the partition column to get values for
        
    Returns:
        Set of unique partition values
    """
    try:
        # Parse S3 path
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get all objects with pagination
        paginator = s3_client.get_paginator('list_objects_v2')
        partition_values = set()
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    path = prefix['Prefix']
                    if f"{partition_column}=" in path:
                        # Extract the partition value
                        value = path.split(f"{partition_column}=")[1].split('/')[0]
                        partition_values.add(value)
        
        return partition_values
        
    except Exception as e:
        logger.error(f"Error getting partition values: {str(e)}")
        raise

def get_partition_paths_by_date(s3_path: str, date: str) -> List[str]:
    """
    Get all partition paths for a specific date.
    
    Args:
        s3_path: Base S3 path (e.g., s3://bucket/prefix)
        date: Date in YYYY-MM-DD format
        
    Returns:
        List of partition paths for the specified date
    """
    try:
        # Parse S3 path
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Construct the prefix for the specific date
        date_prefix = f"{prefix}business_system_date={date}/"
        
        # Get all objects with pagination
        paginator = s3_client.get_paginator('list_objects_v2')
        partition_paths = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix=date_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    partition_paths.append(f"s3://{bucket}/{obj['Key']}")
        
        return partition_paths
        
    except Exception as e:
        logger.error(f"Error getting partition paths for date {date}: {str(e)}")
        raise

def get_lowest_level_prefixes(s3_path: str) -> List[str]:
    """
    Get only the lowest level partition prefixes from an S3 location.
    These are the prefixes that contain the actual data files.
    Works for both single-level and multi-level partitions.
    
    Args:
        s3_path: Base S3 path (e.g., s3://bucket/prefix)
        
    Returns:
        List of lowest level partition prefixes
    """
    try:
        # Parse S3 path
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get all objects with pagination
        paginator = s3_client.get_paginator('list_objects_v2')
        partition_prefixes = set()
        
        # First, get all partition prefixes
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    prefix_path = prefix['Prefix']
                    
                    # Check if this prefix contains any files
                    has_files = False
                    for file_page in paginator.paginate(Bucket=bucket, Prefix=prefix_path):
                        if 'Contents' in file_page:
                            has_files = True
                            break
                    
                    if has_files:
                        # This is a lowest level prefix with files
                        partition_prefixes.add(f"s3://{bucket}/{prefix_path}")
                    else:
                        # This is an intermediate prefix, check its sub-prefixes
                        for sub_page in paginator.paginate(Bucket=bucket, Prefix=prefix_path, Delimiter='/'):
                            if 'CommonPrefixes' in sub_page:
                                for sub_prefix in sub_page['CommonPrefixes']:
                                    # Check if sub-prefix contains files
                                    has_sub_files = False
                                    for sub_file_page in paginator.paginate(Bucket=bucket, Prefix=sub_prefix['Prefix']):
                                        if 'Contents' in sub_file_page:
                                            has_sub_files = True
                                            break
                                    
                                    if has_sub_files:
                                        partition_prefixes.add(f"s3://{bucket}/{sub_prefix['Prefix']}")
        
        return sorted(list(partition_prefixes))
        
    except Exception as e:
        logger.error(f"Error getting partition prefixes: {str(e)}")
        raise

def get_partition_paths_with_prefix(base_path: str, source_prefix: str, target_prefix: str) -> List[str]:
    """
    Get partition paths and replace source prefix with target prefix.
    
    Args:
        base_path: Base S3 path (e.g., s3://data_bucket_name)
        source_prefix: Source prefix to replace (e.g., transactions/)
        target_prefix: Target prefix to use (e.g., backup/transactions/)
        
    Returns:
        List of partition paths with target prefix
    """
    try:
        # Parse S3 path
        parsed_url = urlparse(base_path)
        bucket = parsed_url.netloc
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get all objects with pagination
        paginator = s3_client.get_paginator('list_objects_v2')
        partition_paths = set()
        
        # Construct the full source path
        full_source_path = f"{base_path}/{source_prefix}"
        
        # Get all partition prefixes
        for page in paginator.paginate(Bucket=bucket, Prefix=source_prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    prefix_path = prefix['Prefix']
                    if '=' in prefix_path:  # This is a partition
                        # Replace source prefix with target prefix
                        new_path = prefix_path.replace(source_prefix, target_prefix, 1)
                        partition_paths.add(f"s3://{bucket}/{new_path}")
                        
                        # Check for nested partitions
                        for sub_page in paginator.paginate(Bucket=bucket, Prefix=prefix_path, Delimiter='/'):
                            if 'CommonPrefixes' in sub_page:
                                for sub_prefix in sub_page['CommonPrefixes']:
                                    if '=' in sub_prefix['Prefix']:
                                        # Replace source prefix with target prefix for nested partition
                                        new_sub_path = sub_prefix['Prefix'].replace(source_prefix, target_prefix, 1)
                                        partition_paths.add(f"s3://{bucket}/{new_sub_path}")
        
        return sorted(list(partition_paths))
        
    except Exception as e:
        logger.error(f"Error getting partition paths: {str(e)}")
        raise

def main():
    # Example usage
    base_path = "s3://data_bucket_name"
    source_prefix = "transactions/"
    target_prefix = "backup/transactions/"
    
    try:
        # Get partition paths with target prefix
        prefixes = get_partition_paths_with_prefix(base_path, source_prefix, target_prefix)
        logger.info(f"Found {len(prefixes)} partition paths:")
        for prefix in prefixes:
            logger.info(f"Prefix: {prefix}")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main() 