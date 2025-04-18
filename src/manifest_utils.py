from pyspark.sql import SparkSession
from typing import Optional

def get_latest_manifest_file(spark: SparkSession, iceberg_table_fqn: str) -> None:
    """
    Prints the latest manifest file for the specified Iceberg table.
    
    Args:
        spark: SparkSession
        iceberg_table_fqn: Fully qualified name of the Iceberg table (catalog.schema.table)
    """
    try:
        # Get the table metadata
        metadata_sql = f"""
        SELECT *
        FROM {iceberg_table_fqn}.metadata_log_entries
        ORDER BY timestamp DESC
        LIMIT 1
        """
        latest_metadata = spark.sql(metadata_sql).collect()[0]
        
        # Get the manifest list location
        manifest_list_sql = f"""
        SELECT *
        FROM {iceberg_table_fqn}.manifests
        WHERE path = '{latest_metadata.manifest_list}'
        """
        manifest_list = spark.sql(manifest_list_sql).collect()[0]
        
        print("\nLatest Manifest File Information:")
        print(f"Manifest List Location: {latest_metadata.manifest_list}")
        print(f"Manifest List Content: {manifest_list.content}")
        print(f"Number of Manifests: {manifest_list.added_files_count + manifest_list.deleted_files_count}")
        print(f"Added Files: {manifest_list.added_files_count}")
        print(f"Deleted Files: {manifest_list.deleted_files_count}")
        
        # Get detailed information about the manifest files
        manifest_files_sql = f"""
        SELECT *
        FROM {iceberg_table_fqn}.files
        WHERE manifest_path = '{latest_metadata.manifest_list}'
        """
        manifest_files = spark.sql(manifest_files_sql)
        
        print("\nFiles in Latest Manifest:")
        manifest_files.show(truncate=False)
        
    except Exception as e:
        print(f"Error getting latest manifest file: {e}")
        raise

def get_manifest_history(spark: SparkSession, iceberg_table_fqn: str, limit: Optional[int] = 10) -> None:
    """
    Prints the manifest history for the specified Iceberg table.
    
    Args:
        spark: SparkSession
        iceberg_table_fqn: Fully qualified name of the Iceberg table (catalog.schema.table)
        limit: Number of recent manifest entries to show (default: 10)
    """
    try:
        history_sql = f"""
        SELECT *
        FROM {iceberg_table_fqn}.metadata_log_entries
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        history = spark.sql(history_sql)
        
        print(f"\nLast {limit} Manifest History:")
        history.show(truncate=False)
        
    except Exception as e:
        print(f"Error getting manifest history: {e}")
        raise

def get_manifest_file_details(spark: SparkSession, iceberg_table_fqn: str, manifest_path: str) -> None:
    """
    Prints detailed information about a specific manifest file.
    
    Args:
        spark: SparkSession
        iceberg_table_fqn: Fully qualified name of the Iceberg table (catalog.schema.table)
        manifest_path: Path to the specific manifest file
    """
    try:
        # Get manifest details
        manifest_sql = f"""
        SELECT *
        FROM {iceberg_table_fqn}.manifests
        WHERE path = '{manifest_path}'
        """
        manifest = spark.sql(manifest_sql).collect()[0]
        
        print("\nManifest File Details:")
        print(f"Path: {manifest.path}")
        print(f"Length: {manifest.length}")
        print(f"Partition Spec ID: {manifest.partition_spec_id}")
        print(f"Added Files: {manifest.added_files_count}")
        print(f"Deleted Files: {manifest.deleted_files_count}")
        print(f"Content: {manifest.content}")
        
        # Get files in this manifest
        files_sql = f"""
        SELECT *
        FROM {iceberg_table_fqn}.files
        WHERE manifest_path = '{manifest_path}'
        """
        files = spark.sql(files_sql)
        
        print("\nFiles in Manifest:")
        files.show(truncate=False)
        
    except Exception as e:
        print(f"Error getting manifest file details: {e}")
        raise 