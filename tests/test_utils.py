import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
import json

# --- Mock awsglue before importing utils ---
# Create a comprehensive mock for awsglue and its components
mock_awsglue = MagicMock()
mock_awsglue.transforms = MagicMock()
mock_awsglue.utils = MagicMock()
mock_awsglue.context = MagicMock()
mock_awsglue.job = MagicMock()
mock_awsglue.utils.getResolvedOptions = MagicMock(return_value={}) # Default return
mock_awsglue.context.GlueContext = MagicMock()
mock_awsglue.job.Job = MagicMock()

# Use patch.dict to insert the mock into sys.modules
# This ensures that when 'Icberg_Utils.src.utils' is imported below,
# it finds our mock 'awsglue' module.
with patch.dict(sys.modules, {
    'awsglue': mock_awsglue,
    'awsglue.transforms': mock_awsglue.transforms,
    'awsglue.utils': mock_awsglue.utils,
    'awsglue.context': mock_awsglue.context,
    'awsglue.job': mock_awsglue.job,
}):
    # Now import the module under test *after* awsglue is mocked
    from Icberg_Utils.src.utils import (
        parse_arguments,
        initialize_spark_and_glue,
        build_paths_and_clauses,
        delete_iceberg_partition,
        create_source_view,
        add_files_from_source_view,
        analyze_iceberg_partition,
        cleanup
    )
    # Also import the mocked components directly if needed by tests
    # (These will resolve to the mocks we inserted)
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
# --- End Mocking ---

class TestIcebergUtils:
    @pytest.fixture
    def mock_spark(self):
        """Fixture providing a mock Spark session"""
        spark = MagicMock()
        spark.conf = MagicMock()
        spark.sql = MagicMock()
        spark.catalog = MagicMock()
        # Add schema mock if needed by view creation tests
        mock_schema = MagicMock()
        mock_schema.toDDL.return_value = "col1 INT, col2 STRING"
        mock_schema.fields = [MagicMock(name='col1', dataType=MagicMock(simpleString=lambda: 'INT')),
                              MagicMock(name='col2', dataType=MagicMock(simpleString=lambda: 'STRING'))]
        spark.table.return_value.schema = mock_schema
        return spark

    @pytest.fixture
    def sample_partitions(self):
        """Fixture providing sample partition data"""
        return {
            "year": "2024",
            "month": "03",
            "day": "15"
        }

    @pytest.fixture
    def mock_glue_context(self):
        """Fixture providing a mock GlueContext"""
        glue_context = MagicMock()
        glue_context.spark_session = MagicMock()
        return glue_context

    @pytest.fixture
    def test_config(self):
        """Fixture providing common test configuration"""
        return {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            's3_bucket': 'test-bucket',
            'glue_catalog_name': 'test_catalog',
            'iceberg_warehouse_path': 's3://test-bucket/iceberg_warehouse'
        }

    def test_parse_arguments_valid_input(self):
        """Test argument parsing with valid input"""
        test_argv = [
            'script_name',
            '--JOB_NAME', 'test_job',
            '--schema', 'test_schema',
            '--table', 'test_table',
            '--partitions', '{"year": "2024", "month": "03"}'
        ]
        
        # Configure the mock that was set up in conftest.py
        mock_args = {
            'JOB_NAME': 'test_job',
            'schema': 'test_schema',
            'table': 'test_table',
            'partitions': '{"year": "2024", "month": "03"}'
        }
        getResolvedOptions.return_value = mock_args
        
        job_name, schema, table, partitions, args = parse_arguments(test_argv)
        
        assert job_name == 'test_job'
        assert schema == 'test_schema'
        assert table == 'test_table'
        assert partitions == {"year": "2024", "month": "03"}

    def test_parse_arguments_invalid_json(self):
        """Test argument parsing with invalid JSON input"""
        test_argv = [
            'script_name',
            '--JOB_NAME', 'test_job',
            '--schema', 'test_schema',
            '--table', 'test_table',
            '--partitions', 'invalid_json'
        ]
        
        # Configure the mock
        mock_args = {
            'JOB_NAME': 'test_job',
            'schema': 'test_schema',
            'table': 'test_table',
            'partitions': 'invalid_json'
        }
        getResolvedOptions.return_value = mock_args
        
        with pytest.raises(ValueError) as exc_info:
            parse_arguments(test_argv)
        assert "Invalid partitions argument format" in str(exc_info.value)

    def test_build_paths_and_clauses(self, sample_partitions, test_config):
        """Test building paths and clauses with sample data"""
        result = build_paths_and_clauses(
            test_config['schema_name'],
            test_config['table_name'],
            sample_partitions,
            test_config['s3_bucket'],
            test_config['glue_catalog_name']
        )
        
        iceberg_table_fqn, base_s3_path, delete_where_clause, analyze_partition_clause, partition_filter_map_str, sorted_keys = result
        
        # Verify table FQN
        assert iceberg_table_fqn == f"{test_config['glue_catalog_name']}.{test_config['schema_name']}.{test_config['table_name']}"
        
        # Verify S3 path
        expected_s3_path = f"s3://{test_config['s3_bucket']}/{test_config['schema_name'].lower()}/{test_config['table_name'].lower()}/"
        assert base_s3_path == expected_s3_path
        
        # Verify WHERE clause components
        assert all(f"`{k}` = '{v}'" in delete_where_clause for k, v in sample_partitions.items())
        
        # Verify partition filter map string
        assert partition_filter_map_str.startswith("map(")
        assert partition_filter_map_str.endswith(")")
        for k, v in sample_partitions.items():
            assert f"'{k}', '{v}'" in partition_filter_map_str
        
        # Verify sorted keys
        assert sorted_keys == sorted(sample_partitions.keys())

    def test_delete_iceberg_partition(self, mock_spark):
        """Test deletion of Iceberg partition"""
        iceberg_table_fqn = "catalog.schema.table"
        delete_where_clause = "year = '2024' AND month = '03'"
        
        delete_iceberg_partition(mock_spark, iceberg_table_fqn, delete_where_clause)
        
        expected_sql = f"DELETE FROM {iceberg_table_fqn} WHERE {delete_where_clause}"
        mock_spark.sql.assert_called_once_with(expected_sql)

    def test_create_source_view_success(self, mock_spark):
        """Test successful creation of source view"""
        view_name = "test_view"
        base_s3_path = "s3://bucket/path"
        schema_sql = "col1 STRING, col2 INT"
        partition_columns = ["year", "month"]
        
        create_source_view(mock_spark, view_name, base_s3_path, schema_sql, partition_columns)
        
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        
        # Verify SQL components
        assert "CREATE OR REPLACE TEMP VIEW" in sql_call
        assert view_name in sql_call
        assert base_s3_path in sql_call
        assert "PARTITIONED BY" in sql_call
        assert all(col in sql_call for col in partition_columns)

    def test_create_source_view_failure(self, mock_spark):
        """Test handling of source view creation failure"""
        mock_spark.sql.side_effect = Exception("Failed to create view")
        
        with pytest.raises(Exception) as exc_info:
            create_source_view(
                mock_spark,
                "test_view",
                "s3://bucket/path",
                "col1 STRING",
                ["year"]
            )
        assert "Failed to create view" in str(exc_info.value)

    def test_add_files_from_source_view(self, mock_spark, test_config):
        """Test adding files from source view"""
        source_view_name = "test_view"
        partition_filter_map_str = "map('year', '2024', 'month', '03')"
        iceberg_table_fqn = f"{test_config['glue_catalog_name']}.{test_config['schema_name']}.{test_config['table_name']}"
        
        add_files_from_source_view(
            mock_spark,
            test_config['glue_catalog_name'],
            iceberg_table_fqn,
            source_view_name,
            partition_filter_map_str
        )
        
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        
        assert f"CALL {test_config['glue_catalog_name']}.system.add_files" in sql_call
        assert f"table => '{iceberg_table_fqn}'" in sql_call
        assert f"source_table => '{source_view_name}'" in sql_call
        assert f"partition_filter => {partition_filter_map_str}" in sql_call

    def test_analyze_iceberg_partition(self, mock_spark):
        """Test analyzing Iceberg partition"""
        iceberg_table_fqn = "test_catalog.schema.table"
        analyze_partition_clause = "year = '2024', month = '03'"
        
        analyze_iceberg_partition(mock_spark, iceberg_table_fqn, analyze_partition_clause)
        
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        
        assert "ANALYZE TABLE" in sql_call
        assert iceberg_table_fqn in sql_call
        assert "PARTITION" in sql_call
        assert analyze_partition_clause in sql_call
        assert "COMPUTE STATISTICS FOR ALL COLUMNS" in sql_call

    def test_cleanup_success(self, mock_spark):
        """Test successful cleanup of temporary view"""
        view_name = "test_view"
        cleanup(mock_spark, view_name)
        mock_spark.catalog.dropTempView.assert_called_once_with(view_name)

    def test_cleanup_failure(self, mock_spark):
        """Test handling of cleanup failure"""
        view_name = "test_view"
        mock_spark.catalog.dropTempView.side_effect = Exception("Failed to drop view")
        
        # Should not raise exception
        cleanup(mock_spark, view_name)

    """ @patch('pyspark.context.SparkContext')
    def test_initialize_spark_and_glue(self, mock_spark_context, test_config):
        job_name = "test_job"
        job_args = {}
        
        # Create mock instances
        mock_spark = MagicMock()
        mock_glue_context = MagicMock()
        mock_job = MagicMock()
        
        # Configure the mocks
        mock_spark_context.return_value = MagicMock()
        mock_glue_context.spark_session = mock_spark
        GlueContext.return_value = mock_glue_context
        Job.return_value = mock_job
        
        # Call the function
        spark, sc, glue_context, job = initialize_spark_and_glue(
            job_name,
            job_args,
            test_config['iceberg_warehouse_path'],
            test_config['glue_catalog_name']
        )
        
        # Verify the results
        assert spark == mock_glue_context.spark_session
        assert sc == mock_spark_context.return_value
        assert glue_context == mock_glue_context
        assert job == mock_job
        
        # Verify Spark configurations were set
        expected_configs = [
            f"spark.sql.catalog.{test_config['glue_catalog_name']}.warehouse",
            f"spark.sql.catalog.{test_config['glue_catalog_name']}",
            f"spark.sql.catalog.{test_config['glue_catalog_name']}.catalog-impl",
            f"spark.sql.catalog.{test_config['glue_catalog_name']}.io-impl",
            "spark.sql.extensions"
        ]
        
        for config in expected_configs:
            assert any(call[0][0] == config for call in mock_spark.conf.set.call_args_list)

    @patch('pyspark.context.SparkContext')
    @patch('pyspark.context.GlueContext')
    @patch('pyspark.context.Job')
    def test_initialize_spark_and_glue_with_conftest_mocks(self, mock_job_class, mock_glue_context_class, 
                                                         mock_spark_context, test_config):
        
        job_name = "test_job"
        job_args = {}
        
        mock_spark_session_instance = MagicMock()
        mock_glue_context_instance = MagicMock()
        mock_glue_context_instance.spark_session = mock_spark_session_instance
        mock_glue_context_class.return_value = mock_glue_context_instance

        mock_job_instance = MagicMock()
        mock_job_class.return_value = mock_job_instance
        
        mock_spark_context_instance = MagicMock()
        mock_spark_context.return_value = mock_spark_context_instance

        spark, sc, glue_context, job = initialize_spark_and_glue(
            job_name,
            job_args,
            test_config['iceberg_warehouse_path'],
            test_config['glue_catalog_name']
        )

        mock_spark_context.assert_called_once()
        mock_glue_context_class.assert_called_once_with(mock_spark_context_instance)
        
        expected_configs = [
            f"spark.sql.catalog.{test_config['glue_catalog_name']}.warehouse",
            f"spark.sql.catalog.{test_config['glue_catalog_name']}",
            f"spark.sql.catalog.{test_config['glue_catalog_name']}.catalog-impl",
            f"spark.sql.catalog.{test_config['glue_catalog_name']}.io-impl",
            "spark.sql.extensions"
        ]
        
        assert mock_spark_session_instance.conf.set.call_count >= len(expected_configs)
        for config_key in expected_configs:
             assert any(call[0][0] == config_key for call in mock_spark_session_instance.conf.set.call_args_list)
        
        mock_job_class.assert_called_once_with(mock_glue_context_instance)
        mock_job_instance.init.assert_called_once_with(job_name, job_args)

        assert spark == mock_spark_session_instance
        assert sc == mock_spark_context_instance
        assert glue_context == mock_glue_context_instance
        assert job == mock_job_instance 
        """