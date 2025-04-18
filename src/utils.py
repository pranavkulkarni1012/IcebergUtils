# AWS Glue Job Script using add_files with partition_filter on a source view

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import traceback


def parse_arguments(argv):
    """Parses Glue job arguments."""
    args = getResolvedOptions(argv, ['JOB_NAME', 'schema', 'table', 'partitions'])
    schema_name = args['schema']
    table_name = args['table']
    partitions_str = args['partitions'].replace("'", "\"")
    try:
        partitions_dict = json.loads(partitions_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing partitions JSON string: {partitions_str}")
        raise ValueError(f"Invalid partitions argument format. Must be JSON string. Error: {e}")
    print(f"Received parameters: schema='{schema_name}', table='{table_name}', partitions={partitions_dict}")
    return args['JOB_NAME'], schema_name, table_name, partitions_dict, args


def initialize_spark_and_glue(job_name, job_args, iceberg_warehouse_path, glue_catalog_name):
    """Initializes Spark, GlueContext, Job and configures for Iceberg."""
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, job_args)

    # Configure Spark for Iceberg
    conf = {
        f"spark.sql.catalog.{glue_catalog_name}.warehouse": iceberg_warehouse_path,
        f"spark.sql.catalog.{glue_catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{glue_catalog_name}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        f"spark.sql.catalog.{glue_catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    }
    for key, value in conf.items():
        spark.conf.set(key, value)
    print("Spark Iceberg configurations set.")
    return spark, sc, glueContext, job


def build_paths_and_clauses(schema_name, table_name, partitions_dict, s3_bucket, glue_catalog_name):
    """Builds table FQN, S3 paths, SQL clauses, and partition filter map string."""
    iceberg_table_fqn = f"{glue_catalog_name}.{schema_name}.{table_name}"
    base_s3_path = f"s3://{s3_bucket}/{schema_name.lower()}/{table_name.lower()}/" # Ensure trailing slash for base path

    # Sort partition keys for consistency
    sorted_partition_keys = sorted(partitions_dict.keys())

    # Construct WHERE clause for DELETE and PARTITION spec for ANALYZE
    delete_conditions = []
    analyze_partition_spec = []
    partition_filter_items = []
    for k in sorted_partition_keys:
        v = partitions_dict[k]
        # Format values correctly for SQL clauses and map string
        if isinstance(v, str):
            sql_escaped_v = str(v).replace("'", "''")
            delete_conditions.append(f"`{k}` = '{sql_escaped_v}'")
            analyze_partition_spec.append(f"`{k}` = '{sql_escaped_v}'")
            # For map string, ensure quotes around both key and value if needed
            partition_filter_items.append(f"'{k}', '{sql_escaped_v}'")
        else:
            # Assume numeric/boolean, adjust if other types need different formatting
            delete_conditions.append(f"`{k}` = {v}")
            analyze_partition_spec.append(f"`{k}` = {v}")
            partition_filter_items.append(f"'{k}', '{str(v)}'") # Map values are often strings

    delete_where_clause = " AND ".join(delete_conditions)
    analyze_partition_clause = ", ".join(analyze_partition_spec)
    # Construct the map string for partition_filter argument
    partition_filter_map_str = f"map({', '.join(partition_filter_items)})"

    print(f"Target Iceberg Table: {iceberg_table_fqn}")
    print(f"Base S3 Path: {base_s3_path}")
    print(f"DELETE WHERE clause: {delete_where_clause}")
    print(f"ANALYZE PARTITION clause: {analyze_partition_clause}")
    print(f"Partition Filter Map String: {partition_filter_map_str}")

    return iceberg_table_fqn, base_s3_path, delete_where_clause, analyze_partition_clause, partition_filter_map_str, sorted_partition_keys


def delete_iceberg_partition(spark, iceberg_table_fqn, delete_where_clause):
    """Deletes data for the specified partition from the Iceberg table."""
    delete_sql = f"DELETE FROM {iceberg_table_fqn} WHERE {delete_where_clause}"
    print(f"Executing DELETE: {delete_sql}")
    spark.sql(delete_sql)
    print("DELETE completed.")


def create_source_view(spark, view_name, base_s3_path, schema_sql, partition_columns):
    """Creates a temporary view on the base S3 path with partitioning."""
    partition_spec = ", ".join([f"`{col}`" for col in partition_columns])
    create_view_sql = f"""
        CREATE OR REPLACE TEMP VIEW `{view_name}` ({schema_sql})
        USING parquet
        OPTIONS (
          path "{base_s3_path}"
        )
        PARTITIONED BY ({partition_spec})
    """
    # Note: Using CREATE TEMP VIEW on path with PARTITIONED BY might require specific Spark/env setup.
    # Alternatively, one could register it differently if needed.
    # This syntax aims to treat the S3 path like an external table source.
    print(f"Creating source temporary view: \n{create_view_sql}")
    try:
        spark.sql(create_view_sql)
        print(f"Successfully created temporary view '{view_name}'")
    except Exception as e:
        print(f"Error creating temporary view '{view_name}' pointing to '{base_s3_path}'.")
        print("Ensure the path exists and Spark has permissions.")
        print(f"Error details: {e}")
        # If view creation fails, subsequent add_files will also fail.
        raise  # Re-raise the exception


def add_files_from_source_view(spark, glue_catalog_name, iceberg_table_fqn, source_view_name, partition_filter_map_str):
    """Calls system.add_files using the source view and partition_filter."""
    # Note: source_view_name should NOT be quoted with backticks inside the SQL string here
    # partition_filter_map_str already contains the map(...) syntax
    add_files_sql = f"""
    CALL {glue_catalog_name}.system.add_files(
        table => '{iceberg_table_fqn}',
        source_table => '{source_view_name}',
        partition_filter => {partition_filter_map_str}
    )
    """
    print(f"Executing add_files with partition_filter: \n{add_files_sql}")
    spark.sql(add_files_sql)
    print("add_files with partition_filter completed successfully.")


def analyze_iceberg_partition(spark, iceberg_table_fqn, analyze_partition_clause):
    """Runs ANALYZE TABLE for the specified partition."""
    analyze_sql = f"""
    ANALYZE TABLE {iceberg_table_fqn}
    PARTITION ({analyze_partition_clause})
    COMPUTE STATISTICS FOR ALL COLUMNS
    """
    print(f"Executing ANALYZE TABLE: \n{analyze_sql}")
    spark.sql(analyze_sql)
    print("ANALYZE TABLE completed successfully.")


def cleanup(spark, view_name):
    """Drops the temporary Spark view if it exists."""
    if view_name:
        try:
            print(f"Dropping temporary view: {view_name}")
            spark.catalog.dropTempView(view_name)
            print(f"Successfully dropped temporary view: {view_name}")
        except Exception as cleanup_e:
            print(f"WARN: Failed to drop temporary view {view_name}: {cleanup_e}")


# --- Main Execution ---
if __name__ == "__main__":
    source_view_name = None # Ensure initialized for finally block
    spark = None
    job = None

    try:
        # --- Configuration ---
        S3_BUCKET = "pranav_bucket" # CHANGE IF NEEDED
        ICEBERG_WAREHOUSE_PATH = f"s3://{S3_BUCKET}/iceberg_warehouse" # CHANGE IF NEEDED
        GLUE_CATALOG_NAME = "glue_catalog" # CHANGE IF NEEDED

        # 1. Parse Arguments
        job_name, schema_name, table_name, partitions_dict, job_args = parse_arguments(sys.argv)

        # 2. Initialize Spark & Glue
        spark, sc, glueContext, job = initialize_spark_and_glue(job_name, job_args, ICEBERG_WAREHOUSE_PATH, GLUE_CATALOG_NAME)

        # 3. Build Paths and Clauses
        iceberg_table_fqn, base_s3_path, delete_where_clause, analyze_partition_clause, partition_filter_map_str, partition_columns = \
            build_paths_and_clauses(schema_name, table_name, partitions_dict, S3_BUCKET, GLUE_CATALOG_NAME)

        # 4. Delete existing Iceberg partition data
        delete_iceberg_partition(spark, iceberg_table_fqn, delete_where_clause)

        # 5. Get schema from target Iceberg table for the source view
        try:
            target_schema = spark.table(iceberg_table_fqn).schema
            schema_sql = target_schema.toDDL() # Get DDL representation like "col1 INT, col2 STRING ..."
             # Remove partition columns from the schema definition for the view if they exist in target table schema
            non_partition_cols_schema = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in target_schema.fields if f.name not in partition_columns)
            print(f"Using schema from target table for source view: {non_partition_cols_schema}")
        except Exception as schema_e:
            print(f"ERROR: Could not read schema from target Iceberg table '{iceberg_table_fqn}'.")
            print("Ensure the table exists and has the correct schema before running this job.")
            print(f"Error details: {schema_e}")
            raise schema_e

        # 6. Create Temporary Source View on base S3 path
        # Generate a unique view name
        partition_suffix = "_".join(f"{k}_{v}" for k, v in sorted(partitions_dict.items())).replace('-', '_').replace('.', '_')
        source_view_name = f"source_view_{table_name}_{partition_suffix}"
        source_view_name = ''.join(c if c.isalnum() else '_' for c in source_view_name)[:100] # Sanitize

        create_source_view(spark, source_view_name, base_s3_path, non_partition_cols_schema, partition_columns)

        # 7. Add files using the source view and partition_filter
        add_files_from_source_view(spark, GLUE_CATALOG_NAME, iceberg_table_fqn, source_view_name, partition_filter_map_str)

        # 8. Analyze the newly added partition
        analyze_iceberg_partition(spark, iceberg_table_fqn, analyze_partition_clause)

    except Exception as e:
        print(f"\n--- JOB FAILED ---")
        print(f"An error occurred: {e}")
        traceback.print_exc()
        # Re-raise to ensure Glue marks the job as failed
        raise e
    finally:
        # 9. Cleanup the temporary source view
        if spark:
            cleanup(spark, source_view_name) # Pass the correct view name
        # 10. Commit Job status
        if job:
             job.commit()
             print("\n--- Glue job finished. ---")
        else:
             # Handle case where job object might not be initialized if init failed
             print("\n--- Glue job potentially finished (Job object not initialized). ---")
