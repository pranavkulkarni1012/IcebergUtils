# Iceberg Partition Management Utility (AWS Glue)

## Overview

This project provides an AWS Glue job script for managing partitions within Apache Iceberg tables stored in AWS S3 and cataloged using the AWS Glue Data Catalog. The primary function is to replace data within specified partitions efficiently using Spark.

## Components

*   **`Icberg_Utils/src/utils.py`**:
    *   An AWS Glue Spark job script designed to replace data for specified partitions in an Iceberg table.
    *   It leverages Spark SQL and the Iceberg Spark extensions integrated with AWS Glue.
    *   The script first deletes the existing data matching the partition criteria and then uses the `add_files` procedure to register Parquet files from a corresponding source S3 path for that partition.
    *   Suitable for processing large partitions (e.g., 50GB+) due to leveraging the distributed processing power of Spark within AWS Glue.
*   **`Icberg_Utils/tests/`**:
    *   Contains unit tests for the `utils.py` Glue script using `pytest`.
    *   Uses mocking extensively (`unittest.mock`) to simulate the AWS Glue and Spark environments (`awsglue`, `pyspark`), allowing tests to run locally without needing actual AWS resources or a Spark cluster.
*   **`requirements-test.txt`**: Lists Python dependencies required specifically for running the *unit tests* locally. Note that dependencies for the *Glue job itself* (like `pyspark`, `awsglue`) are provided by the AWS Glue environment.

## Prerequisites

*   AWS Account
*   S3 Bucket:
    *   For storing the source Parquet data (partitioned structure matching the target table).
    *   For the Iceberg table warehouse (where Iceberg metadata and data files reside).
*   AWS Glue Data Catalog configured to catalog your target Iceberg table.
*   An AWS Glue Job Role (IAM Role) with permissions for:
    *   Reading from/writing to the source S3 path.
    *   Reading from/writing to the Iceberg warehouse S3 path.
    *   Interacting with the Glue Data Catalog (`glue:GetTable`, `glue:UpdateTable`, `glue:GetPartitions`, etc.).
    *   Executing Glue jobs.
*   Python environment (e.g., Python 3.8+) for running the *unit tests* locally.

## Setup and Execution

### 1. AWS Glue Job Deployment and Execution

1.  **Upload Script:** Upload `Icberg_Utils/src/utils.py` to an S3 bucket accessible by AWS Glue.
2.  **Create/Configure Glue Job:**
    *   Navigate to AWS Glue in the AWS Console.
    *   Create a new Spark job (e.g., "Spark" type, not "Python Shell").
    *   Configure the job settings:
        *   **Script location:** Point to the `utils.py` script you uploaded to S3.
        *   **IAM Role:** Select the Glue Job Role created with the necessary permissions.
        *   **Glue version:** Choose a version that supports Apache Iceberg (e.g., Glue 3.0 or later).
        *   **Job parameters:** This is crucial for enabling Iceberg and passing arguments to your script. Add parameters like these (adjust `glue_catalog`, warehouse path, and region as needed):
            *   `--conf`: `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
            *   `--conf`: `spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog`
            *   `--conf`: `spark.sql.catalog.glue_catalog.warehouse=s3://<your-iceberg-warehouse-bucket>/path/`
            *   `--conf`: `spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`
            *   `--conf`: `spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO`
            *   `--conf`: `spark.serializer=org.apache.spark.serializer.KryoSerializer` (Recommended for performance)
            *   `--conf`: `spark.sql.adaptive.enabled=true` (Recommended for performance)
            *   `--schema`: `<your_glue_database_name>` (Argument for your script)
            *   `--table`: `<your_glue_table_name>` (Argument for your script)
            *   `--partitions`: `<json_string_of_partitions>` (Argument for your script, e.g., `'{"year": "2024", "month": "03"}'`)
        *   **Worker type:** Choose an appropriate worker type (e.g., `G.1X` or `G.2X`).
        *   **Number of workers:** Allocate sufficient workers based on the data size (e.g., start with 5-10 workers for 50GB and monitor).
3.  **Run Job:** Start the configured Glue job. Monitor its progress, logs, and metrics in the AWS Glue console.

### 2. Running Unit Tests Locally

1.  **Clone Repository:** Get the project code.
2.  **Setup Environment:** Create and activate a Python virtual environment.
    ```bash
    python -m venv venv
    # On Linux/macOS:
    source venv/bin/activate
    # On Windows:
    # .\venv\Scripts\activate
    ```
3.  **Install Test Dependencies:**
    ```bash
    pip install -r requirements-test.txt
    ```
4.  **Run Tests:** Execute pytest from the workspace root directory (`/c%3A/learnpranav/POCs`):
    ```bash
    pytest Icberg_Utils/tests/ -v
    ```
5.  **Check Test Coverage:**
    ```bash
    # Generate a terminal report
    pytest --cov=Icberg_Utils.src Icberg_Utils/tests/ -v

    # Generate an interactive HTML report
    pytest --cov=Icberg_Utils.src --cov-report=html Icberg_Utils/tests/ -v
    # Then open the generated 'htmlcov/index.html' file in your browser.
    ```

## Dependencies for Job Execution

The core dependencies required to *run* the Glue job (`pyspark`, `awsglue` libraries) are provided by the AWS Glue runtime environment itself and do not need to be explicitly installed via pip for job execution. The dependencies listed in `requirements-test.txt` are only for running the local unit tests.