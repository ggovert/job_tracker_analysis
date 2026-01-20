from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator # ADDED THIS
import sys
from datetime import datetime, timedelta
from airflow.models.dataset import Dataset

# Ensure scripts folder is accessible
sys.path.append('/opt/airflow/scripts')
from silver_to_gold import run_gold_logic # Import your DuckDB logic


# Define the dataset
HN_BRONZE_DATA = Dataset("rustfs://bronze/hackernews")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# This is the consumer dag
with DAG(
    dag_id="silver_layer_event_driven",
    schedule = [HN_BRONZE_DATA],
    default_args=default_args,
    start_date=datetime(2026, 1, 1), 
    catchup=False
) as dag:

    # 1. Set the trigger date
    # Instead of using ds that might cause mismatched data, we will use the trigger date
    trigger_date = "{{ triggering_dataset_events['rustfs://bronze/hackernews'][0].source_dag_run.logical_date.strftime('%Y-%m-%d') }}"


    # 2. THE SPARK JOB (Silver Layer)
    spark_config = {
        "spark.master": "local[*]",
        "spark.hadoop.fs.s3a.endpoint": "http://rustfs:9000",
        "spark.hadoop.fs.s3a.access.key": "rustfsadmin",
        "spark.hadoop.fs.s3a.secret.key": "rustfsadmin123",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.sources.partitionOverwriteMode": "dynamic", # Crucial for S3
        "spark.hadoop.fs.s3a.change.detection.mode": "none"    # Fixes the 400 error
    }

    # Process the silver layer using event driven approach
    process_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/airflow/scripts/bronze_to_silver.py",
        application_args = [
            trigger_date
        ],
        conn_id="spark_local", 
        conf=spark_config,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        verbose=True
    )

    # 3. THE DUCKDB JOB (Physical Gold Layer)
    generate_gold_metrics = PythonOperator(
        task_id="duckdb_silver_to_gold",
        python_callable=run_gold_logic,
        op_kwargs={'target_date': trigger_date}
    )

    # UPDATED FLOW
    process_silver >> generate_gold_metrics