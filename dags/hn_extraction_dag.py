from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException

# 1. Tell airflow where to find the scripts folder to import the run_extraction function
sys.path.append('/opt/airflow/scripts')
from extract_bronze import run_extraction

# 2. Define the default behavior
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 3. Define the logical pointer to RustFS folder
HN_BRONZE_DATA = Dataset("rustfs://bronze/hackernews")

# 4. Define the function only post if there is new dataset available
def extract_wrapper(**kwargs):
    '''
    Function wrapper to extract data from HN and put it inside RustFS Bronze layer
    Making sure that the data is only extracted if there is new dataset available
    '''
    success = run_extraction(date = kwargs['ds'])
    
    if not success:
        raise AirflowSkipException("No Data found today. Stopping the pipeline")

    return "Data saved to RustFS Bronze layer"
    
    

# 5. Define the DAG (the pipeline)
with DAG(
    dag_id = 'hn_extraction_json_to_bronze',
    default_args=default_args,
    description='Extract HN comments from Ask HN: Who is hiring? thread and put it inside RustFS Bronze layer',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags = ['medallion', 'bronze']
) as dag:

    # 4. Define and create the task
    extract_hn_comments = PythonOperator(
        task_id='extract_hn_comments',
        python_callable= extract_wrapper,
        outlets = [HN_BRONZE_DATA]
    )

    extract_hn_comments
