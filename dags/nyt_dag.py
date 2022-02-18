from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from scripts.best_sellers_ETL import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'nyt_dag',
    default_args=default_args,
    description='NYT Best-selling books ETL pipeline',
    schedule_interval="@weekly",
)

extract_data = PythonOperator(
    task_id='ETL_process_extract',
    python_callable=extract,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='ETL_process_transform',
    python_callable=transform,
    dag=dag,
)

load_data = PythonOperator(
    task_id='ETL_process_load',
    python_callable=load,
    dag=dag,
)

verify_data = PythonOperator(
    task_id='ETL_process_verify',
    python_callable=verify,
    dag=dag,
)

extract_data>>transform_data>>load_data>>verify_data