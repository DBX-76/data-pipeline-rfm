from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from scripts.ingestion import load_data
from scripts.transformation import compute_rfm

with DAG(
    dag_id='rfm_pipeline',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=load_data,
    )

    transform = PythonOperator(
        task_id='compute_rfm',
        python_callable=compute_rfm,
    )

    ingest >> transform
