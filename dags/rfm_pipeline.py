from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

doc_md_dag = """
### 🏭 Pipeline RFM Automatisé (Config Docker)
Ce DAG utilise des variables définies dans `docker-compose.yml` (AIRFLOW_VAR_*).
Aucune configuration manuelle dans l'UI n'est requise.
"""

dag = DAG(
    'rfm_data_pipeline_pro',
    default_args=default_args,
    description='Pipeline ETL RFM avec variables injectées par Docker',
    schedule_interval=None,
    catchup=False,
    tags=['projet_rfm', 'production', 'bac5'],
    doc_md=doc_md_dag,
)

# Optionnel : Vérification que les variables sont bien chargées par Airflow
# (Elles le seront automatiquement grâce aux env AIRFLOW_VAR_...)
excel_path = Variable.get("rfm_excel_path")
raw_table = Variable.get("rfm_raw_table")
result_table = Variable.get("rfm_result_table")

# Les scripts Python liront ces variables via os.getenv('RFM_EXCEL_PATH') 
# CAR Airflow transforme AIRFLOW_VAR_RFM_EXCEL_PATH en RFM_EXCEL_PATH dans l'environnement de la tâche.

task_ingestion = BashOperator(
    task_id='ingestion_donnees_brutes',
    bash_command='python /opt/airflow/scripts/ingestion.py',
    dag=dag,
)

task_transformation = BashOperator(
    task_id='transformation_rfm',
    bash_command='python /opt/airflow/scripts/transformation.py',
    dag=dag,
)

task_ingestion >> task_transformation