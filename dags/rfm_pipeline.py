from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

doc_md_dag = """
### 🏭 Pipeline RFM Automatisé
Ce DAG orchestre le processus complet de segmentation client RFM en utilisant des variables dynamiques.

#### 📋 Étapes :
1. **Ingestion** : Lecture Excel, fusion, nettoyage -> `raw_online_retail`
2. **Transformation** : Calcul R, F, M -> `rfm_result`

#### ⚙️ Variables Airflow utilisées :
- `rfm_excel_path`
- `rfm_raw_table`
- `rfm_result_table`
"""

dag = DAG(
    'rfm_data_pipeline_pro',
    default_args=default_args,
    description='Pipeline ETL RFM Professionnel avec variables et retries',
    schedule_interval=None,
    catchup=False,
    tags=['projet_rfm', 'production', 'bac5'],
    doc_md=doc_md_dag,
)

# Récupération des variables Airflow
excel_path = Variable.get("rfm_excel_path")
raw_table = Variable.get("rfm_raw_table")
result_table = Variable.get("rfm_result_table")

# Définition des variables d'environnement à passer aux scripts
env_vars = {
    "RFM_EXCEL_PATH": excel_path,
    "RFM_RAW_TABLE": raw_table,
    "RFM_RESULT_TABLE": result_table,
    "DB_USER": "rfm_user",
    "DB_PASSWORD": "rfm_password",
    "DB_HOST": "postgres-business",
    "DB_PORT": "5432",
    "DB_NAME": "rfm_db"
}

# Tâche 1 : Ingestion
task_ingestion = BashOperator(
    task_id='ingestion_donnees_brutes',
    bash_command='python /opt/airflow/scripts/ingestion.py',
    env=env_vars, # Injection des variables
    dag=dag,
)

# Tâche 2 : Transformation
task_transformation = BashOperator(
    task_id='transformation_rfm',
    bash_command='python /opt/airflow/scripts/transformation.py',
    env=env_vars, # Injection des variables
    dag=dag,
)

task_ingestion >> task_transformation