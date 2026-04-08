from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuration par défaut pour tous les tasks du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1), # Date passée pour déclencher immédiatement
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'rfm_data_pipeline',
    default_args=default_args,
    description='Pipeline complet RFM : Ingestion -> Transformation',
    schedule_interval=None, # Déclenchement manuel (on clique sur le bouton Play)
    catchup=False,
    tags=['projet_rfm', 'etl'],
)

# Tâche 1 : Ingestion des données
# On utilise BashOperator pour lancer un script Python dans le conteneur
task_ingestion = BashOperator(
    task_id='ingestion_donnees_brutes',
    bash_command='python /opt/airflow/scripts/ingestion.py',
    dag=dag,
)

# Tâche 2 : Transformation RFM (À créer dans la foulée)
task_transformation = BashOperator(
    task_id='transformation_rfm',
    bash_command='python /opt/airflow/scripts/transformation.py',
    dag=dag,
)

# Définition de l'ordre : Ingestion DOIT finir avant Transformation
task_ingestion >> task_transformation