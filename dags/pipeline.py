from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_lake_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données',
    schedule=timedelta(days=1),
)

# Tâche 1: Extraction vers raw
extract_task = BashOperator(
    task_id='unpack_to_raw',
    bash_command='python /opt/airflow/build/unpack_to_raw.py --output-dir /opt/airflow/data/raw --endpoint-url http://localstack:4566',
    dag=dag,
)

# Tâche 2: Transformation vers MySQL
transform_task = BashOperator(
    task_id='preprocess_to_staging',
    bash_command='python /opt/airflow/scripts/preprocess_to_staging.py --bucket_raw raw --db_host mysql --db_user root --db_password root --endpoint-url http://localstack:4566',
    dag=dag,
)

# Tâche 3: Chargement vers MongoDB
load_task = BashOperator(
    task_id='process_to_curated',
    bash_command='python /opt/airflow/scripts/process_to_curated.py --mysql_host mysql --mysql_user root --mysql_password root --mongo_uri mongodb://mongodb:27017/',
    dag=dag,
)

# Définir l'ordre des tâches
extract_task >> transform_task >> load_task