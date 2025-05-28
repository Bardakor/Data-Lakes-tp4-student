from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_pipeline_start(**context):
    """Log pipeline start with execution date."""
    logger.info(f"=== Starting robust data lake pipeline at {context['execution_date']} ===")
    return "Pipeline started successfully"

def validate_prerequisites(**context):
    """Validate that required services and directories are available."""
    import os
    import boto3
    
    # Check if required directories exist
    required_dirs = ['/opt/airflow/data', '/opt/airflow/logs']
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            logger.error(f"Required directory not found: {dir_path}")
            raise FileNotFoundError(f"Directory {dir_path} does not exist")
    
    # Test S3 connection
    try:
        s3_client = boto3.client('s3', endpoint_url='http://localstack:4566')
        s3_client.list_buckets()
        logger.info("S3 connection validated")
    except Exception as e:
        logger.error(f"S3 connection failed: {e}")
        raise
    
    logger.info("All prerequisites validated successfully")
    return "Prerequisites check passed"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'robust_data_lake_pipeline',
    default_args=default_args,
    description='Enhanced ETL Pipeline with Data Validation and Error Handling',
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['data-lake', 'etl', 'robust'],
)

# Task to log pipeline start
log_start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=dag,
)

# Task to validate prerequisites
validate_task = PythonOperator(
    task_id='validate_prerequisites',
    python_callable=validate_prerequisites,
    dag=dag,
)

# Tâche 1: Extraction vers raw avec validation
extract_task = BashOperator(
    task_id='extract_and_validate',
    bash_command='''
    set -e
    echo "Starting data extraction with validation..."
    python /opt/airflow/build/unpack_to_raw.py \
        --output-dir /opt/airflow/data/raw \
        --endpoint-url http://localstack:4566
    echo "Extraction completed successfully"
    ''',
    dag=dag,
)

# Tâche 2: Transformation vers MySQL avec validation
transform_task = BashOperator(
    task_id='transform_and_stage',
    bash_command='''
    set -e
    echo "Starting data transformation and staging..."
    python /opt/airflow/src/preprocess_to_staging.py \
        --bucket_raw raw \
        --db_host mysql \
        --db_port 3306 \
        --db_user root \
        --db_password root \
        --endpoint-url http://localstack:4566
    echo "Transformation and staging completed successfully"
    ''',
    dag=dag,
)

# Tâche 3: Chargement vers MongoDB avec validation
load_task = BashOperator(
    task_id='load_and_curate',
    bash_command='''
    set -e
    echo "Starting data loading and curation..."
    python /opt/airflow/src/process_to_curated.py \
        --mysql_host mysql \
        --mysql_port 3306 \
        --mysql_user root \
        --mysql_password root \
        --mongo_uri mongodb://mongodb:27017/
    echo "Loading and curation completed successfully"
    ''',
    dag=dag,
)

# Task to validate final results
def validate_pipeline_completion(**context):
    """Validate that all pipeline stages completed successfully."""
    import mysql.connector
    from pymongo import MongoClient
    
    try:
        # Check MySQL staging data
        mysql_conn = mysql.connector.connect(
            host='mysql',
            user='root',
            password='root',
            database='staging'
        )
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM texts")
        mysql_count = cursor.fetchone()[0]
        mysql_conn.close()
        
        # Check MongoDB curated data
        mongo_client = MongoClient('mongodb://mongodb:27017/')
        mongo_count = mongo_client.curated.wikitext.count_documents({})
        mongo_client.close()
        
        logger.info(f"Pipeline validation - MySQL: {mysql_count} records, MongoDB: {mongo_count} documents")
        
        if mysql_count == 0 or mongo_count == 0:
            raise ValueError(f"Pipeline validation failed - MySQL: {mysql_count}, MongoDB: {mongo_count}")
        
        logger.info("=== Pipeline completed successfully with data validation ===")
        return f"Pipeline completed: {mysql_count} staged records, {mongo_count} curated documents"
        
    except Exception as e:
        logger.error(f"Pipeline validation failed: {e}")
        raise

final_validation_task = PythonOperator(
    task_id='validate_completion',
    python_callable=validate_pipeline_completion,
    dag=dag,
)

# Définir l'ordre des tâches avec validation
log_start_task >> validate_task >> extract_task >> transform_task >> load_task >> final_validation_task