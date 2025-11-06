from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from spark_config import (
    SPARK_S3_CONFIG, 
    SPARK_SEDONA_CONFIG,
    SPARK_S3_PACKAGES, 
    SPARK_S3_POSTGRES_PACKAGES,
    SPARK_S3_POSTGRES_SEDONA_PACKAGES,
    SPARK_CONN_ID, 
    SPARK_DEPLOY_MODE
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = 'postgres_traffic_db'
PROCESS_FILE= 'dags/sparks_job/preprocess.py'
ENGINEER_FILE= 'dags/sparks_job/feature_app_data.py'
MY_BUCKET_NAME = "traffic-congestion"
MINIO_CONN_ID = "minio_traffic_data"


db_label_table = "street_labels"
db_traffic_table = "traffic_events"

default_args = {
    'owner': 'airflow',
    'retries': False,
    'retry_delay': 60,
}

@dag(
    dag_id='process_traffic_data_dagv12',
    default_args=default_args,
    description='DAG to process traffic data using Spark jobs',
    schedule_interval=None,
    start_date=None,
    catchup=False,
    tags=['traffic', 'spark', 'data_processing'],
)

def process_traffic_data_dag():
    
    #get postgres connection details
    @task(task_id='get_postgres_connection_details')
    def get_postgres_connection_details() -> dict:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_connection(POSTGRES_CONN_ID)
        jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        db_user = conn.login
        db_password = conn.password
        return {
            "jdbc_url": jdbc_url,
            "db_user": db_user,
            "db_password": db_password,
        }
    
    conn_details = get_postgres_connection_details()

    @task(task_id='fetch_data_task')
    def fetch_data_task():
        from tasks.fetch_hanoi_traffic_data import fetch_hanoi_traffic_data
        s3_path = fetch_hanoi_traffic_data(MINIO_CONN_ID, MY_BUCKET_NAME)
        # Convert s3:// to s3a:// for Spark compatibility
        s3_path = s3_path.replace("s3://", "s3a://")
        return s3_path
    
    # get s3 raw path
    @task
    def get_s3_process_path(s3_raw_path: str):
        s3_process_path = s3_raw_path.replace("raw", "processed")\
                                    .replace(".json", ".parquet")
        return s3_process_path
    
    # get s3 feature path (separate from processed path)
    @task
    def get_s3_feature_path(s3_raw_path: str):
        s3_feature_path = s3_raw_path.replace("raw", "features")\
                                    .replace(".json", ".parquet")
        return s3_feature_path

    # retrieve s3 raw path
    s3_raw_path = fetch_data_task()

    # get s3 processed path
    s3_process_path = get_s3_process_path(s3_raw_path)
    
    # get s3 feature path
    s3_feature_path = get_s3_feature_path(s3_raw_path)
    
    preprocess_task = SparkSubmitOperator(
        task_id='preprocess_traffic_data',
        application=PROCESS_FILE,
        conn_id=SPARK_CONN_ID,
        conf=SPARK_S3_CONFIG,
        packages=SPARK_S3_PACKAGES,
        deploy_mode=SPARK_DEPLOY_MODE,
        application_args = [
            "--input_path", s3_raw_path,
            "--output_path", s3_process_path
        ]
    )


    feature_app_data = SparkSubmitOperator(
        task_id='engineer_feature_data',
        application=ENGINEER_FILE,
        conn_id=SPARK_CONN_ID,
        conf=SPARK_SEDONA_CONFIG,  # Use Sedona config for GIS functions
        packages=SPARK_S3_POSTGRES_SEDONA_PACKAGES,  # Include Sedona packages
        deploy_mode=SPARK_DEPLOY_MODE,
        application_args = [
            "--input_path", s3_process_path,  # Read from processed
            "--output_path", s3_feature_path,  # Write to features (different path!)
            "--jdbc_url", "{{ task_instance.xcom_pull(task_ids='get_postgres_connection_details')['jdbc_url'] }}",
            "--db_user", "{{ task_instance.xcom_pull(task_ids='get_postgres_connection_details')['db_user'] }}",
            "--db_password", "{{ task_instance.xcom_pull(task_ids='get_postgres_connection_details')['db_password'] }}",
            "--db_label_table", db_label_table,
            "--db_traffic_table", db_traffic_table
        ]
    )

    conn_details >> s3_raw_path >> s3_process_path >> s3_feature_path >> preprocess_task >> feature_app_data

process_traffic_data_dag()