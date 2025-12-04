from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from spark_config import (
    SPARK_S3_CONFIG, 
    SPARK_SEDONA_CONFIG,
    SPARK_S3_PACKAGES, 
    SPARK_S3_POSTGRES_SEDONA_PACKAGES,
    SPARK_CONN_ID, 
    SPARK_DEPLOY_MODE,
    get_postgres_env_vars
)
import logging
logger = logging.getLogger(__name__)


POSTGRES_CONN_ID = 'postgres_traffic_db'
MINIO_CONN_ID = "minio_traffic_data"

PROCESS_FILE= 'dags/sparks_job/preprocess.py'
ENGINEER_FILE= 'dags/sparks_job/engineer_feature_data.py'

MY_BUCKET_NAME = "traffic-congestion"

# Database table names
db_static_road_segments_table = "road_segments"
db_traffic_status_table = "traffic_events"

default_args = {
    'owner': 'airflow',
    'retries': True,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='process_traffic_data_dagv1',
    default_args=default_args,
    description='DAG to process traffic data using Spark jobs',
    schedule=timedelta(days=1), 
    start_date=(datetime(2025, 11, 11)),
    catchup=False,
    # backfill=False,
    tags=['traffic', 'spark', 'data_processing'],
)

def process_traffic_data_dag():

    # create dynamic s3 config with access key and secret key
    dynamic_s3_config = SPARK_S3_CONFIG.copy()
    dynamic_s3_config.update({
        # get rid of top-level conn reference error by using f-string
        # via jinja templating
        'spark.hadoop.fs.s3a.access.key': f"{{{{ conn.{MINIO_CONN_ID}.login }}}}",
        'spark.hadoop.fs.s3a.secret.key': f"{{{{ conn.{MINIO_CONN_ID}.password }}}}"
    })

    # create dynamic sedona config by merging s3 config
    dynamic_sedona_config = SPARK_SEDONA_CONFIG.copy()
    # Merge S3 config into Sedona config
    dynamic_sedona_config.update(dynamic_s3_config)

    
    # fetch data task
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
    

    # spark task 1
    preprocess_task = SparkSubmitOperator(
        task_id='preprocess_traffic_data',
        application=PROCESS_FILE,
        conn_id=SPARK_CONN_ID,
        conf=dynamic_s3_config,
        packages=SPARK_S3_PACKAGES,
        deploy_mode=SPARK_DEPLOY_MODE,
        application_args = [
            "--input_path", s3_raw_path,
            "--output_path", s3_process_path
        ]
    )


    engine_feature_data = SparkSubmitOperator(
        task_id='engineer_feature_data',
        application=ENGINEER_FILE,
        conn_id=SPARK_CONN_ID,
        conf=dynamic_sedona_config,  # Use Sedona config for GIS functions
        packages=SPARK_S3_POSTGRES_SEDONA_PACKAGES,  # Include Sedona packages
        deploy_mode=SPARK_DEPLOY_MODE,
        env_vars=get_postgres_env_vars(POSTGRES_CONN_ID),
        
        application_args = [
            "--input_path", s3_process_path,
            "--output_path", s3_feature_path,
            "--jdbc_url", f"jdbc:postgresql://{{{{ conn.{POSTGRES_CONN_ID}.host }}}}:{{{{ conn.{POSTGRES_CONN_ID}.port }}}}/{{{{ conn.{POSTGRES_CONN_ID}.schema }}}}",
            "--db_user", f"{{{{ conn.{POSTGRES_CONN_ID}.login }}}}",
            "--db_password", f"{{{{ conn.{POSTGRES_CONN_ID}.password }}}}",
            "--db_static_road_segments_table", db_static_road_segments_table,
            "--db_traffic_status_table", db_traffic_status_table
        ]
    )

    preprocess_task >> engine_feature_data

process_traffic_data_dag()