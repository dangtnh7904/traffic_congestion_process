from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import logging
import argparse
from spark_config import (
    SPARK_S3_CONFIG,
    SPARK_SEDONA_CONFIG,
    SPARK_S3_POSTGRES_SEDONA_PACKAGES,
    SPARK_CONN_ID,
    SPARK_DEPLOY_MODE,
    get_postgres_env_vars
)

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = 'postgres_traffic_db'
MINIO_CONN_ID = "minio_traffic_data"

MY_BUCKET_NAME = "traffic-congestion"
PROCESS_FILE= 'dags/sparks_job/process_road_network.py'

CHUNK_SIZE = 100

db_road_network_table = "road_network"
db_road_segment_table = "road_segments"

default_args = {
    'owner': 'airflow',
    'retries': True,
    'retry_delay': 60,
}

@dag(
    dag_id='static_road_network_dagv8',
    default_args=default_args,
    description='DAG to fetch and store static road network data for Hanoi',
    schedule= '0 0 1 * *',  # Monthly at midnight on the first day
    start_date=(datetime(2024, 6, 1)),
    catchup=False,
    tags=['static_road_network', 'data_fetch'],
)
def static_road_network_dag():
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



    @task(task_id='fetch_road_network_task')
    def fetch_road_network_task():
        from static_road_tasks.fetch_road_network import fetch_road_network
        s3_path = fetch_road_network(MINIO_CONN_ID, MY_BUCKET_NAME)
        s3_path = s3_path.replace("s3://", "s3a://")
        return s3_path

    # truncate the old data before processing new data
    truncate_task = PostgresOperator(
        task_id='truncate_road_network_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"TRUNCATE TABLE {db_road_network_table} RESTART IDENTITY CASCADE;", # restarts identity/serial columns
        autocommit=True
    )
    truncate_segment_task = PostgresOperator(
        task_id='truncate_road_segment_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"TRUNCATE TABLE {db_road_segment_table} RESTART IDENTITY CASCADE;", # restarts identity/serial columns
        autocommit=True
    )

    # retrieve fetched path
    s3_fetched_path = fetch_road_network_task()
    
    process_road_network_task = SparkSubmitOperator(
        task_id='process_road_network_task',
        application=PROCESS_FILE,
        name='process_road_network',
        conn_id=SPARK_CONN_ID,
        conf = dynamic_s3_config,
        deploy_mode=SPARK_DEPLOY_MODE,
        packages = SPARK_S3_POSTGRES_SEDONA_PACKAGES,
        env_vars=get_postgres_env_vars(POSTGRES_CONN_ID),
        application_args = [
            "--input_path", s3_fetched_path,
            # "--output_path", s3_feature_path,
            "--db_road_network_table", db_road_network_table,
            "--db_road_segment_table", db_road_segment_table,
            "--chunk_size", str(CHUNK_SIZE)
        ]
    )
    s3_fetched_path >> truncate_task >> truncate_segment_task >> process_road_network_task

static_road_network_dag() 


    