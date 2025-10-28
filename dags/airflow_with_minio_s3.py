from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'dangg',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="airflow_with_minio_s3",
    start_date=datetime(2025, 9, 9),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["example", "s3", "minio"],
) as dag:
    def show_endpoint(**kwargs):
        conn = BaseHook.get_connection("minio_default")
        extras = getattr(conn, "extra_dejson", {}) or {}
        endpoint = extras.get("endpoint") or extras.get("endpoint_url") or conn.host
        #get mean get the value of the atriibute, if not exist, return default value None
        logging.info("minio_conn endpoint=%s", endpoint)    
        return endpoint

    show_endpoint = PythonOperator(
        task_id="show_endpoint",
        python_callable=show_endpoint,
    )

    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_key="data.csv",
        bucket_name="airflow",
        aws_conn_id="minio_default",
        poke_interval=5,
        timeout=100,
    )

    show_endpoint >> wait_for_file


    process_file = BashOperator(
        task_id="process_file",
        bash_command='echo "Processing file..."',
    )

    wait_for_file >> process_file
