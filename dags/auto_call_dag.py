from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
}
with DAG(
    'auto_call_dag',
    default_args=default_args,
    description='A simple DAG to call a Python script',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 10, 15),
    catchup=False,
) as dag:

    run_script = BashOperator(
        task_id='run_python_script',
        bash_command='python D:/career/Project/traffic_congestion_process/prepocessing.py',
    )

run_script