from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'retries': False,
    'retry_delay': 60,
}

dag = DAG(
    'process_dag',
    default_args=default_args,
    description='A DAG for processing data',
    schedule_interval=None,
    start_date=None,
    catchup=False,
)

engineer_features_task = SparkSubmitOperator(
        task_id="engineer_feature_data",
        application="dags/spark_jobs/engineer_features.py", # Sửa lại tên file cho chuẩn
        conn_id="your_spark_conn_id", # Connection tới Spark master
        verbose=True,
        application_args=[
            "--input-path", processed_data_path,
            "--output-path", feature_data_path,
            "--jdbc-url", jdbc_url,
            "--db-user", db_user,
            "--db-password", db_password,
            "--db-table", db_table
        ]
    )