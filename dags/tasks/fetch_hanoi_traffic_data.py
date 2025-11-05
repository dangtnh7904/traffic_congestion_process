# dags/tasks/traffic_fetcher.py

import json
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_hanoi_traffic_data(minio_conn_id: str, bucket_name: str) -> str:

    # Import here within the function to avoid Airflow import issues
    import requests
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.exceptions import AirflowNotFoundException
    from airflow.models import Variable 

    logger.info("Starting to fetch traffic data using Airflow Variables.")

    try:
        # get airflow variables
        api_key = Variable.get("here_api_key")
        bbox = Variable.get("hanoi_bbox")
        # using default value if variable not set
        base_url = Variable.get("here_base_url", 
                                default_var="https://data.traffic.hereapi.com/v7/flow")

    except KeyError as e:
        logger.error(f"Airflow Variable '{e.args[0]}' not found!")
        raise
    
    # Construct the request URL
    url = f"{base_url}?locationReferencing=shape&in=bbox:{bbox}&apiKey={api_key}"

    logger.info(f"Requesting URL: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        msg = f"Traffic API failed with {response.status_code}: {response.text[:200]}"
        logger.error(msg)
        raise RuntimeError(msg)

    response_json = response.json()

    # Extract timestamp for file naming
    source_updated = response_json.get("sourceUpdated", "unknown_time").replace(" ", "_").replace(":", "-")
    
    # define S3 path
    file_key = f"raw/{source_updated}/hanoi_traffic_data.json"
    s3_path = f"s3://{bucket_name}/{file_key}"



    try:
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)

        # Check if the bucket exists
        logger.info("Checking if bucket exists")
        if not s3_hook.check_for_bucket(bucket_name=bucket_name):
            logger.warning(f"Bucket '{bucket_name}' not found. Creating it...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")


        logger.info(f"Saving data to MinIO: {s3_path}")
        # Save the JSON response to MinIO
        s3_hook.load_string(
            string_data=json.dumps(response_json),
            bucket_name=bucket_name,
            key=file_key, 
            replace=True
        )
    except AirflowNotFoundException:
        logger.error(f"Connection ID '{minio_conn_id}' not found!")
        raise
    except Exception as e:
        logger.error(f"Failed to save to MinIO. Error: {e}")
        raise ConnectionError(f"Failed to save to MinIO using conn_id '{minio_conn_id}'.")

    logger.info(f"Successfully saved data to {s3_path}")
    
    # Return the S3 path of the saved file
    return s3_path