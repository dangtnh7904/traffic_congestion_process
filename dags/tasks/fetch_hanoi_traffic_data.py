# dags/tasks/traffic_fetcher.py

import json
import logging 

def fetch_hanoi_traffic_data(minio_conn_id: str) -> str:
    
    # Import heare within the function to avoid Airflow import issues
    import requests
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.exceptions import AirflowNotFoundException
    from airflow.models import Variable 

    logging.info("Starting to fetch traffic data using Airflow Variables.")

    try:
        # get airflow variables
        api_key = Variable.get("here_api_key")
        bbox = Variable.get("hanoi_bbox")
        # using default value if variable not set
        base_url = Variable.get("here_base_url", 
                                default_var="https://data.traffic.hereapi.com/v7/flow")

    except KeyError as e:
        logging.error(f"Airflow Variable '{e.args[0]}' not found!")
        raise
    
    # Construct the request URL
    url = f"{base_url}?locationReferencing=shape&in=bbox:{bbox}&apiKey={api_key}"

    logging.info(f"Requesting URL: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        msg = f"Traffic API failed with {response.status_code}: {response.text[:200]}"
        logging.error(msg)
        raise RuntimeError(msg)

    response_json = response.json()

    # Extract timestamp for file naming
    source_updated = response_json.get("sourceUpdated", "unknown_time").replace(" ", "_").replace(":", "-")
    
    # Save the response to MinIO
    bucket_name = "traffic-congestion"
    file_key = f"raw/{source_updated}/hanoi_traffic_data.json"
    s3_path = f"s3://{bucket_name}/{file_key}"

    try:
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)
        logging.info(f"Saving data to MinIO: {s3_path}")
        
        s3_hook.load_string(
            string_data=json.dumps(response_json),
            bucket_name=bucket_name,
            key=file_key, 
            replace=True
        )
    except AirflowNotFoundException:
        logging.error(f"Connection ID '{minio_conn_id}' not found!")
        raise
    except Exception as e:
        logging.error(f"Failed to save to MinIO. Error: {e}")
        raise ConnectionError(f"Failed to save to MinIO using conn_id '{minio_conn_id}'.")
    
    logging.info(f"Successfully saved data to {s3_path}")

    return s3_path