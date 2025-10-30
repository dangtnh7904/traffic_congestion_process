import requests
import json
import pathlib

#fetch traffic data for Hanoi from HERE API and save to MinIO
def fetch_hanoi_traffic_data() -> str:
# Load API key from configs/api_keys.json
    with open('configs/api_keys.json', 'r') as f:
        config = json.load(f)

    api_key = config.get("here_api_key")

    # Bounding box for central Hanoi
    # Format: west,south,east,north (longitude, latitude)
    bbox = config.get("bbox")

    base_url = config.get("base_url", "https://data.traffic.hereapi.com/v7/flow")

    url = f"{base_url}?locationReferencing=shape&in=bbox:{bbox}&apiKey={api_key}"

    # response = requests.get(url)

    # #get the sourceUpdated time from response
    # response_json = response.json()
    with open("data/raw/2025-10-22T02-57-05Z/hanoi_traffic_data.json", "r") as f:
        response_json = json.load(f)
    source_updated = response_json.get("sourceUpdated", "unknown_time").replace(" ", "_").replace(":", "-")

    #example response to test before actual API call

    # response = {"sourceUpdated": "2024-06-15 10:30:00", "results": [{"id": 1, "speed": 30}, {"id": 2, "speed": 25}]}

    # if response.status_code != 200:
    if not response_json:
        # msg = f"Traffic API failed with {response.status_code}: {response.text[:200]}"
        raise RuntimeError("Failed to fetch traffic data from HERE API.")
    else:
        # Save to MinIO
        bucket_name = "traffic-congestion"
        file_key = f"raw/{source_updated}/hanoi_traffic_data.json"

        s3_hook = S3Hook(aws_conn_id='minio_default')

        # raise error if unable to save to MinIO
        try:
            s3_hook.get_connection('minio_default')
            # save to /raw/{source_updated}/hanoi_traffic_data.json
            s3_hook.load_string(
                string_data=json.dumps(response_json),
                bucket_name=bucket_name,
                object_name=file_key,
                replace=True
            )
        except Exception as e:
            raise ConnectionError("Failed to connect to MinIO. Please check your connection settings.") from e

        # Return the S3 path for downstream tasks
        s3_path = f"s3://{bucket_name}/{file_key}"
        return s3_path    
