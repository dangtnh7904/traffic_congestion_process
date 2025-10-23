import requests
import json
import pathlib

BASE_DIR = pathlib.Path("opt/airflow/data/")

def fetch_hanoi_traffic_data(run_id: str) -> str:
# Load API key from configs/api_keys.json
    with open('configs/api_keys.json', 'r') as f:
        config = json.load(f)

    api_key = config.get("here_api_key")

    # Bounding box for central Hanoi
    # Format: west,south,east,north (longitude, latitude)
    bbox = config.get("bbox")

    base_url = config.get("base_url", "https://data.traffic.hereapi.com/v7/flow")

    url = f"{base_url}?locationReferencing=shape&in=bbox:{bbox}&apiKey={api_key}"

    response = requests.get(url)

    #example response to test before actual API call

    # response = {"sourceUpdated": "2024-06-15 10:30:00", "results": [{"id": 1, "speed": 30}, {"id": 2, "speed": 25}]}

    if response.status_code != 200:
        print(f"Error fetching traffic data: {response.status_code}")
        return None
    else:
        traffic_data = response.json()
        raw_file_name = "traffic_data.json" 

        save_folder = BASE_DIR / "raw" / run_id
        save_folder.mkdir(parents=True, exist_ok=True)

        raw_file_path = save_folder / raw_file_name

        with open(raw_file_path, 'w', encoding='utf-8') as f:
            json.dump(traffic_data, f, ensure_ascii=False, indent=2)
        print(f"Saved raw traffic data to: {raw_file_path}")

        return str(raw_file_path)