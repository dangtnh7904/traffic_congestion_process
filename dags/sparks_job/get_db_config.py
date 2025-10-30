import json
from pyspark.sql import SparkSession

def get_db_config(config_path="db_config.json"):
    """
    Reads the database configuration from a JSON file.
    """
    try:
        with open("configs/db_config.json", 'r') as f:
            config = json.load(f)
        
        # Now, let's unpack the values
        jdbc_url = config['jdbc_url']
        connection_properties = config['connection_properties']
        
        # We also need the table name, which we can get from the properties
        # or pass in separately. Let's assume it's not in the config file.
        
        return jdbc_url, connection_properties

    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        raise
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_path}")
        raise
    except KeyError as e:
        print(f"Error: Missing key {e} in config file")
        raise