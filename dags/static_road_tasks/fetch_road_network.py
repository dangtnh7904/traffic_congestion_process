#down load bbox from open street map
import logging

logger = logging.getLogger(__name__)

VAR_BBOX_NAME = "hanoi_bbox"


def fetch_road_network(minio_conn_id: str, bucket_name: str) -> str:

    # Import here within the function to avoid Airflow import issues
    import osmnx as ox
    import os
    import geopandas as gpd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.exceptions import AirflowNotFoundException
    from airflow.models import Variable 

    coords = Variable.get("hanoi_bbox", deserialize_json=True)
    try:
        coords = Variable.get(VAR_BBOX_NAME, deserialize_json=True)
        n = coords['north']
        s = coords['south'] 
        e = coords['east'] 
        w = coords['west']
        
    except TypeError:
        logger.error(f"Variable '{VAR_BBOX_NAME}' not found or is empty! (TypeError)")
        raise
    except KeyError as e:
        logger.error(f"Variable '{VAR_BBOX_NAME}' is missing key: {e} (KeyError)")
        raise

    logger.info("Downloading road network data from OpenStreetMap...")
    G = ox.graph_from_bbox(n, s, e, w, network_type='drive', retain_all=True)

    logger.info("Converting graph to GeoDataFrame...")
    gdf_edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

    logger.info(f"Columns in the GeoDataFrame: {list(gdf_edges.columns)}")

    logger.info("Resetting index to access 'u' and 'v' as columns...")
    gdf_edges = gdf_edges.reset_index()

    logger.info("Select relevant columns...")
    target_columns = [
        'osmid',     
        'u', 'v',  
        'name',      
        'oneway',    
        'highway', 
        'length', 
        'maxspeed',
        'lanes',
        'geometry',        
    ]
    
    # Filter only existing columns to avoid KeyError
    existing_cols = [col for col in target_columns if col in gdf_edges.columns]

    # Loging final columns
    gdf_edges = gdf_edges[existing_cols]
    logger.info(f"Final columns to save: {list(gdf_edges.columns)}")
    
    for col in gdf_edges.columns:
    # if not geometry and is object, convert to string
        if col != 'geometry' and gdf_edges[col].dtype == 'object': 
            gdf_edges[col] = gdf_edges[col].astype(str)


    # define S3 path
    local_file = "/tmp/hanoi_road_network.parquet"
    file_key = f"tmp/hanoi_road_network.parquet"
    s3_path = f"s3://{bucket_name}/{file_key}"

    # save to minio as parquet
    logger.info(f"Check if bucket exists...")   
    try:
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)

        if not s3_hook.check_for_bucket(bucket_name=bucket_name):
            logger.warning(f"Bucket '{bucket_name}' not found. Creating it...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")

        #save to local file
        gdf_edges.to_parquet(local_file, index=False)

        logger.info("Saving road network data to MinIO...")
        s3_hook.load_file(
            filename=local_file, 
            key=file_key,
            bucket_name=bucket_name,
            replace=True
        )

    except AirflowNotFoundException:
        logger.error(f"Connection ID '{minio_conn_id}' not found!")
        raise
    except Exception as e:
        logger.error(f"Failed to save to MinIO. Error: {e}")
        raise ConnectionError(f"Failed to save to MinIO using conn_id '{minio_conn_id}'.")
    finally:
        if os.path.exists(local_file):
            os.remove(local_file)


    logger.info(f"Successfully saved road network data to {s3_path}")

    return s3_path





