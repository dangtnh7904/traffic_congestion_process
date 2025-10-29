from pyspark.sql import SparkSession
import psycopg2
import logging
import pathlib
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def engineer_feature_data(**kwargs):
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, greatest, least, when
    except ImportError as e:
        raise ImportError("PySpark is not installed. Please install PySpark to use this function.") from e
    
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()
    ti = kwargs['ti']

    # Fetch the s3 path from XCom
    s3_path = ti.xcom_pull(task_ids='preprocess_traffic_data')
    # Read the cleaned data from S3
    clean_df = spark.read.parquet(s3_path)

    # Load Postgres configuration
    with open('configs/postgres.json', 'r') as f:
            pg_config = json.load(f)
    
    #fetch from postgres to get existing road descriptions from exsisting configured database
    try:
        conn = psycopg2.connect(**pg_config)
        cursor = conn.cursor()
        cursor.execute("SELECT description FROM road_data")
        distinct_road_descriptions = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching road descriptions from Postgres: {e}")
        distinct_road_descriptions = []

    # Create a DataFrame from the distinct road descriptions
    distinct_road_df = spark.createDataFrame(distinct_road_descriptions, ["description"]).schema(InferSchema=True)

    #Extract the description to get road names
    parsed_df = clean_df.withColumn("road_name", col("description"))

    #Label id for each description
    from pyspark.sql.functions import broadcast

    new_roads_df = parsed_df.join(
        
    )