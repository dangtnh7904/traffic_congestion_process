# dags/sparks_job/engineer_feature_data.py

try:
    import argparse 
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, greatest, least, when, broadcast

except ImportError as e:
    raise ImportError("Required modules are not installed. Please install the necessary packages to run this script.") from e

def engineer_feature_data(spark : SparkSession, input_path: str, output_path: str):
    logging.info(f"Reading data from: {input_path}")

    # Read the cleaned data from S3
    clean_df = spark.read.parquet(input_path)

    # Load Postgres configuration from 
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()
    

    # Fetch the s3 path from XCom
    s3_path = ti.xcom_pull(task_ids='preprocess_traffic_data')
    # Read the cleaned data from S3
    clean_df = spark.read.parquet(s3_path)

    # Extract the road name to get road names
    parsed_df = clean_df.select("road_name").distinct()

    new_roads_df = parsed_df.join(
        labels_df,
        on="road_name",
        how="left_anti"
    )

    #load new road names to Postgres
    try:
        new_roads_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "street_labels") \
            .mode("append") \
            .options(**connection_properties) \
            .save()
        
        print("Successfully appended new streets to Postgres.")

    except Exception as e:
        msg = f"Error appending new streets to Postgres: {e}"
        logger.error(msg)



