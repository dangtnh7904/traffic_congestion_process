# dags/spark_jobs/preprocess.py
try:
    import argparse 
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, greatest, least, when
except ImportError as e:
    raise ImportError("Required modules are not installed. Please install the necessary packages to run this script.") from e

from traffic_schema import root_schema 

def run_spark_job(spark: SparkSession, input_path: str, output_path: str):

    logging.info(f"Reading data from: {input_path}")

    # Read raw JSON data with predefined schema
    raw_df = spark.read.json(input_path, schema=root_schema)

    # Flatten nested structures
    parsed_df = raw_df.selectExpr("sourceUpdated", "explode(results) as result")

    # Further flattening and selecting relevant fields
    flattened_df = parsed_df.select(
        col("sourceUpdated").alias("timestamp"),
        col("result.location.description").alias("road_name"),
        col("result.location.length").alias("traverse_length"),
        col("result.location.shape").alias("shape"),
        col("result.location.links").alias("links"),
        col("result.currentFlow.speed").alias("speed"),
        col("result.currentFlow.jamFactor").alias("jamFactor"),
        col("result.currentFlow.confidence").alias("confidence"),
        col("result.currentFlow.traversability").alias("traversability"),
        col("result.currentFlow.freeFlowSpeed").alias("freeFlowSpeed")
    )
    # Data Cleaning Steps
    clean_df = flattened_df\
        .filter(col("speed").isNotNull() & (col("speed") > 0))\
        .filter(col("traverse_length") > 0)\
        .filter(col("freeFlowSpeed").isNotNull() & (col("freeFlowSpeed") > 0))
    
    
    # Logic to calculate jamFactor if missing
    calculated_jamFactor = greatest(lit(0), 
        least(lit(10), 
              10 * (1 - (col("speed") / col("freeFlowSpeed")))
            )
    )
    # Fill missing jamFactor values
    clean_df = clean_df\
        .withColumn("jamFactor", 
            when(col("jamFactor").isNull(),
                    calculated_jamFactor
            ).otherwise(col("jamFactor"))
        )
    
    # Filter out low-confidence records
    confidence_threshold = 0.7
    clean_df = clean_df.filter(col("confidence") >= confidence_threshold)

    # Write the cleaned data to Parquet
    logging.info(f"Write parquet to: {output_path}")
    clean_df.write.mode("overwrite").parquet(output_path)

    try:
        _ = spark.read.parquet(output_path)
        logging.info("Read back processed data successfully.")
    except Exception as e:
        raise IOError(f"Failed to read back processed data from {output_path}") from e
    

# Entry point for standalone execution
if __name__ == "__main__":

    # 1. Set up to read parameters from command-line
    parser = argparse.ArgumentParser(description="Spark Traffic Preprocessing Job")
    parser.add_argument(
        '--input-path',
        required=True,
        help="S3 path to the raw input JSON data."
    )
    parser.add_argument(
        '--output-path',
        required=True,
        help="S3 path prefix to write the processed Parquet data."
    )
    args = parser.parse_args()

    # 2. Initialize Spark Session
    spark = SparkSession.builder.appName("PreprocessTrafficData").getOrCreate()
    
    try:
        # 3. Run main logic with parsed parameters
        run_spark_job(
            spark, 
            args.input_path, 
            args.output_path_prefix
        )
    finally:
        # Stop Spark session 
        spark.stop()