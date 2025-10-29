import json
from resources.schema.traffic_schema import root_schema

def process_traffic_data(**kwargs):
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, greatest, least, when
    except ImportError as e:
        raise ImportError("PySpark is not installed. Please install PySpark to use this function.") from e

    spark = SparkSession.builder.appName("PreprocessTrafficData").getOrCreate()
    ti = kwargs['ti']

    #Fetch the s3 path from XCom
    s3_path = ti.xcom_pull(task_ids='fetch_hanoi_traffic_data')
    # Read the raw JSON data from S3
    raw_df = spark.read.json(s3_path, schema=root_schema)

    # Flatten the DataFrame
    parsed_df = raw_df.selectExpr("sourceUpdated", "explode(results) as result")
    
    # Flatten the DataFrame and selecting the required fields
    flattened_df = parsed_df.select(
        col("sourceUpdated").alias("timestamp"),
        col("result.location.description").alias("description"),
        col("result.location.length").alias("traverse_length"),
        col("result.location.shape").alias("shape"),
        col("result.location.links").alias("links"),
        col("result.currentFlow.speed").alias("speed"),
        col("result.currentFlow.jamFactor").alias("jamFactor"),
        col("result.currentFlow.confidence").alias("confidence"),
        col("result.currentFlow.traversability").alias("traversability"),
        col("result.currentFlow.freeFlowSpeed").alias("freeFlowSpeed")
    )


    #consolidate and clean data
    clean_df = flattened_df\
        .filter(col("speed").isNotNull())\
        .filter(col("speed") > 0)\
        .filter(col("traverse_length") > 0)\
        .filter(col("freeFlowSpeed").isNotNull())\
        .filter(col("freeFlowSpeed") > 0)
    
    calculated_jamFactor = greatest(lit(0), 
        least(lit(10), 
              10 * (1 - (col("speed") / col("freeFlowSpeed")))
            )
    )

    #remove null jamFactor and using this function to fill missing jamFactor 
    # JF=min(10,max(0,10×(1−speed/freeFlowSpeed)))
    clean_df = clean_df\
        .withColumn("jamFactor", 
            when(col("jamFactor").isNull(),
                calculated_jamFactor  # Use the full, correct formula
            ).otherwise(col("jamFactor"))
        )
    
    #choose only data that has confidence score above threshold
    confidence_threshold = 0.7
    clean_df = clean_df.filter(col("confidence") >= confidence_threshold)

    #save the cleaned data back to S3 in parquet format
    output_path = s3_path.replace("raw", "processed").replace(".json", ".parquet")
    clean_df.write.mode("overwrite").parquet(output_path)

    #check if data is saved correctly
    try:
        _ = spark.read.parquet(output_path)
    except Exception as e:
        raise IOError(f"Failed to read back the processed data from {output_path}") from e
    
    # Return the output path for downstream tasks
    return output_path