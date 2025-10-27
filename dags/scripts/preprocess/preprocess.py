import json

from dags.schema.traffic_schema import root_schema
def process_traffic_data(**kwargs):
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, monotonically_increasing_id
    except ImportError as e:
        raise ImportError("PySpark is not installed. Please install PySpark to use this function.") from e

    spark = SparkSession.builder.appName("TrafficDataProcessing").getOrCreate()
    ti = kwargs['ti']

    raw_file_path = ti.xcom_pull(task_ids='fetch_hanoi_traffic_data')
    
    parsed_df = spark.read.json(raw_file_path).schema(root_schema)

    #first flatten the results array
    parsed_df = parsed_df.selectExpr("sourceUpdated", "explode(results) as result")



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
        .filter(col("traverse_length") > 0)
    
    #remove null jamFactor and using this function to fill missing jamFactor 
    # JF=min(10,max(0,10×(1−speed/freeFlowSpeed)))
    clean_df = clean_df\
        .withColumn("jamFactor", 
                    col("jamFactor").when(col("jamFactor").isNull(),
                    10 * (1 - (col("speed") / col("freeFlowSpeed"))))\
                    .otherwise(col("jamFactor"))
                    )
    #choose only data that has confidence score above threshold
    confidence_threshold = 0.7
    clean_df = clean_df.filter(col("confidence") >= confidence_threshold)

    # result

