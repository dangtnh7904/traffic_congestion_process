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
        col("result.currentFlow.traversability").alias("traversability")
    )

    # Label the description fields with id
    labeled_df = flattened_df.select("description")\
        .withColumn("id", monotonically_increasing_id())

