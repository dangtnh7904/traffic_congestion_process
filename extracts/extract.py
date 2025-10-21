from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

point_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True)
])

link_schema = StructType([
    StructField("points", ArrayType(point_schema), True),
    StructField("length", DoubleType(), True),
    StructField("functionalClass", LongType(), True)
])

location_schema = StructType([
    StructField("description", StringType(), True),
    StructField("length", DoubleType(), True),
    StructField("shape", StructType([
        StructField("links", ArrayType(link_schema), True)
    ]), True)
])

current_flow_schema = StructType([
    StructField("speed", DoubleType(), True),
    StructField("speedUncapped", DoubleType(), True),
    StructField("freeFlow", DoubleType(), True),
    StructField("jamFactor", DoubleType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("traversability", StringType(), True)
])

results_schema = StructType([
    StructField("location", location_schema, True),
    StructField("currentFlow", current_flow_schema, True)
])


df = spark.read.schema(results_schema).json("data\hanoi_traffic_data.jsonl")
# df.printSchema()
df.cache()
df.show(10)


#select road descriptions
selected_df = df.select(
    df.location.description.alias("road_description"),)
selected_df.createOrReplaceTempView("selected_df")
#take the distinct road descriptions
distinct_selected_df = spark.sql(
    """
    WITH ranked AS (
    SELECT 
        road_description,
        ROW_NUMBER() OVER (PARTITION BY road_description ORDER BY road_description DESC) AS rn
    FROM selected_df
    )
    SELECT road_description
    FROM ranked
    WHERE rn = 1;
    """
)

distinct_selected_df.withColumn("road_id", func.monotonically_increasing_id()).show(10, truncate=False)
distinct_selected_df.cache()
#try to read old road dictionary
try:
    old_road_dict_df = spark.read.json("mapping_road_dictionary/road_dictionary.json")
except:
    old_road_dict_df = spark.createDataFrame([], distinct_selected_df.schema)

old_road_dict_df.createOrReplaceTempView("old_road_dict_df")
# selected_df.show(10, truncate=False)
#save to json dictionary
distinct_selected_df.withColumn("road_id", func.monotonically_increasing_id()) \
    .write.mode("overwrite").json("mapping_road_dictionary/road_dictionary.json")

spark.stop()