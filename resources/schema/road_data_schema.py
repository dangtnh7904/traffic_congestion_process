from pyspark.sql.types import StructType, StructField, StringType, IntegerType

road_data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("road_name", StringType(), True),
])
