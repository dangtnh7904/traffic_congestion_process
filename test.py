from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()