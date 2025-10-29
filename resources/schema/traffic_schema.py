from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType

# --- Định nghĩa các schema nhỏ ---

# Cấu trúc của "points" (lat, lng)
point_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True)
])

# Cấu trúc của "links" (chứa mảng 'points')
link_schema = StructType([
    StructField("points", ArrayType(point_schema), True),
    StructField("length", DoubleType(), True),
    StructField("functionalClass", StringType(), True)
])

# Cấu trúc của "location"
location_schema = StructType([
    StructField("description", StringType(), True),
    StructField("length", DoubleType(), True),
    StructField("shape", StructType([
        StructField("links", ArrayType(link_schema), True)
    ]), True)
])

# Cấu trúc của "currentFlow"
flow_schema = StructType([
    StructField("speed", DoubleType(), True),
    StructField("speedUncapped", DoubleType(), True),
    StructField("freeFlow", DoubleType(), True),
    StructField("jamFactor", DoubleType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("traversability", DoubleType(), True)
])

# --- Đây là schema tổng bạn sẽ IMPORT ---
# Cấu trúc TỔNG của mảng "results"
result_schema = StructType([
    StructField("location", location_schema, True),
    StructField("currentFlow", flow_schema, True)
])

root_schema = StructType([
    StructField("sourceUpdated", TimestampType(), True), 
    StructField("results", ArrayType(result_schema), True)
])


