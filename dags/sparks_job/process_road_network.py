from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, expr, ceil, sequence, explode, lit, concat
import logging
import argparse
from sedona.register import SedonaRegistrator
import os 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# cleaning the speed limit column
def clean_speed_limit(speed_str):
    if speed_str is None or speed_str == "nan" or speed_str == "":
        return None
    try:
        if "[" in speed_str:
            clean_str = speed_str.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
            speeds = [float(s.strip()) for s in clean_str.split(",") if s.strip()]
                # take the minimum speed limit
            return min(speeds) if speeds else None
        # condition 2 - single value with units
        return float(speed_str)
    except Exception as e:
        logger.warning(f"Could not convert speed limit '{speed_str}' to float: {e}")
        return None
    
# register UDF
clean_speed_limit_udf = udf(clean_speed_limit, StringType())

def process_road_network(spark : SparkSession,
                        input_path: str,
                        # output_path: str,
                        db_road_network_table: str,
                        db_road_segment_table: str,
                        chunk_size: int
                        ) -> str:
    
    try:
    # Fetch DB credentials from environment variables
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")
        db_host = os.environ.get("DB_HOST")
        db_port = os.environ.get("DB_PORT")
        db_schema = os.environ.get("DB_SCHEMA", "public")

        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_schema}"
    except Exception as e:
        logger.error(f"Error fetching DB credentials from environment variables: {e}")
        raise


    logger.info("Reading road network data from temporary file in minio")

    # Read the road network data from the temporary Parquet file in MinIO
    try: 
        df_raw = spark.read.parquet(input_path)
    except Exception as e:
        logger.error(f"Error reading road network data from {input_path}: {e}")
        raise
    
    logger.info(f"Road network data read successfully with {df_raw.count()} records.")

    
    # remove identical columns if any
    df_raw = df_raw.filter(col("u") != col("v"))
    # Deduplicate based on (osmid, u, v) - keep first occurrence
    # This handles cases where multiple OSM ways share the same topology
    df_raw = df_raw.dropDuplicates(["osmid", "u", "v"])

    df_raw.cache()


    # create parent table road_network for routing
    logger.info("Preparing parent table (road_network) for routing topology")
    
    df_road_network = df_raw \
        .withColumn("temp_geom", expr("ST_GeomFromWKB(geometry)")) \
        .withColumn("length", expr("ST_LengthSpheroid(temp_geom)")) \
        .withColumn("geometry_hex", expr("hex(ST_AsBinary(temp_geom))")) \
        .withColumn("azimuth", expr("degrees(ST_Azimuth(ST_StartPoint(temp_geom), ST_EndPoint(temp_geom)))")) \
        .drop("temp_geom", "geometry")
    
    # Clean speed limit for routing cost calculation
    df_road_network_clean = df_road_network.withColumn(
        "speed_limit_clean", 
        clean_speed_limit_udf(col("maxspeed"))
    )
    
    df_road_network_save = df_road_network_clean.select(
        # Column order matching database schema: u, v first (composite PK)
        "u",
        "v",
        "osmid",
        "name",
        "oneway",
        "highway",
        col("speed_limit_clean").alias("speed_limit"),
        "lanes",
        "length",
        "azimuth",
        col("geometry_hex").alias("geometry")
    )
    
    logger.info(f"Parent table (road_network) has {df_road_network_save.count()} records after deduplication")
    
    # Save parent table to PostgreSQL
    try:
        logger.info(f"Saving parent table to {db_road_network_table}")
        df_road_network_save.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_road_network_table) \
            .option("user", db_user) \
            .option("driver", "org.postgresql.Driver") \
            .option("stringtype", "unspecified") \
            .option("password", db_password) \
            .option("batchsize", 1000) \
            .mode("overwrite") \
            .option("truncate", "true") \
            .save()
        logger.info("Parent table saved successfully")
    except Exception as e:
        logger.error(f"Error saving parent table: {e}")
        raise
        


    # create child table road_segments for map matching
    # using Sedona to process geometries
    logger.info(f"Splitting road network into segments using Sedona Native SQL (chunk_size={chunk_size}m)")
    
    # Create geometry column from WKB and calculate length in meters
    df_with_geom = df_raw \
        .withColumn("temp_geom", expr("ST_GeomFromWKB(geometry)")) \
        .withColumn("len_m", expr("ST_LengthSpheroid(temp_geom)"))
    
    # count before splitting
    logger.info(f"Road network data has {df_with_geom.count()} records before splitting linestrings.")
    df_calc = df_with_geom \
        .withColumn("num_chunks", ceil(col("len_m") / lit(chunk_size))) \
        .withColumn("num_chunks", F.when(col("num_chunks") < 1, 1).otherwise(col("num_chunks")))
    
    # explode rows into multiple chunks
    df_exploded = df_calc \
        .withColumn("chunk_id", explode(sequence(lit(0), col("num_chunks") - 1))) #create from 0 to num_chunks-1
    
    # split linestring into chunks using ST_LineSubstring
    # start_fraction = (chunk_id * chunk_size) / len_m
    # end_fraction = ((chunk_id + 1) * chunk_size) / len_m or 1.0 if last chunk
    df_split = df_exploded.withColumn("geometry_chunk", expr(f"""
        ST_LineSubstring(
            temp_geom,
            (chunk_id * {chunk_size}) / len_m,
            CASE 
                WHEN chunk_id = num_chunks - 1 THEN 1.0
                ELSE LEAST(((chunk_id + 1) * {chunk_size}) / len_m, 1.0)
            END
        )
    """))
    
    # recalculate the exact length of each chunk

    logger.info(f"Road network data has {df_split.count()} records after splitting linestrings.")
    df_final_geom = df_split \
        .withColumn("segment_length", expr("ST_LengthSpheroid(geometry_chunk)")) \
        .withColumn("geometry_hex", expr("hex(ST_AsBinary(geometry_chunk))")) \
        .withColumn("azimuth", expr("degrees(ST_Azimuth(ST_StartPoint(geometry_chunk), ST_EndPoint(geometry_chunk)))")) \
        .withColumn("segment_id", concat(col("osmid"), lit("_"), col("chunk_id"))) \
        .drop("temp_geom", "geometry", "geometry_chunk", "len_m", "num_chunks", "chunk_id")
    

    # clean speed limit column
    df_cleaned = df_final_geom.withColumn(
        "speed_limit_clean", 
        clean_speed_limit_udf(col("maxspeed"))
    )
    df_save_final = df_cleaned.dropDuplicates(["segment_id"])
    logger.info(f"Cleaned road network data has {df_save_final.count()} records after cleaning speed limits.")
    df_save = df_save_final.select(
        # segment_id will be auto-generated by PostgreSQL SERIAL
        "segment_id",
        "osmid",
        "u",
        "v",
        "name",
        "oneway",
        "highway",
        col("speed_limit_clean").alias("speed_limit"),
        "lanes",
        "azimuth",
        col("segment_length").alias("length"),
        col("geometry_hex").alias("geometry")
    )
    
    logger.info("5 sample records:")
    # show some sample records
    df_save.select("name", "length", "geometry").show(5)


    # Save child table (road_segments) to PostgreSQL
    try:
        logger.info(f"Saving child table (road_segments) to {db_road_segment_table}")
        df_save.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_road_segment_table) \
            .option("user", db_user) \
            .option("driver", "org.postgresql.Driver") \
            .option("stringtype", "unspecified") \
            .option("password", db_password) \
            .option("batchsize", 1000) \
            .mode("append") \
            .save() # if overwrite will erase the postgis indexes
        logger.info("Child table (road_segments) saved successfully")
    except Exception as e:
        logger.error(f"Error saving child table to Postgres: {e}")
        raise

    df_raw.unpersist()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process road network data with Spark")
    parser.add_argument("--input_path", type=str, required=True, help="Input S3 path for raw road network data")
    parser.add_argument("--db_road_network_table", type=str, required=True, help="Database table name for parent road network (for routing)")
    parser.add_argument("--db_road_segment_table", type=str, required=True, help="Database table name for child road segments (for map matching)")
    parser.add_argument("--chunk_size", type=int, default=100, help="Chunk size in meters for splitting linestrings")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("ProcessRoadNetwork") \
        .getOrCreate()
    SedonaRegistrator.registerAll(spark)

    try:
        process_road_network(
            spark,
            args.input_path,
            args.db_road_network_table,
            args.db_road_segment_table,
            args.chunk_size
        )
    except Exception as e:
        logger.error(f"Error processing road network data: {e}")
        raise
    finally:
        spark.stop()