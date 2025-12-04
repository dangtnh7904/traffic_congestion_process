# dags/sparks_job/engineer_feature_data.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import argparse
from pyspark.sql.functions import col, lit, when, broadcast, udf, expr
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# udf to transform HERE API links to WKT LINESTRING
def transform_links_to_linestring(links):
    if not links or len(links) == 0:
        return None
    
    line_strings = []
    for link in links:
        list_points = link['points']
        
        # Remove duplicates between consecutive links 
        # (first point of next link = last point of previous link)
        if line_strings and list_points:
            list_points = list_points[1:]
        
        for point in list_points:
            # Note: HERE API uses 'lng' and 'lat' keys
            longitude = point['lng']
            latitude = point['lat']
            line_strings.append((longitude, latitude))
    
    # Convert to WKT LINESTRING format
    if not line_strings:
        return None
    
    wkt_coords = ', '.join([f"{lon} {lat}" for lon, lat in line_strings])
    return f"LINESTRING({wkt_coords})"

build_wkt_udf = udf(transform_links_to_linestring, StringType())


# Main Feature Engineering Function

def engineer_feature_data(spark: SparkSession, input_path: str, 
                            output_path: str,
                            jdbc_url: str, 
                            db_user: str, db_password: str, 
                            db_static_road_segments_table: str,
                            db_traffic_status_table: str):
    """
    Main feature engineering function implementing Map Matching algorithm.
    
    Args:
        spark: SparkSession object
        input_path: S3 path to preprocessed traffic data (Parquet)
        output_path: S3 path to save feature-engineered data (Parquet)
        jdbc_url: JDBC connection URL for Postgres
        db_user: Database username
        db_password: Database password
        db_static_road_segments_table: Table name for static road network
        db_traffic_status_table: Table name for traffic events/status
    """
    
    
    logger.info("STARTING FEATURE ENGINEERING WITH MAP MATCHING")

    # read dynamic traffic data and static road network data
    logger.info(f"Reading data sources")
    logger.info(f"Reading preprocessed traffic data from: {input_path}")
    try:
        preprocessed_df = spark.read.parquet(input_path)
        input_count = preprocessed_df.count()
        logger.info(f"   Loaded {input_count} traffic records")
    except Exception as e:
        logger.error(f" Error reading preprocessed data: {e}")
        raise


    # read static road segments from Postgres
    logger.info(f"Reading static road segments from Postgres: {db_static_road_segments_table}")
    try:
        # Use subquery to ensure all columns including SERIAL are loaded
        query = f"""
            (SELECT segment_id, osmid, name, oneway, highway, u, v, 
                    speed_limit, lanes, length, azimuth, 
                    ST_AsBinary(geometry) as geometry
            FROM {db_static_road_segments_table}) AS segments
        """
        
        road_static_segments_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
            
        static_count = road_static_segments_df.count()
        logger.info(f"Loaded {static_count} road segments")
    except Exception as e:
        logger.error(f" Error reading static road segments: {e}")
        raise

    # create geometry objects from dynamic and static data
    logger.info("Converting to Geometry Objects")
    logger.info("  Dynamic Data")
    logger.info(" Creating WKT LINESTRING from API links structure")
    dynamic_df = preprocessed_df.withColumn("wkt_shape", build_wkt_udf(col("links")))
    
    logger.info(" Converting WKT to Geometry using Sedona ST_GeomFromText")
    dynamic_df = dynamic_df.withColumn(
        "dynamic_geom", 
        expr("ST_GeomFromText(wkt_shape, 4326)")
    )
    # create azimuth angle for dynamic data
    logger.info(" Calculating road azimuth angle using ST_Azimuth")
    dynamic_df = dynamic_df.withColumn(
        "dyn_angle", 
        expr("degrees(ST_Azimuth(ST_StartPoint(dynamic_geom), ST_EndPoint(dynamic_geom)))")
    )

    # create geometry for static data
    logger.info("  Static Data")
    logger.info(" Converting Hex String to Binary, then to Geometry using unhex + ST_GeomFromWKB")
    static_df = road_static_segments_df.withColumn(
        "static_geom", 
        expr("ST_GeomFromWKB(geometry)") # unhex because geomfromwkb needs binary input
    )
    static_df = static_df.withColumnRenamed("azimuth", "static_angle")
    
    # Broadcast static data for performance optimization (smaller dataset)
    logger.info(" Broadcasting static road network for join optimization")
    static_df = broadcast(static_df)
    
    logger.info("All geometries and angles calculated")

    # MAP MATCHING STEPS
    logger.info("SPATIAL FILTERING - ST_Intersects Join")
    logger.info("  - Performing spatial join to find intersecting segments")
    

    # ST_Intersects returns true if geometries share any space (touch or overlap)
    logger.info("SPATIAL JOIN - Finding intersections...")
    joined_df = dynamic_df.alias("dyn").join(
        static_df.alias("static"),
        expr("ST_Intersects(dyn.dynamic_geom, static.static_geom)"),
        "inner"
    )

    initial_matches = joined_df.count()
    if initial_matches == 0:
        logger.warning("No spatial intersections found! Check coordinate systems.")
    else:
        logger.info(f"Found {initial_matches} candidates based on ST_Intersects")

    # find the most identical direction between dynamic and static segments
    logger.info("CALCULATING MATCH METRICS...")

    # calculate angle difference and overlap length
    buffer_distance = 0.00015 
    
    joined_df = joined_df.withColumn(
        "angle_diff",
        expr("LEAST(ABS(dyn_angle - static_angle), 360 - ABS(dyn_angle - static_angle))")
    )\
    .withColumn(
        "intersection_geom", 
        # TẠO VÙNG ĐỆM cho Dynamic Geom trước khi giao cắt
        # Điều này biến 'vệt xe' thành 'làn đường rộng', giúp bắt dính static segment
        expr(f"ST_Intersection(ST_Buffer(dyn.dynamic_geom, {buffer_distance}), static.static_geom)")
    )\
    .withColumn(
        "overlap_length", expr("ST_LengthSpheroid(intersection_geom)")
    )

    #log length stats
    overlap_stats = joined_df.select(
        F.min("overlap_length").alias("min_overlap_length"),
        F.max("overlap_length").alias("max_overlap_length"),
        F.avg("overlap_length").alias("avg_overlap_length")
    )
    logger.info("Overlap Length Statistics:")
    overlap_stats.show(10, truncate=False)

    

    # filter based on matching rules
    logger.info("APPLYING MATCHING RULES...")


    filtered_df = joined_df.filter(
        # # Rule 1: aligned directions
        (col("angle_diff") <= 90) 
        & 
        # Rule 2: adequate overlap length
        (col("overlap_length") > 5 )
    )
    
    logger.info(f"Records after applying matching rules: {filtered_df.count()}")


    


    logger.info("AGGREGATION - Grouping by Road Segment")
    
    logger.info("  - Handling overlaps: multiple dynamic -> one static, or vice versa")
    logger.info("  - Strategy: MIN(speed) for safety, MAX(jamFactor) for worst-case")
    
    # Aggregation strategy explanation:
    # - Multiple API traffic segments may map to one OSM road segment (due to GPS inaccuracy)
    # - One API traffic segment may overlap multiple OSM road segments (due to segment splitting)
    # - Solution: Group by segment_id and take conservative estimates
    aggregated_df = filtered_df.groupBy("static.segment_id").agg(
        # Traffic metrics - conservative approach
        F.avg("dyn.speed").alias("speed"),                    # Slower is safer
        F.max("dyn.jamFactor").alias("jamFactor"),            # Report worst congestion
        F.max("dyn.confidence").alias("confidence"),          # Take highest confidence
        
        F.max("dyn.timestamp").alias("timestamp"),
        # Metadata - take first occurrence (all should be same for a segment)
        F.first("dyn.freeFlowSpeed").alias("freeFlowSpeed"),
        F.first("dyn.traversability").alias("traversability"),
        F.first("dyn.traverse_length").alias("traverse_length"),
        
        # Static road information
        F.first("static.name").alias("street_name"),
        F.first("static.length").alias("segment_length"),
        F.first("static.oneway").alias("oneway")
        
    )

    
    
    logger.info("  - Calculating congestion levels from jamFactor")
    logger.info("    * High: jamFactor >= 7")
    logger.info("    * Medium: jamFactor >= 4")
    logger.info("    * Low: jamFactor < 4")
    
    # Calculate congestion level categories based on aggregated jamFactor
    aggregated_df = aggregated_df.withColumn("congestion_level",
        when(col("jamFactor") >= 7, lit("High"))
        .when(col("jamFactor") >= 4, lit("Medium"))
        .otherwise(lit("Low"))
    )
    
    final_count = aggregated_df.count()
    logger.info(f"Complete: {final_count} unique road segments with traffic data")
    

    logger.info("SAVING RESULTS - Parquet and Postgres")
    # Select only necessary columns for storage
    # Note: We don't save geometry - it's already in road_network table (no duplication)
    # Only save segment_id (FK) + traffic metrics (speed, jamFactor, etc.)
    logger.info("  - Preparing final schema (segment_id + traffic metrics only)")
    final_df = aggregated_df.select(
        col("segment_id").cast("string").alias("street_id"),  # FK to road_segments.segment_id (VARCHAR)
        "timestamp",
        #convert from m/s to km/h
        (col("speed") * 3.6).alias("speed"),
        "jamFactor",
        "confidence",
        "traversability",
        "freeFlowSpeed",
        "congestion_level"
    )
    
    # Show sample data for verification
    logger.info("  - Sample final data (first 5 rows):")
    final_df.show(5, truncate=False)
    
    logger.info(f"  - Writing to Parquet: {output_path}")
    try:
        final_df.write.mode("overwrite").parquet(output_path)
        logger.info("     Parquet write successful")
    except Exception as e:
        logger.error(f"Error writing to Parquet: {e}")
        raise
    

    logger.info(f"  - Writing to Postgres table: {db_traffic_status_table}")
    logger.info("    Mode: OVERWRITE (replace all data with latest snapshot)")
    try:
        final_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_traffic_status_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        logger.info("     Postgres write successful (table replaced with latest data)")
    except Exception as e:
        logger.error(f"    Error writing to Postgres: {e}")
        raise

    logger.info("FEATURE ENGINEERING COMPLETE ")
    logger.info(f"Pipeline Summary:")
    logger.info(f"  Input traffic records:     {input_count:,}")
    logger.info(f"  Static road segments:      {static_count:,}")
    logger.info(f"  Spatial intersections:     {initial_matches:,}")
    logger.info(f"  Final unique segments:     {final_count:,}")
    logger.info(f"  Match rate:                {final_count*100//input_count if input_count > 0 else 0}%")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering with Map Matching for Traffic Data"
    )
    parser.add_argument("--input_path", required=True, help="Input S3 path (preprocessed Parquet)")
    parser.add_argument("--output_path", required=True, help="Output S3 path (feature Parquet)")
    parser.add_argument("--jdbc_url", required=True, help="JDBC URL for Postgres")
    parser.add_argument("--db_user", required=True, help="Database username")
    parser.add_argument("--db_password", required=True, help="Database password")
    parser.add_argument("--db_static_road_segments_table", required=True, help="Static road segments table")
    parser.add_argument("--db_traffic_status_table", required=True, help="Traffic status/events table")
    
    args = parser.parse_args()

    # Build Spark session with Sedona extensions
    logger.info("Initializing Spark session with Sedona support")
    spark = SparkSession.builder \
        .appName("FeatureEngineering_MapMatching") \
        .getOrCreate()

    try:
        engineer_feature_data(
            spark,
            args.input_path,
            args.output_path,
            args.jdbc_url,
            args.db_user,
            args.db_password,
            args.db_static_road_segments_table,
            args.db_traffic_status_table
        )
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")
