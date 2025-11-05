# dags/sparks_job/engineer_feature_data.py
from ast import expr
from pyspark.sql import SparkSession
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, when, broadcast, udf
    from pyspark.sql.types import StringType, ArrayType

except ImportError as e:
    logger.error("Required modules are not installed. Please install the necessary packages to run this script.")
    raise

# udf to create linestring from links
@udf(returnType=StringType())
def transform_links_to_linestring(links):
    if not links or len(links) == 0:
        return None
    line_strings = []
    for link in links:
        list_points = link['points']
        #remove duplicates within link
        if line_strings and list_points:
            list_points = list_points[1:]
        for point in list_points:
            #selecting latitude and longitude
            latitude = point['lng']
            longitude = point['lat']
            line_strings.append((longitude, latitude))

    return str(line_strings)

build_wkt_udf = udf(transform_links_to_linestring, StringType())

# Main feature engineering function
def engineer_feature_data(spark : SparkSession, input_path: str, 
                            output_path: str,
                            jdbc_url: str, 
                            db_user: str, db_password: str, 
                            db_label_table: str,
                            db_traffic_table: str):
    logger.info(f"Reading data from: {input_path}")

    # Create session
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()

    # Read the cleaned data from S3
    try:
        clean_df = spark.read.parquet(input_path)
    except Exception as e:
        logger.error(f"Error reading cleaned data from S3: {e}")
        raise

    # Extract the street names for label checking
    parsed_df = clean_df.select("street_name").distinct()

    # Load existing street labels from Postgres
    try:
        labels_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_label_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .load()
        
        logger.info("Successfully loaded existing street labels from Postgres.")
    except Exception as e:
        logger.error(f"Error loading existing street labels from Postgres: {e}")
        raise

    # Broadcast the labels_df for performance
    labels_df = broadcast(labels_df)
    # Identify new street names not in Postgres
    new_streets_df = parsed_df.join(
        labels_df,
        on="street_name",
        how="left_anti"
    )

    # Load new street names to Postgres
    try:
        new_streets_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "street_labels") \
            .mode("append") \
            .save()
        
        print("Successfully appended new streets to Postgres.")

    except Exception as e:
        logger.error(f"Error appending new streets to Postgres: {e}")
        raise

    # Reload the new labels including the newly added ones
    try:
        labels_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_label_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .load()
        
        logger.info("Successfully reloaded street labels from Postgres.")
    except Exception as e:
        logger.error(f"Error reloading street labels from Postgres: {e}")
        raise

    # Feature engineering: Calculate congestion level
    feature_df = clean_df.withColumn("congestion_level", 
        when(col("jamFactor") >= 7, lit("High"))
        .when((col("jamFactor") >= 4) & (col("jamFactor") < 7), lit("Medium"))
        .otherwise(lit("Low"))
    )

    # Join with labels to get street IDs
    feature_df = feature_df.join(
        labels_df.select(col("street_id"), col("street_name")),
        on ="street_name",
        how="left"
    )
    # Write the feature engineered data to Parquet
    logger.info(f"Writing feature engineered data to: {output_path}")
    try:
        feature_df.write.mode("overwrite").parquet(output_path)
    except Exception as e:
        logger.error(f"Error writing feature engineered data to S3: {e}")
        raise

    logger.info("Applying udf to create WKT String.")
    # create WKT String from links
    feature_df = feature_df.withColumn("wkt_shape", build_wkt_udf(col("links")))

    logger.info("Create geometry column from WKT String.")
    # Create geometry column
    try:
        feature_df = feature_df.\
            withColumn("geom", expr("ST_GeomFromText(wkt_shape, 4326)"))
    except Exception as e:
        logger.error(f"Error creating geometry column: {e}")
        raise

    # Filter feature_df to keep only necessary columns
    feature_df = feature_df.select(
        "timestamp",
        "street_id",
        "traverse_length",
        "speed",
        "jamFactor",
        "confidence",
        "traversability",
        "freeFlowSpeed",
        "congestion_level",
        "geom"
    )

    logger
    # Write the final feature engineered data to Postgres
    try:
        feature_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_traffic_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .mode("overwrite") \
            .save()
        
        logger.info("Successfully wrote feature engineered data to Postgres.")

    except Exception as e:
        logger.error(f"Error writing feature engineered data to Postgres: {e}")
        raise


# Main entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feature Engineering for Traffic Data")
    parser.add_argument("--input_path", required=True, help="Input path for cleaned data")
    parser.add_argument("--output_path", required=True, help="Output path for feature engineered data")
    parser.add_argument("--jdbc_url", required=True, help="JDBC URL for Postgres")
    parser.add_argument("--db_user", required=True, help="Database user")
    parser.add_argument("--db_password", required=True, help="Database password")
    parser.add_argument("--db_table", required=True, help="Database table name")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()

    try:
        engineer_feature_data(
            spark,
            args.input_path,
            args.output_path,
            args.jdbc_url,
            args.db_user,
            args.db_password,
            args.db_label_table,
            args.db_traffic_table
            )
    finally:
        spark.stop()