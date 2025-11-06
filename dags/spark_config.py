"""
Spark Configuration for S3/MinIO Integration
"""

# S3A Configuration for MinIO
SPARK_S3_CONFIG = {
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'minioadmin',
    'spark.hadoop.fs.s3a.secret.key': 'minioadmin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}

# Sedona configuration for GIS functions
SPARK_SEDONA_CONFIG = {
    **SPARK_S3_CONFIG,  # Include S3 config
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.kryo.registrator': 'org.apache.sedona.core.serde.SedonaKryoRegistrator',
    'spark.sql.extensions': 'org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions',
}

# Required packages for S3 access
SPARK_S3_PACKAGES = 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262'

# Required packages for S3 + PostgreSQL access
SPARK_S3_POSTGRES_PACKAGES = 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.2'

# Required packages for S3 + PostgreSQL + Sedona (GIS)
SPARK_S3_POSTGRES_SEDONA_PACKAGES = 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.2,org.apache.sedona:sedona-spark-3.5_2.12:1.5.1,org.datasyslab:geotools-wrapper:1.5.1-28.2'

# Common Spark defaults
SPARK_CONN_ID = 'spark_default'
SPARK_DEPLOY_MODE = 'client'
