"""
spark/utils/spark_factory.py
Shared SparkSession builder used by all Spark jobs.
"""

from pyspark.sql import SparkSession


def build_spark(app_name: str, master: str = "spark://spark-master:7077") -> SparkSession:
    """
    Build a configured SparkSession.

    Packages bundled at submit time:
      - PostgreSQL JDBC driver
      - Cassandra Spark connector (DataStax)
      - Kafka Spark connector
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


PG_URL = "jdbc:postgresql://postgres:5432/ecommerce"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def pg_read(spark: SparkSession, table: str):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPERTIES)


def pg_write(df, table: str, mode: str = "overwrite"):
    (
        df.write
        .jdbc(url=PG_URL, table=table, mode=mode, properties=PG_PROPERTIES)
    )


def cassandra_write(df, keyspace: str, table: str, mode: str = "append"):
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace=keyspace, table=table)
        .mode(mode)
        .save()
    )
