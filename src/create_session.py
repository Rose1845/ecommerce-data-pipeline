

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_spark_session():
    """Create a Spark session for our ETL job"""
    return SparkSession.builder \
        .appName("Ecommerce_ETL_Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
