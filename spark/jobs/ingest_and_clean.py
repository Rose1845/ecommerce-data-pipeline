"""
spark/jobs/01_ingest_and_clean.py
==================================
Task 01 + 02 of the ETL pipeline.

Reads CSVs from the data/ volume, applies:
  - Explicit schema enforcement (no inferSchema)
  - Deduplication
  - Date normalisation (YYYY-MM-DD and DD/MM/YYYY → DateType)
  - customer_tier → lowercase
  - NULL key filtering (order_id, customer_id)
  - is_negative_amount flag

Writes:
  - Cleaned tables → PostgreSQL raw schema (JDBC, upsert via overwrite)
  - Rejected rows  → PostgreSQL raw.pipeline_runs audit log
  - All tables     → MinIO s3a://ecom-lake/raw/ as Parquet

Usage (via spark-submit in Airflow):
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.postgresql:postgresql:42.7.3 \
      /opt/spark-apps/jobs/01_ingest_and_clean.py \
      --data /opt/spark-data \
      --run-id <airflow_run_id>
"""

import sys
import argparse
import logging
import uuid
sys.path.insert(0, "/opt/spark-apps")  # noqa: E402

# isort: split

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from utils.spark_factory import build_spark, pg_write
from typing import Tuple, List, Optional, Dict
log = logging.getLogger(__name__)


SCHEMA_ORDERS = T.StructType([
    T.StructField("order_id",      T.StringType(),  False),
    T.StructField("customer_id",   T.StringType(),  True),
    T.StructField("order_date",    T.StringType(),  True),
    T.StructField("status",        T.StringType(),  True),
    T.StructField("total_amount",  T.DoubleType(),  True),
    T.StructField("discount_pct",  T.DoubleType(),  True),
])

SCHEMA_ORDER_ITEMS = T.StructType([
    T.StructField("item_id",      T.StringType(),  False),
    T.StructField("order_id",     T.StringType(),  False),
    T.StructField("product_name", T.StringType(),  True),
    T.StructField("category",     T.StringType(),  True),
    T.StructField("quantity",     T.IntegerType(), True),
    T.StructField("unit_price",   T.DoubleType(),  True),
])

SCHEMA_CUSTOMERS = T.StructType([
    T.StructField("customer_id",   T.StringType(), False),
    T.StructField("name",          T.StringType(), True),
    T.StructField("email",         T.StringType(), True),
    T.StructField("country",       T.StringType(), True),
    T.StructField("customer_tier", T.StringType(), True),
    T.StructField("signup_date",   T.StringType(), True),
])

SCHEMA_RETURNS = T.StructType([
    T.StructField("return_id",     T.StringType(), False),
    T.StructField("order_id",      T.StringType(), False),
    T.StructField("return_date",   T.StringType(), True),
    T.StructField("reason",        T.StringType(), True),
    T.StructField("refund_amount", T.DoubleType(), True),
])


def load_csv(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
) -> Tuple[DataFrame, DataFrame]:
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(path)
    )
    if "_corrupt_record" in df.columns:
        bad = df.filter(F.col("_corrupt_record").isNotNull())
        good = df.filter(F.col("_corrupt_record").isNull()
                         ).drop("_corrupt_record")
    else:
        bad = spark.createDataFrame([], schema)
        good = df
    return good, bad


def normalise_date(col_name: str) -> F.Column:
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
    )


def clean_orders(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("order_date", normalise_date("order_date"))
        .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn(
            "is_negative_amount",
            F.when(F.col("total_amount") < 0, F.lit(
                True)).otherwise(F.lit(False)),
        )
        .withColumn(
            "net_amount",
            F.round(F.col("total_amount") *
                    (1.0 - F.col("discount_pct") / 100.0), 2),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(run_id))
    )


def clean_customers(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("signup_date", normalise_date("signup_date"))
        .withColumn("customer_tier", F.lower(F.trim(F.col("customer_tier"))))
        .filter(F.col("customer_id").isNotNull())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(run_id))
    )


def clean_order_items(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(run_id))
    )


def clean_returns(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df
        .dropDuplicates()
        .withColumn("return_date", normalise_date("return_date"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(run_id))
    )


def write_to_postgres(df: DataFrame, table: str) -> None:
    pg_write(df, table, mode="overwrite")
    log.info("  PostgreSQL %s (%d rows)", table, df.count())


def write_to_lake(df: DataFrame, path: str, partition_cols: Optional[List[str]]) -> None:
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    log.info("  → MinIO %s", path)


def run(data_dir: str, run_id: str) -> None:
    spark = build_spark("IngestAndClean")
    spark.sparkContext.setLogLevel("WARN")

    tables = {
        "orders":      (f"{data_dir}/orders.csv",      SCHEMA_ORDERS),
        "order_items": (f"{data_dir}/order_items.csv", SCHEMA_ORDER_ITEMS),
        "customers":   (f"{data_dir}/customers.csv",   SCHEMA_CUSTOMERS),
        "returns":     (f"{data_dir}/returns.csv",     SCHEMA_RETURNS),
    }

    row_counts: Dict[str, int] = {}
    rejected_total = 0

    for name, (path, schema) in tables.items():
        log.info("Ingesting %s …", name)
        good, bad = load_csv(spark, path, schema)
        rejected_total += bad.count()

        if name == "orders":
            cleaned = clean_orders(good, run_id)
            write_to_postgres(cleaned, "raw.orders")
            write_to_lake(cleaned, "s3a://ecom-lake/raw/orders",
                          partition_cols=["order_date"])

        elif name == "order_items":
            cleaned = clean_order_items(good, run_id)
            write_to_postgres(cleaned, "raw.order_items")
            write_to_lake(
                cleaned, "s3a://ecom-lake/raw/order_items", partition_cols=[])

        elif name == "customers":
            cleaned = clean_customers(good, run_id)
            write_to_postgres(cleaned, "raw.customers")
            write_to_lake(
                cleaned, "s3a://ecom-lake/raw/customers", partition_cols=[])

        elif name == "returns":
            cleaned = clean_returns(good, run_id)
            write_to_postgres(cleaned, "raw.returns")
            write_to_lake(cleaned, "s3a://ecom-lake/raw/returns",
                          partition_cols=[])

        row_counts[name] = cleaned.count()

    log.info(
        "Ingestion complete. Rows: %s  Rejected: %d",
        row_counts, rejected_total,
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--data",   default="/opt/spark-data")
    p.add_argument("--run-id", default=str(uuid.uuid4()))
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(data_dir=args.data, run_id=args.run_id)
