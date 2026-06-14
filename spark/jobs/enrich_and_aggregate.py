# """
# spark/jobs/02_enrich_and_aggregate.py
# ======================================
# Tasks 03 + 04 + 05 of the ETL pipeline.

# Reads cleaned tables from PostgreSQL raw schema, performs:
#   - Orders → customers → order_items enrichment (inner / left / anti joins)
#   - net_amount derivation
#   - Window aggregations: customer spend ranks, 7-day rolling counts, category revenue share
#   - Return analysis: rates per category/tier, top refund customers, anomaly flags

# Writes:
#   - Enriched orders      → PostgreSQL staging schema + MinIO enriched/ Parquet
#   - Orphaned items       → PostgreSQL raw.orphaned_order_items
#   - Aggregation tables   → PostgreSQL analytics schema
#   - Rolling metrics      → Cassandra rolling_metrics
#   - Category daily rev   → Cassandra category_daily_revenue
#   - Power BI exports     → /opt/airflow/powerbi/ as CSV

# Usage:
#     spark-submit \
#       --master spark://spark-master:7077 \
#       --packages org.postgresql:postgresql:42.7.3,\
#                  com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
#       /opt/spark-apps/jobs/02_enrich_and_aggregate.py \
#       --run-id <airflow_run_id>
# """

# import sys
# import argparse
# import logging
# import uuid
# sys.path.insert(0, "/opt/spark-apps")  # noqa: E402

# # isort: split


# from pyspark.sql import DataFrame, SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql import types as T
# from pyspark.sql.window import Window
# from typing import Tuple, List, Optional, Dict

# from utils.spark_factory import (
#     build_spark,
#     cassandra_write,
#     pg_read,
#     pg_write,
# )

# log = logging.getLogger(__name__)
# POWERBI_DIR = "/opt/airflow/powerbi"


# def enrich(
#     orders: DataFrame,
#     customers: DataFrame,
#     order_items: DataFrame,
# ) -> Tuple[DataFrame, DataFrame]:
#     """
#     Join strategy
#     -------------
#     orders → customers : inner
#         Keeps only orders with a valid customer match.
#         B2: customers is broadcast — eliminates shuffle for this join.
#     enriched → order_items : left
#         Preserves orders without line items.
#     order_items → orders : left_anti
#         Captures orphaned items (order_id has no matching order).
#     """
#     orphaned = order_items.join(
#         orders.select("order_id"), on="order_id", how="left_anti"
#     )
#     valid_items = order_items.join(
#         orders.select("order_id"), on="order_id", how="inner"
#     )

#     enriched = (
#         orders
#         # B2 — broadcast small customers dimension
#         .join(F.broadcast(customers), on="customer_id", how="inner")
#         .join(valid_items, on="order_id", how="left")
#     )

#     return enriched, orphaned


# def dq_gate(enriched: DataFrame) -> None:
#     null_custs = enriched.filter(F.col("customer_id").isNull()).count()
#     if null_custs > 0:
#         raise RuntimeError(
#             f"DQ gate: {null_custs} NULL customer_id rows after enrichment")

#     bad_nets = enriched.filter(
#         (F.col("net_amount") < 0) & (~F.col("is_negative_amount"))
#     ).count()
#     if bad_nets > 0:
#         raise RuntimeError(
#             f"DQ gate: {bad_nets} unflagged negative net_amount rows")

#     log.info("DQ gate passed ✓")


# def customer_spend_ranks(enriched: DataFrame) -> DataFrame:
#     spend = (
#         enriched
#         .groupBy("customer_id", "country", "name")
#         .agg(F.round(F.sum("net_amount"), 2).alias("lifetime_net_spend"))
#     )
#     w = Window.partitionBy("country").orderBy(
#         F.col("lifetime_net_spend").desc())
#     return spend.withColumn("spend_rank_in_country", F.rank().over(w))


# def rolling_7d_order_counts(enriched: DataFrame) -> DataFrame:
#     daily = (
#         enriched
#         .select("customer_id", "order_date")
#         .dropDuplicates(["customer_id", "order_date"])
#         .withColumn("date_as_long", F.col("order_date").cast(T.LongType()))
#     )
#     w = (
#         Window.partitionBy("customer_id")
#         .orderBy("date_as_long")
#         .rangeBetween(-6, 0)
#     )
#     return daily.withColumn("rolling_7d_order_count", F.count("order_date").over(w))


# def category_revenue_share(enriched: DataFrame) -> DataFrame:
#     monthly = (
#         enriched
#         .withColumn("year",  F.year("order_date"))
#         .withColumn("month", F.month("order_date"))
#         .groupBy("year", "month", "category")
#         .agg(F.round(F.sum("net_amount"), 2).alias("category_monthly_revenue"))
#     )
#     w = Window.partitionBy("year", "month")
#     return monthly.withColumn(
#         "revenue_share_pct",
#         F.round(
#             F.col("category_monthly_revenue")
#             / F.sum("category_monthly_revenue").over(w) * 100,
#             2,
#         ),
#     )


# def analyse_returns(enriched: DataFrame, returns: DataFrame) -> Dict[str, DataFrame]:
#     order_snapshot = (
#         enriched
#         .dropDuplicates(["order_id"])
#         .select("order_id", "customer_id", "name", "category",
#                 "customer_tier", "net_amount")
#     )

#     returns_enriched = (
#         returns
#         .join(order_snapshot, on="order_id", how="left")
#         .withColumn(
#             "refund_exceeds_order",
#             F.when(F.col("refund_amount") > F.col("net_amount"), True)
#             .otherwise(False),
#         )
#     )

#     orders_per_cat = (
#         enriched.dropDuplicates(["order_id"])
#         .groupBy("category")
#         .agg(F.countDistinct("order_id").alias("total_orders"))
#     )
#     returns_per_cat = (
#         returns_enriched.groupBy("category")
#         .agg(F.count("return_id").alias("total_returns"))
#     )
#     rate_by_category = (
#         orders_per_cat.join(returns_per_cat, on="category", how="left")
#         .fillna(0, subset=["total_returns"])
#         .withColumn(
#             "return_rate_pct",
#             F.round(F.col("total_returns") / F.col("total_orders") * 100, 2),
#         )
#     )
#     orders_per_tier = (
#         enriched.dropDuplicates(["order_id"])
#         .groupBy("customer_tier")
#         .agg(F.countDistinct("order_id").alias("total_orders"))
#     )
#     returns_per_tier = (
#         returns_enriched.groupBy("customer_tier")
#         .agg(F.count("return_id").alias("total_returns"))
#     )
#     rate_by_tier = (
#         orders_per_tier.join(returns_per_tier, on="customer_tier", how="left")
#         .fillna(0, subset=["total_returns"])
#         .withColumn(
#             "return_rate_pct",
#             F.round(F.col("total_returns") / F.col("total_orders") * 100, 2),
#         )
#     )

#     top_refund_customers = (
#         returns_enriched
#         .groupBy("customer_id", "name")
#         .agg(F.round(F.sum("refund_amount"), 2).alias("total_refund_amount"))
#         .orderBy(F.col("total_refund_amount").desc())
#         .limit(10)
#     )

#     return {
#         "returns_enriched":     returns_enriched,
#         "rate_by_category":     rate_by_category,
#         "rate_by_tier":         rate_by_tier,
#         "top_refund_customers": top_refund_customers,
#     }


# def write_rolling_to_cassandra(rolling: DataFrame) -> None:
#     cassandra_df = (
#         rolling
#         .withColumn("last_updated", F.current_timestamp())
#         .withColumnRenamed("rolling_7d_order_count", "rolling_7d_orders")
#         .withColumn("rolling_7d_spend", F.lit(0.0).cast(T.DecimalType(12, 2)))
#         .select("customer_id", "order_date", "rolling_7d_orders",
#                 "rolling_7d_spend", "last_updated")
#         .withColumnRenamed("order_date", "metric_date")
#     )
#     cassandra_write(cassandra_df, keyspace="ecommerce",
#                     table="rolling_metrics")


# def write_category_rev_to_cassandra(cat_rev: DataFrame) -> None:
#     cassandra_df = (
#         cat_rev
#         .withColumn(
#             "revenue_date",
#             F.to_date(F.concat_ws("-", F.col("year"),
#                       F.col("month"), F.lit("01"))),
#         )
#         .withColumnRenamed("category_monthly_revenue", "total_revenue")
#         .withColumn("order_count", F.lit(0).cast(T.IntegerType()))
#         .select("category", "revenue_date", "total_revenue", "order_count")
#     )
#     cassandra_write(cassandra_df, keyspace="ecommerce",
#                     table="category_daily_revenue")


# def export_for_powerbi(dfs: Dict[str, DataFrame]) -> None:
#     """Write summary CSVs that Power BI can import directly."""
#     import os
#     os.makedirs(POWERBI_DIR, exist_ok=True)
#     for name, df in dfs.items():
#         path = f"{POWERBI_DIR}/{name}"
#         df.coalesce(1).write.mode("overwrite").option(
#             "header", "true").csv(path)
#         log.info("Power BI export: %s", path)


# def run(run_id: str) -> None:
#     spark = build_spark("EnrichAndAggregate")
#     spark.sparkContext.setLogLevel("WARN")

#     log.info("Reading cleaned tables from PostgreSQL …")
#     orders = pg_read(spark, "raw.orders")
#     order_items = pg_read(spark, "raw.order_items")
#     customers = pg_read(spark, "raw.customers")
#     returns = pg_read(spark, "raw.returns")

#     enriched, orphaned = enrich(orders, customers, order_items)
#     dq_gate(enriched)

#     pg_write(orphaned.withColumn("pipeline_run_id", F.lit(run_id)),
#              "raw.orphaned_order_items", mode="append")

#     pg_write(enriched, "staging.enriched_orders", mode="overwrite")
#     (
#         enriched
#         .withColumn("order_year",  F.year("order_date"))
#         .withColumn("order_month", F.month("order_date"))
#         .write.mode("overwrite")
#         .partitionBy("order_year", "order_month")
#         .parquet("s3a://ecom-lake/enriched/orders")
#     )
#     spend_ranks = customer_spend_ranks(enriched)
#     rolling = rolling_7d_order_counts(enriched)
#     cat_share = category_revenue_share(enriched)

#     pg_write(spend_ranks, "analytics.customer_spend_ranks", mode="overwrite")
#     pg_write(rolling,     "analytics.rolling_order_counts", mode="overwrite")
#     pg_write(cat_share,   "analytics.category_revenue_share", mode="overwrite")

#     write_rolling_to_cassandra(rolling)
#     write_category_rev_to_cassandra(cat_share)

#     return_results = analyse_returns(enriched, returns)
#     pg_write(return_results["returns_enriched"],
#              "analytics.returns_enriched",     mode="overwrite")
#     pg_write(return_results["rate_by_category"],
#              "analytics.return_rate_category", mode="overwrite")
#     pg_write(return_results["rate_by_tier"],
#              "analytics.return_rate_tier",     mode="overwrite")
#     pg_write(return_results["top_refund_customers"],
#              "analytics.top_refund_customers", mode="overwrite")

#     export_for_powerbi({
#         "enriched_orders":        enriched,
#         "customer_spend_ranks":   spend_ranks,
#         "category_revenue_share": cat_share,
#         "return_rate_category":   return_results["rate_by_category"],
#         "return_rate_tier":       return_results["rate_by_tier"],
#         "top_refund_customers":   return_results["top_refund_customers"],
#     })

#     log.info("Enrichment & aggregation complete ✓")
#     spark.stop()


# def parse_args() -> argparse.Namespace:
#     p = argparse.ArgumentParser()
#     p.add_argument("--run-id", default=str(uuid.uuid4()))
#     return p.parse_args()


# if __name__ == "__main__":
#     args = parse_args()
#     run(run_id=args.run_id)
"""
spark/jobs/02_enrich_and_aggregate.py
"""

import sys
import argparse
import logging
import uuid
sys.path.insert(0, "/opt/spark-apps")  # noqa: E402

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from typing import Tuple, Dict

from utils.spark_factory import (
    build_spark,
    cassandra_write,
    pg_read,
    pg_write,
)

log = logging.getLogger(__name__)
POWERBI_DIR = "/opt/airflow/powerbi"


def enrich(
    orders: DataFrame,
    customers: DataFrame,
    order_items: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """
    Renames duplicate columns (ingested_at, pipeline_run_id) on customers
    and order_items before joining to avoid the PSQLException on write.
    """
    orphaned = order_items.join(
        orders.select("order_id"), on="order_id", how="left_anti"
    )

    # Rename columns that clash with orders
    customers_clean = customers.select(
        "customer_id", "name", "email", "country", "customer_tier", "signup_date",
        F.col("ingested_at").alias("customer_ingested_at"),
        F.col("pipeline_run_id").alias("customer_run_id"),
    )

    items_clean = order_items.join(
        orders.select("order_id"), on="order_id", how="inner"
    ).select(
        "item_id",
        F.col("order_id").alias("item_order_id"),
        "product_name", "category", "quantity", "unit_price",
        F.col("ingested_at").alias("item_ingested_at"),
        F.col("pipeline_run_id").alias("item_run_id"),
    )

    enriched = (
        orders
        .join(F.broadcast(customers_clean), on="customer_id", how="inner")
        .join(items_clean, orders["order_id"] == items_clean["item_order_id"], how="left")
        .drop("item_order_id")
    )

    return enriched, orphaned


def dq_gate(enriched: DataFrame) -> None:
    null_custs = enriched.filter(F.col("customer_id").isNull()).count()
    if null_custs > 0:
        raise RuntimeError(
            f"DQ gate: {null_custs} NULL customer_id rows after enrichment")
    bad_nets = enriched.filter(
        (F.col("net_amount") < 0) & (~F.col("is_negative_amount"))
    ).count()
    if bad_nets > 0:
        raise RuntimeError(
            f"DQ gate: {bad_nets} unflagged negative net_amount rows")
    log.info("DQ gate passed ✓")


def customer_spend_ranks(enriched: DataFrame) -> DataFrame:
    spend = (
        enriched
        .groupBy("customer_id", "country", "name")
        .agg(F.round(F.sum("net_amount"), 2).alias("lifetime_net_spend"))
    )
    w = Window.partitionBy("country").orderBy(
        F.col("lifetime_net_spend").desc())
    return spend.withColumn("spend_rank_in_country", F.rank().over(w))


def rolling_7d_order_counts(enriched: DataFrame) -> DataFrame:
    daily = (
        enriched
        .select("customer_id", "order_date")
        .dropDuplicates(["customer_id", "order_date"])
        .withColumn("date_as_long", F.col("order_date").cast(T.LongType()))
    )
    w = (
        Window.partitionBy("customer_id")
        .orderBy("date_as_long")
        .rangeBetween(-6, 0)
    )
    return daily.withColumn("rolling_7d_order_count", F.count("order_date").over(w))


def category_revenue_share(enriched: DataFrame) -> DataFrame:
    monthly = (
        enriched
        .withColumn("year",  F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .groupBy("year", "month", "category")
        .agg(F.round(F.sum("net_amount"), 2).alias("category_monthly_revenue"))
    )
    w = Window.partitionBy("year", "month")
    return monthly.withColumn(
        "revenue_share_pct",
        F.round(
            F.col("category_monthly_revenue")
            / F.sum("category_monthly_revenue").over(w) * 100, 2,
        ),
    )


def analyse_returns(enriched: DataFrame, returns: DataFrame) -> Dict[str, DataFrame]:
    order_snapshot = (
        enriched.dropDuplicates(["order_id"])
        .select("order_id", "customer_id", "name", "category", "customer_tier", "net_amount")
    )

    returns_enriched = (
        returns
        .join(order_snapshot, on="order_id", how="left")
        .withColumn(
            "refund_exceeds_order",
            F.when(F.col("refund_amount") > F.col(
                "net_amount"), True).otherwise(False),
        )
    )

    orders_per_cat = (
        enriched.dropDuplicates(["order_id"])
        .groupBy("category")
        .agg(F.countDistinct("order_id").alias("total_orders"))
    )
    returns_per_cat = (
        returns_enriched.groupBy("category")
        .agg(F.count("return_id").alias("total_returns"))
    )
    rate_by_category = (
        orders_per_cat.join(returns_per_cat, on="category", how="left")
        .fillna(0, subset=["total_returns"])
        .withColumn("return_rate_pct",
                    F.round(F.col("total_returns") / F.col("total_orders") * 100, 2))
    )

    orders_per_tier = (
        enriched.dropDuplicates(["order_id"])
        .groupBy("customer_tier")
        .agg(F.countDistinct("order_id").alias("total_orders"))
    )
    returns_per_tier = (
        returns_enriched.groupBy("customer_tier")
        .agg(F.count("return_id").alias("total_returns"))
    )
    rate_by_tier = (
        orders_per_tier.join(returns_per_tier, on="customer_tier", how="left")
        .fillna(0, subset=["total_returns"])
        .withColumn("return_rate_pct",
                    F.round(F.col("total_returns") / F.col("total_orders") * 100, 2))
    )

    top_refund_customers = (
        returns_enriched
        .groupBy("customer_id", "name")
        .agg(F.round(F.sum("refund_amount"), 2).alias("total_refund_amount"))
        .orderBy(F.col("total_refund_amount").desc())
        .limit(10)
    )

    return {
        "returns_enriched":     returns_enriched,
        "rate_by_category":     rate_by_category,
        "rate_by_tier":         rate_by_tier,
        "top_refund_customers": top_refund_customers,
    }


def write_rolling_to_cassandra(rolling: DataFrame) -> None:
    cassandra_df = (
        rolling
        .withColumn("last_updated", F.current_timestamp())
        .withColumnRenamed("rolling_7d_order_count", "rolling_7d_orders")
        .withColumn("rolling_7d_spend", F.lit(0.0).cast(T.DecimalType(12, 2)))
        .select("customer_id", "order_date", "rolling_7d_orders", "rolling_7d_spend", "last_updated")
        .withColumnRenamed("order_date", "metric_date")
    )
    cassandra_write(cassandra_df, keyspace="ecommerce",
                    table="rolling_metrics")


def write_category_rev_to_cassandra(cat_rev: DataFrame) -> None:
    cassandra_df = (
        cat_rev
        .withColumn("revenue_date",
                    F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01"))))
        .withColumnRenamed("category_monthly_revenue", "total_revenue")
        .withColumn("order_count", F.lit(0).cast(T.IntegerType()))
        .select("category", "revenue_date", "total_revenue", "order_count")
    )
    cassandra_write(cassandra_df, keyspace="ecommerce",
                    table="category_daily_revenue")


def export_for_powerbi(dfs: Dict[str, DataFrame]) -> None:
    import os
    os.makedirs(POWERBI_DIR, exist_ok=True)
    for name, df in dfs.items():
        path = f"{POWERBI_DIR}/{name}"
        df.coalesce(1).write.mode("overwrite").option(
            "header", "true").csv(path)
        log.info("Power BI export: %s", path)


def run(run_id: str) -> None:
    spark = build_spark("EnrichAndAggregate")
    spark.sparkContext.setLogLevel("WARN")

    orders = pg_read(spark, "raw.orders")
    order_items = pg_read(spark, "raw.order_items")
    customers = pg_read(spark, "raw.customers")
    returns = pg_read(spark, "raw.returns")

    enriched, orphaned = enrich(orders, customers, order_items)
    dq_gate(enriched)

    pg_write(orphaned.withColumn("pipeline_run_id", F.lit(run_id)),
             "raw.orphaned_order_items", mode="append")

    # Explicit column selection — guarantees no duplicate column names on write
    enriched_clean = enriched.select(
        "order_id", "customer_id", "order_date", "status",
        "total_amount", "discount_pct", "is_negative_amount", "net_amount",
        "ingested_at", "pipeline_run_id",
        "name", "email", "country", "customer_tier", "signup_date",
        "item_id", "product_name", "category", "quantity", "unit_price",
    )

    pg_write(enriched_clean, "staging.enriched_orders", mode="overwrite")

    (
        enriched_clean
        .withColumn("order_year",  F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .write.mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet("s3a://ecom-lake/enriched/orders")
    )

    spend_ranks = customer_spend_ranks(enriched_clean)
    rolling = rolling_7d_order_counts(enriched_clean)
    cat_share = category_revenue_share(enriched_clean)

    pg_write(spend_ranks, "analytics.customer_spend_ranks",   mode="overwrite")
    pg_write(rolling,     "analytics.rolling_order_counts",   mode="overwrite")
    pg_write(cat_share,   "analytics.category_revenue_share", mode="overwrite")

    write_rolling_to_cassandra(rolling)
    write_category_rev_to_cassandra(cat_share)

    return_results = analyse_returns(enriched_clean, returns)
    pg_write(return_results["returns_enriched"],
             "analytics.returns_enriched",     mode="overwrite")
    pg_write(return_results["rate_by_category"],
             "analytics.return_rate_category", mode="overwrite")
    pg_write(return_results["rate_by_tier"],
             "analytics.return_rate_tier",     mode="overwrite")
    pg_write(return_results["top_refund_customers"],
             "analytics.top_refund_customers", mode="overwrite")

    export_for_powerbi({
        "enriched_orders":        enriched_clean,
        "customer_spend_ranks":   spend_ranks,
        "category_revenue_share": cat_share,
        "return_rate_category":   return_results["rate_by_category"],
        "return_rate_tier":       return_results["rate_by_tier"],
        "top_refund_customers":   return_results["top_refund_customers"],
    })

    log.info("Enrichment & aggregation complete ✓")
    spark.stop()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default=str(uuid.uuid4()))
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(run_id=args.run_id)
