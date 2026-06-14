"""
spark/jobs/03_cdc_consumer.py
==============================
Structured Streaming job that consumes Debezium CDC events from Kafka
and writes them to Cassandra cdc_events table.

Run as a long-lived streaming job (separate from the batch DAG):
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
                 com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
      /opt/spark-apps/jobs/03_cdc_consumer.py
"""

import sys
sys.path.insert(0, "/opt/spark-apps")

from pyspark.sql import functions as F
from pyspark.sql import types as T
from utils.spark_factory import build_spark

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPICS = "ecom.raw.orders,ecom.raw.order_items,ecom.raw.customers,ecom.raw.returns"

EVENT_SCHEMA = T.StructType([
    T.StructField("__op",           T.StringType(), True),   
    T.StructField("__source_table", T.StringType(), True),
    T.StructField("order_id",       T.StringType(), True),
    T.StructField("customer_id",    T.StringType(), True),
    T.StructField("return_id",      T.StringType(), True),
    T.StructField("item_id",        T.StringType(), True),
])

def map_op_to_event_type(op: str) -> str:
    return {"c": "INSERT", "u": "UPDATE", "d": "DELETE"}.get(op, "UNKNOWN")

map_op_udf = F.udf(map_op_to_event_type, T.StringType())

def run():
    spark = build_spark("CDCConsumer")
    spark.sparkContext.setLogLevel("WARN")
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw_stream
        .withColumn("value_str", F.col("value").cast(T.StringType()))
        .withColumn("parsed",    F.from_json(F.col("value_str"), EVENT_SCHEMA))
        .withColumn("topic_parts", F.split(F.col("topic"), r"\."))
        .withColumn("table_name", F.element_at(F.col("topic_parts"), -1))
        .withColumn(
            "record_id",
            F.coalesce(
                F.col("parsed.order_id"),
                F.col("parsed.customer_id"),
                F.col("parsed.return_id"),
                F.col("parsed.item_id"),
                F.lit("unknown"),
            ),
        )
        .withColumn("event_type", map_op_udf(F.col("parsed.__op")))
        .withColumn("event_ts",   F.col("timestamp"))
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn("payload",    F.col("value_str"))
        .withColumn("kafka_offset", F.col("offset"))
        .select("table_name", "event_date", "event_ts",
                "event_type", "record_id", "payload", "kafka_offset")
    )

    query = (
        parsed.writeStream
        .foreachBatch(lambda batch_df, batch_id: _write_batch(batch_df, batch_id))
        .option("checkpointLocation", "s3a://ecom-lake/checkpoints/cdc_consumer")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()

def _write_batch(df, batch_id: int) -> None:
    if df.isEmpty():
        return
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace="ecommerce", table="cdc_events")
        .mode("append")
        .save()
    )

if __name__ == "__main__":
    run()
