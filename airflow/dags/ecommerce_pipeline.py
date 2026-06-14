"""
airflow/dags/ecommerce_pipeline.py
====================================
Full e-commerce data platform DAG.

Schedule: daily at 02:00 EAT (UTC+3 → 23:00 UTC)
Trigger:  scheduled + manual (supports both)

Graph
-----
start
  └─ register_debezium_connector
  └─ create_minio_bucket
       └─ spark_ingest_clean            (Job 01 — CSV → PostgreSQL raw + MinIO)
            └─ dbt_staging              (stg_* views)
                 └─ dbt_intermediate    (int_enriched_orders ephemeral)
                      └─ spark_enrich   (Job 02 — enrich + aggregate → PG analytics + Cassandra)
                           └─ dbt_marts (fct_orders, dim_customers, agg_* tables)
                                └─ export_powerbi_csvs
                                     └─ run_dbt_tests
                                          └─ notify_success
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator, Mount

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}
SPARK_MASTER = "spark://spark-master:7077"
SPARK_APPS_DIR = "/opt/spark-apps/jobs"
SPARK_DATA_DIR = "/opt/spark-data"
SPARK_CONF = {
    "spark.sql.shuffle.partitions":      "16",
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    "spark.cassandra.connection.host":   "cassandra",
    "spark.hadoop.fs.s3a.endpoint":      "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":    "minio_admin",
    "spark.hadoop.fs.s3a.secret.key":    "minio_secret",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}
PG_JDBC_PKG = "org.postgresql:postgresql:42.7.3"
CASSANDRA_PKG = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
S3A_PKG = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-sdk-java:1.12.262"
DBT_DIR = "/opt/airflow/dbt"
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL")
MINIO_ALIAS = os.getenv("MINIO_ALIAS")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


def generate_run_id(**context) -> str:
    run_id = f"run_{context['ds_nodash']}_{uuid.uuid4().hex[:8]}"
    context["task_instance"].xcom_push(key="run_id", value=run_id)
    return run_id


def register_debezium(**context) -> None:
    import json
    import os
    import requests

    connector_path = "/opt/airflow/dags/../debezium/postgres-connector.json"
    if not os.path.exists(connector_path):
        connector_path = "/opt/airflow/spark/../debezium/postgres-connector.json"

    connector_name = "ecommerce-postgres-connector"
    check = requests.get(
        f"{DEBEZIUM_URL}/connectors/{connector_name}", timeout=10)

    if check.status_code == 200:
        print(f"Connector '{connector_name}' already registered, skipping.")
        return

    with open(connector_path) as f:
        config = json.load(f)

    resp = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Debezium connector registered: {resp.json()['name']}")


def create_minio_bucket(**context) -> None:
    from minio import Minio
    from minio.error import S3Error
    client = Minio(
        "minio:9000",
        access_key="minio_admin",
        secret_key="minio_secret",
        secure=False,
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' created.")
    else:
        print(f"Bucket '{MINIO_BUCKET}' already exists.")


def notify_success(**context) -> None:
    run_id = context["task_instance"].xcom_pull(
        key="run_id", task_ids="generate_run_id")
    print(
        f"Pipeline run {run_id} completed successfully on {context['ds']}")


with DAG(
    dag_id="ecommerce_pipeline",
    description="E-Commerce full data platform pipeline",
    default_args=default_args,
    schedule="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "production"],
    doc_md=__doc__,
) as dag:

    t_run_id = PythonOperator(
        task_id="generate_run_id",
        python_callable=generate_run_id,
    )

    t_debezium = PythonOperator(
        task_id="register_debezium_connector",
        python_callable=register_debezium,
        retries=5,
        retry_delay=timedelta(seconds=30),
    )

    t_minio = PythonOperator(
        task_id="create_minio_bucket",
        python_callable=create_minio_bucket,
    )

    # t_spark_ingest = SparkSubmitOperator(
    #     task_id="spark_ingest_clean",
    #     application=f"{SPARK_APPS_DIR}/ingest_and_clean.py",
    #     conn_id="spark_default",
    #     application_args=[
    #         "--data",   SPARK_DATA_DIR,
    #         "--run-id", "{{ task_instance.xcom_pull(key='run_id', task_ids='generate_run_id') }}",
    #     ],
    #     conf=SPARK_CONF,
    #     packages=f"{PG_JDBC_PKG},{S3A_PKG}",
    #     verbose=False,
    #     executor_memory="2g",
    #     driver_memory="1g",
    # )

    spark_ingest = DockerOperator(
        task_id="spark_ingest_clean",
        image="spark-ecom:3.5.1",
        api_version="auto",
        auto_remove="success",
        docker_url="unix:///var/run/docker.sock",
        network_mode="ecommerce-data-pipeline_platform",
        mount_tmp_dir=False,
        entrypoint=["/bin/bash", "-c"],
        command=[
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://spark-master:7077 "
                "--py-files /opt/spark-apps/utils/spark_factory.py "  # <-- add this
                "--conf spark.sql.shuffle.partitions=16 "
                "--conf spark.sql.legacy.timeParserPolicy=LEGACY "
                "--conf spark.cassandra.connection.host=cassandra "
                "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                "--conf spark.hadoop.fs.s3a.access.key=minio_admin "
                "--conf spark.hadoop.fs.s3a.secret.key=minio_secret "
                "--conf spark.hadoop.fs.s3a.path.style.access=true "
                "/opt/spark-apps/jobs/ingest_and_clean.py "
                "--data /opt/spark-data "
                "--run-id {{ task_instance.xcom_pull(key='run_id', task_ids='generate_run_id') }}"
            )
        ],
        mounts=[
            Mount(
                source="/home/nyaugenya/dev/personal/ecommerce-data-pipeline/spark",
                target="/opt/spark-apps",
                type="bind",
            ),
            Mount(
                source="/home/nyaugenya/dev/personal/ecommerce-data-pipeline/data",
                target="/opt/spark-data",
                type="bind",
            ),
        ],
    )

    t_dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_DIR} --select staging --target dev"
        ),
    )

    # t_spark_enrich = SparkSubmitOperator(
    #     task_id="spark_enrich_aggregate",
    #     application=f"{SPARK_APPS_DIR}/enrich_and_aggregate.py",
    #     conn_id="spark_default",
    #     application_args=[
    #         "--run-id", "{{ task_instance.xcom_pull(key='run_id', task_ids='generate_run_id') }}",
    #     ],
    #     conf=SPARK_CONF,
    #     packages=f"{PG_JDBC_PKG},{CASSANDRA_PKG},{S3A_PKG}",
    #     verbose=False,
    #     executor_memory="2g",
    #     driver_memory="1g",
    # )
    spark_enrich = DockerOperator(
        task_id="spark_enrich_aggregate",
        image="spark-ecom:3.5.1",
        api_version="auto",
        auto_remove="success",
        docker_url="unix:///var/run/docker.sock",
        network_mode="ecommerce-data-pipeline_platform",
        mount_tmp_dir=False,
        entrypoint=["/bin/bash", "-c"],
        command=[
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://spark-master:7077 "
                "--py-files /opt/spark-apps/utils/spark_factory.py "  # <-- add this
                "--conf spark.sql.shuffle.partitions=16 "
                "--conf spark.cassandra.connection.host=cassandra "
                "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                "--conf spark.hadoop.fs.s3a.access.key=minio_admin "
                "--conf spark.hadoop.fs.s3a.secret.key=minio_secret "
                "--conf spark.hadoop.fs.s3a.path.style.access=true "
                "/opt/spark-apps/jobs/enrich_and_aggregate.py "
                "--run-id {{ task_instance.xcom_pull(key='run_id', task_ids='generate_run_id') }}"
            )
        ],
        mounts=[
            Mount(
                source="/home/nyaugenya/dev/personal/ecommerce-data-pipeline/spark",
                target="/opt/spark-apps",
                type="bind",
            ),
            Mount(
                source="/home/nyaugenya/dev/personal/ecommerce-data-pipeline/data",
                target="/opt/spark-data",
                type="bind",
            ),
        ],
    )

    t_dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_DIR} --select marts --target dev"
        ),
    )

    t_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_DIR} --target dev"
        ),
    )

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    t_run_id >> [t_debezium, t_minio]
    [t_debezium, t_minio] >> spark_ingest
    spark_ingest >> t_dbt_staging
    t_dbt_staging >> spark_enrich
    spark_enrich >> t_dbt_marts
    t_dbt_marts >> t_dbt_tests
    t_dbt_tests >> t_notify
