# E-Commerce Data Platform

Production-grade data engineering platform built on:
**Apache Spark · PostgreSQL · Cassandra · Kafka · Debezium · Apache Airflow · dbt · Power BI**

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
│   orders.csv   order_items.csv   customers.csv   returns.csv             │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │ spark-submit
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               SPARK JOB 01 — Ingest & Clean                              │
│  • Schema enforcement (no inferSchema)    • Deduplication                │
│  • Date normalisation (2 formats)         • NULL key filtering           │
│  • is_negative_amount flag                • net_amount derivation        │
└───────┬─────────────────────────────────────────────────┬────────────────┘
        │ JDBC overwrite                                  │ s3a:// Parquet
        ▼                                                 ▼
┌───────────────────┐                         ┌─────────────────────┐
│   PostgreSQL      │                         │   MinIO (S3-compat)  │
│   raw schema      │◄── CDC ─── Debezium ───►│   ecom-lake/raw/     │
│   (raw.orders,    │         (pgoutput)       │   ecom-lake/enriched/│
│    raw.customers, │              │           └─────────────────────┘
│    raw.returns,   │              ▼
│    raw.items)     │         ┌─────────────┐
└───────┬───────────┘         │    Kafka     │
        │ dbt run             │  ecom.raw.*  │
        ▼                     └──────┬───────┘
┌───────────────────┐                │ Structured Streaming
│   dbt staging     │                ▼
│   stg_orders      │         ┌─────────────────────┐
│   stg_customers   │         │  SPARK JOB 03        │
│   stg_order_items │         │  CDC Consumer        │
│   stg_returns     │         │  (Kafka → Cassandra) │
└───────┬───────────┘         └─────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               SPARK JOB 02 — Enrich & Aggregate                          │
│  • inner join orders→customers (broadcast hint)                          │
│  • left join →order_items  • anti-join orphaned items                    │
│  • Window: spend rank, 7d rolling, category share                        │
│  • Return analysis: rates, top refunds, anomaly flags                    │
│  • DQ gate before write                                                  │
└───────┬──────────────────────────────────────┬────────────────────────────┘
        │ JDBC                                 │ Cassandra connector
        ▼                                      ▼
┌───────────────────┐                 ┌────────────────────┐
│  PostgreSQL       │                 │   Cassandra         │
│  staging +        │                 │   rolling_metrics   │
│  analytics schema │                 │   category_daily_rev│
└───────┬───────────┘                 │   cdc_events        │
        │ dbt run                     └────────────────────┘
        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               dbt MARTS                                                  │
│   fct_orders  dim_customers  agg_revenue_by_category_month               │
│   agg_return_analysis                                                    │
└───────┬──────────────────────────────────────────────────────────────────┘
        │ dbt test + CSV export
        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               POWER BI                                                   │
│   Folder connector imports CSVs from powerbi/                            │
│   Star schema: fct_orders ─► dim_customers                               │
└──────────────────────────────────────────────────────────────────────────┘

All of the above orchestrated by AIRFLOW (daily @ 02:00 EAT + manual trigger)
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker Desktop | 4.x+ |
| Docker Compose | v2+ |
| Java | 11 (for Spark; provided by bitnami image) |
| Power BI Desktop | Any current version (Windows/Mac) |

RAM: 8 GB minimum for all containers. 16 GB recommended.

---

## Quick Start

```bash
# 1. Clone and enter the project
git clone <your-repo>
cd ecommerce-platform

# 2. Copy your CSVs into the data/ directory
cp /path/to/your/csvs/*.csv ./data/

# 3. Bring up the full stack
docker compose up -d

# 4. Wait for all services to be healthy (~3 minutes)
docker compose ps

# 5. Set up Airflow connections (run once)
docker compose exec airflow-scheduler \
  python /opt/airflow/config/setup_connections.py

# 6. Open the Airflow UI and trigger the DAG
open http://localhost:8082
# Login: admin / admin
# DAG: ecommerce_pipeline → trigger manually

# 7. After the run completes, import CSVs into Power BI
# See powerbi/POWERBI_SETUP.md
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8082 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| Kafka UI | http://localhost:8090 | — |
| MinIO Console | http://localhost:9001 | minio_admin / minio_secret |
| Debezium REST | http://localhost:8083 | — |
| PostgreSQL | localhost:5432 | postgres / postgres |
| Cassandra | localhost:9042 | — |

---

## Project Structure

```
ecommerce-platform/
├── docker-compose.yml
├── requirements.txt
│
├── data/                          # Drop your CSVs here
│   ├── orders.csv
│   ├── order_items.csv
│   ├── customers.csv
│   └── returns.csv
│
├── spark/
│   ├── jobs/
│   │   ├── ingest_and_clean.py     
│   │   ├── enrich_and_aggregate.py 
│   │   └── cdc_consumer.py        
│   └── utils/
│       └── spark_factory.py           
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/              
│       ├── intermediate/         
│       └── marts/                 
│
├── airflow/
│   ├── dags/
│   │   └── ecommerce_pipeline.py  
│   └── config/
│       └── setup_connections.py   
│
├── postgres/init/
│   ├── 01_databases.sql          
│   └── 02_schema.sql              
│
├── cassandra/init/
│   └── 01_keyspace.cql            
│
├── debezium/
│   └── postgres-connector.json    
│
└── powerbi/
    └── POWERBI_SETUP.md          
```

---

## Data Flow in Detail

### Batch Pipeline (Airflow DAG — daily)

| Step | Tool | Output |
|------|------|--------|
| 1. Generate run ID | Airflow Python | XCom run_id |
| 2. Register Debezium connector | Airflow Python → REST | CDC streaming starts |
| 3. Create MinIO bucket | Airflow Python → MinIO | s3a://ecom-lake |
| 4. Ingest & clean CSVs | Spark Job 01 | PostgreSQL raw.* + MinIO Parquet |
| 5. dbt staging | dbt run | stg_* views in PostgreSQL |
| 6. Enrich & aggregate | Spark Job 02 | staging.enriched_orders, analytics.*, Cassandra, powerbi/ CSVs |
| 7. dbt marts | dbt run | fct_orders, dim_customers, agg_* tables |
| 8. dbt tests | dbt test | Assertions on all mart models |
| 9. Notify | Airflow Python | Console log (extendable to Slack/email) |

### Streaming Pipeline (always-on)

Debezium watches `raw.orders`, `raw.customers`, `raw.order_items`, `raw.returns`
via PostgreSQL logical replication → publishes CDC events to Kafka topics
`ecom.raw.*` → Spark Structured Streaming (Job 03) consumes every 30 seconds
and writes to Cassandra `cdc_events` with a 30-day TTL.

---

## Design Decisions

**Why PostgreSQL + Cassandra?**
PostgreSQL is the relational source of truth for batch analytics, dbt transformations,
and Power BI. Cassandra stores high-volume streaming events and pre-aggregated rolling
metrics where write throughput and time-series access patterns matter.

**Why MinIO?**
S3-compatible object store runs locally without an AWS account. The Parquet lake
(partitioned by year/month) serves as a cheap, durable backup and is the natural
migration target if the project moves to AWS S3 or GCS.

**Why dbt on top of Spark?**
Spark handles scale and complex joins that SQL can't; dbt handles documentation,
testing, and the incremental view materialisations that benefit from PostgreSQL's
query planner. The two layers are complementary, not redundant.

**Broadcast hint on customers join**
`customers` is ~200 rows (a dimension table). Broadcasting it to each executor
avoids a shuffle join and produces a `BroadcastHashJoin` in the Spark plan,
measurably faster than a `SortMergeJoin` for this size ratio.

**Idempotency**
Every Spark write uses `mode("overwrite")`. Every dbt model is fully re-runnable.
Re-triggering the DAG for the same date is safe.

---

## Extending to Cloud

| Component | Local | AWS equivalent |
|-----------|-------|----------------|
| MinIO | s3a://ecom-lake | S3 bucket |
| PostgreSQL | docker | RDS PostgreSQL |
| Cassandra | docker | Amazon Keyspaces |
| Kafka | docker | MSK |
| Spark | bitnami/spark | EMR / Glue |
| Airflow | docker | MWAA |

Change `spark.hadoop.fs.s3a.*` configs in `docker-compose.yml` and
`spark_factory.py` to point at real AWS credentials and bucket names.

---

## Known Limitations

- Spark `spark.sql.shuffle.partitions=16` is tuned for local. Set to `2–3×cores`
  on a real cluster.
- Cassandra runs with `replication_factor=1` (single node). Set to 3 for production.
- Debezium connector registration in the DAG retries 5× with 30s delays;
  if Debezium is still starting, the first DAG run may need to be re-triggered.
- The streaming CDC job (Job 03) is not managed by the batch DAG — start it
  separately or add a long-running task in a dedicated DAG.
