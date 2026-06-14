"""
airflow/config/setup_connections.py
Run once after airflow-init to register Airflow connections.

    docker compose exec airflow-scheduler python /opt/airflow/config/setup_connections.py
"""

from airflow.models import Connection
from airflow.utils.session import create_session


def upsert_connection(conn: Connection) -> None:
    with create_session() as session:
        existing = session.query(Connection).filter(
            Connection.conn_id == conn.conn_id
        ).first()
        if existing:
            session.delete(existing)
        session.add(conn)
    print(f"Connection '{conn.conn_id}' registered.")


upsert_connection(Connection(
    conn_id="spark_default",
    conn_type="spark",
    host="spark://spark-master",
    port=7077,
))

upsert_connection(Connection(
    conn_id="postgres_ecommerce",
    conn_type="postgres",
    host="postgres",
    schema="ecommerce",
    login="postgres",
    password="postgres",
    port=5432,
))

print("All connections registered ✓")