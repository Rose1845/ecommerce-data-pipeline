cat > postgres/init/01_databases.sh << 'EOF'
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow OWNER postgres;
    CREATE USER airflow WITH PASSWORD 'password';
    CREATE ROLE airflow WITH LOGIN PASSWORD 'password';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
    CREATE SCHEMA staging;
    CREATE SCHEMA raw;
    CREATE SCHEMA analytics;
    GRANT ALL ON SCHEMA public TO postgres;
    CREATE DATABASE ecommerce OWNER postgres;
    CREATE DATABASE ecommerce_dbt OWNER postgres;
    CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;
    GRANT CONNECT ON DATABASE ecommerce TO debezium;
EOSQL
EOF
chmod +x postgres/init/01_databases.sh
cat postgres/init/01_databases.sh