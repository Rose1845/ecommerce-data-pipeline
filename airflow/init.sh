#!/bin/bash

set -e

echo "Running Airflow DB migrations..."
airflow db migrate

echo "Creating admin user..."

airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@ecom.local || echo "User already exists"

echo "Initialization complete."