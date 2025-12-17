#!/bin/bash
set -e

echo "=========================================="
echo "Airflow Initialization Script"
echo "=========================================="

# Wait for PostgreSQL
echo "Waiting for PostgreSQL to be ready..."
until python -c "import psycopg2; psycopg2.connect(host='postgres', port=5432, user='airflow', password='airflow', dbname='airflow')" 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "PostgreSQL is up!"

# Install additional requirements
if [ -f /requirements.txt ]; then
  echo "Installing additional requirements..."
  pip install -r /requirements.txt || echo "Warning: Some packages failed to install"
fi

# Initialize database
echo "Initializing Airflow database..."
airflow db upgrade

# Create admin user (ignore if exists)
echo "Creating admin user..."
airflow users create \
  --username admin \
  --password admin \
  --firstname Hodaya \
  --lastname Pozen \
  --role Admin \
  --email hodaya@example.com || echo "User may already exist"

echo "=========================================="
echo "Initialization complete!"
echo "=========================================="
