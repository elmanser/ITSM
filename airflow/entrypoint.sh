#!/bin/bash
set -e

echo "==> Initialising Airflow DB…"
airflow db migrate

echo "==> Creating admin user…"
airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin \
    --lastname ITSM \
    --role Admin \
    --email admin@itsm.local 2>/dev/null || true

echo "==> Starting Airflow scheduler in background…"
airflow scheduler &

echo "==> Starting Airflow webserver…"
exec airflow webserver --port 8080
