"""
DAG: dag_ml_retrain — Weekly ML model retraining
Calls POST /retrain on the FastAPI service every Sunday at 02:00.
The API runs the full training pipeline in a background thread and
saves the new model.joblib to the shared Docker volume.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://api:8000")

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def trigger_retrain(**context):
    """Call POST /retrain on the FastAPI service and confirm acknowledgment."""
    url = f"{API_URL}/retrain"
    logger.info("Triggering ML retraining at %s", url)
    try:
        resp = requests.post(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Retrain triggered: %s", data)
        return data
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to trigger retraining: {e}") from e


def check_api_health(**context):
    """Verify the API is reachable before attempting retrain."""
    url = f"{API_URL}/health"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        logger.info("API healthy: %s", resp.json())
    except requests.RequestException as e:
        raise RuntimeError(f"API health check failed: {e}") from e


with DAG(
    dag_id="dag_ml_retrain",
    description="Weekly ML model retraining — every Sunday at 02:00",
    schedule_interval="0 2 * * 0",   # Every Sunday at 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "ml", "retraining"],
) as dag:

    health_check = PythonOperator(
        task_id="check_api_health",
        python_callable=check_api_health,
    )

    retrain = PythonOperator(
        task_id="trigger_model_retrain",
        python_callable=trigger_retrain,
    )

    health_check >> retrain
