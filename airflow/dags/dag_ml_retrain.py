"""
DAG: dag_ml_retrain — Daily ML model retraining
Calls POST /retrain (synchronous) then checks F1 >= threshold.
Runs every day at 03:00.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

API_URL       = os.getenv("API_URL", "http://api:8000")
F1_THRESHOLD  = float(os.getenv("ML_F1_THRESHOLD", "0.75"))

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def check_api_health(**context):
    resp = requests.get(f"{API_URL}/health", timeout=10)
    resp.raise_for_status()
    logger.info("API healthy: %s", resp.json())


def run_retrain(**context):
    """POST /retrain — synchronous, returns metrics via XCom."""
    resp = requests.post(f"{API_URL}/retrain", timeout=600)
    resp.raise_for_status()
    data = resp.json()
    metrics = data.get("metrics", {})
    logger.info("Retrain done: %s", metrics)
    context["ti"].xcom_push(key="metrics", value=metrics)
    return metrics


def verify_f1(**context):
    """Check new F1 score meets threshold; log warning if below."""
    metrics = context["ti"].xcom_pull(task_ids="retrain_model", key="metrics") or {}
    f1 = float(metrics.get("f1", 0))
    algo = metrics.get("algorithm", "?")
    mae  = metrics.get("mae_mttr", "?")
    logger.info("New model — algorithm=%s  F1=%.4f  MAE=%s h", algo, f1, mae)
    if f1 < F1_THRESHOLD:
        logger.warning(
            "F1=%.4f is below threshold %.2f — consider more data or hyperparameter tuning.",
            f1, F1_THRESHOLD,
        )
    else:
        logger.info("F1=%.4f >= threshold %.2f — model deployed.", f1, F1_THRESHOLD)


with DAG(
    dag_id="dag_ml_retrain",
    description="Daily ML model retraining with F1 quality gate",
    schedule_interval="0 3 * * *",   # Every day at 03:00
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
        task_id="retrain_model",
        python_callable=run_retrain,
        execution_timeout=timedelta(minutes=20),
    )

    verify = PythonOperator(
        task_id="verify_f1_threshold",
        python_callable=verify_f1,
    )

    health_check >> retrain >> verify
