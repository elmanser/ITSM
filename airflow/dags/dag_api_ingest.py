"""
DAG: dag_api_ingest — GLPI API polling DAG
Triggers the GLPI producer logic directly from Airflow on a schedule.
Schedule: every 5 minutes
"""
from __future__ import annotations
import json, logging, os
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

GLPI_BASE_URL   = os.getenv("GLPI_BASE_URL", "http://glpi:80/apirest.php")
GLPI_APP_TOKEN  = os.getenv("GLPI_APP_TOKEN", "")
GLPI_USER_TOKEN = os.getenv("GLPI_USER_TOKEN", "")
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_RAW", "itsm.tickets.raw")

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def fetch_and_publish(**context):
    """Fetch recent tickets from GLPI and push to Kafka."""
    from kafka import KafkaProducer

    # --- Authenticate ---
    headers = {"Content-Type": "application/json", "App-Token": GLPI_APP_TOKEN,
               "Authorization": f"user_token {GLPI_USER_TOKEN}"}
    resp = requests.get(f"{GLPI_BASE_URL}/initSession", headers=headers, timeout=10)
    resp.raise_for_status()
    session_token = resp.json()["session_token"]

    headers = {"Content-Type": "application/json", "App-Token": GLPI_APP_TOKEN,
               "Session-Token": session_token}

    # --- Fetch tickets (last 100) ---
    params = {"range": "0-99", "order": "DESC", "sort": "id", "expand_dropdowns": "true"}
    tickets_resp = requests.get(f"{GLPI_BASE_URL}/Ticket", headers=headers, params=params, timeout=15)
    tickets = tickets_resp.json() if tickets_resp.status_code == 200 else []

    if not isinstance(tickets, list):
        logger.warning("Unexpected GLPI response: %s", tickets_resp.text[:200])
        tickets = []

    # --- Publish to Kafka ---
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    count = 0
    for ticket in tickets:
        msg = {"event_type": "ticket_ingested", "source": "airflow_dag",
               "ingested_at": datetime.now(timezone.utc).isoformat(), "data": ticket}
        producer.send(KAFKA_TOPIC, key=str(ticket.get("id", "")), value=msg)
        count += 1
    producer.flush()
    producer.close()

    # --- Kill GLPI session ---
    requests.get(f"{GLPI_BASE_URL}/killSession", headers=headers, timeout=5)

    logger.info("DAG published %d tickets to Kafka topic %s", count, KAFKA_TOPIC)
    return count


with DAG(
    dag_id="dag_api_ingest",
    description="Poll GLPI REST API and publish tickets to Kafka",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "ingestion", "glpi", "kafka"],
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_publish_to_kafka",
        python_callable=fetch_and_publish,
    )
