"""
DAG: dag_sla_alerts — Check for SLA breaches every 30 minutes.
Queries PostgreSQL for tickets that have exceeded their SLA deadline
and logs actionable alerts. Extensible to email/Slack notifications.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

PG_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "itsm_dw"),
    user=os.getenv("POSTGRES_USER", "itsm"),
    password=os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
)

# SLA limits in hours per priority
SLA_LIMITS = {"Very High": 4, "High": 8, "Medium": 24, "Low": 72, "Very Low": 168}

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def check_sla_breaches(**context):
    """
    Find open tickets that have already exceeded their SLA deadline.
    Logs each breach with ticket ID, priority, group, and overdue hours.
    """
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    # SLA hours per priority code: 1=VH 4h, 2=H 8h, 3=M 24h, 4=L 72h, 5=VL 168h
    cur.execute("""
        WITH sla AS (
            SELECT priority_id,
                   CASE code WHEN 1 THEN 4 WHEN 2 THEN 8 WHEN 3 THEN 24
                             WHEN 4 THEN 72 ELSE 168 END AS sla_hours
            FROM dim_priority
        )
        SELECT
            ft.glpi_ticket_id,
            dp.label          AS priority,
            ds.label          AS status,
            dg.name           AS assigned_group,
            ft.date_creation,
            ft.date_creation + (sla.sla_hours * INTERVAL '1 hour') AS sla_deadline,
            EXTRACT(EPOCH FROM (NOW() - (ft.date_creation + sla.sla_hours * INTERVAL '1 hour'))) / 3600
                              AS overdue_hours
        FROM fact_tickets ft
        JOIN dim_priority dp ON ft.priority_id = dp.priority_id
        JOIN sla            ON sla.priority_id = ft.priority_id
        JOIN dim_status   ds ON ft.status_id   = ds.status_id
        LEFT JOIN dim_group dg ON ft.group_id  = dg.group_id
        WHERE ds.code NOT IN (5, 6)
          AND ft.date_creation + (sla.sla_hours * INTERVAL '1 hour') < NOW()
        ORDER BY sla.sla_hours ASC, overdue_hours DESC
        LIMIT 50;
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close()
    conn.close()

    if not rows:
        logger.info("No SLA breaches detected.")
        return

    breaches = [dict(zip(cols, r)) for r in rows]
    critical = [b for b in breaches if b["priority"] == "Very High"]
    high     = [b for b in breaches if b["priority"] == "High"]

    logger.warning("=" * 60)
    logger.warning("SLA BREACH REPORT — %d tickets overdue", len(breaches))
    logger.warning("  Critical (Very High): %d", len(critical))
    logger.warning("  High:                 %d", len(high))
    logger.warning("  Other:                %d", len(breaches) - len(critical) - len(high))
    logger.warning("=" * 60)

    for b in breaches[:20]:
        logger.warning(
            "[%s] Ticket #%s | Group: %s | Overdue: %.1fh | Since: %s",
            b["priority"],
            b["glpi_ticket_id"],
            b["assigned_group"] or "unassigned",
            float(b["overdue_hours"] or 0),
            str(b["date_creation"])[:16],
        )

    # Push summary to XCom for downstream tasks
    context["ti"].xcom_push(key="breach_count",    value=len(breaches))
    context["ti"].xcom_push(key="critical_count",  value=len(critical))


def check_approaching_sla(**context):
    """
    Find open tickets that will breach their SLA within the next 2 hours.
    These are early-warning candidates for escalation.
    """
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    cur.execute("""
        WITH sla AS (
            SELECT priority_id,
                   CASE code WHEN 1 THEN 4 WHEN 2 THEN 8 WHEN 3 THEN 24
                             WHEN 4 THEN 72 ELSE 168 END AS sla_hours
            FROM dim_priority
        )
        SELECT
            ft.glpi_ticket_id,
            dp.label AS priority,
            dg.name  AS assigned_group,
            EXTRACT(EPOCH FROM (
                ft.date_creation + sla.sla_hours * INTERVAL '1 hour' - NOW()
            )) / 3600 AS hours_remaining
        FROM fact_tickets ft
        JOIN dim_priority dp ON ft.priority_id = dp.priority_id
        JOIN sla            ON sla.priority_id = ft.priority_id
        JOIN dim_status   ds ON ft.status_id   = ds.status_id
        LEFT JOIN dim_group dg ON ft.group_id  = dg.group_id
        WHERE ds.code NOT IN (5, 6)
          AND ft.date_creation + (sla.sla_hours * INTERVAL '1 hour')
              BETWEEN NOW() AND NOW() + INTERVAL '2 hours'
        ORDER BY hours_remaining ASC
        LIMIT 30;
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close()
    conn.close()

    approaching = [dict(zip(cols, r)) for r in rows]
    if not approaching:
        logger.info("No tickets approaching SLA breach in next 2h.")
        return

    logger.warning("APPROACHING SLA — %d tickets breach in < 2h:", len(approaching))
    for t in approaching:
        logger.warning(
            "  [%s] Ticket #%s | Group: %s | %.1fh remaining",
            t["priority"],
            t["glpi_ticket_id"],
            t["assigned_group"] or "unassigned",
            float(t["hours_remaining"] or 0),
        )


with DAG(
    dag_id="dag_sla_alerts",
    description="SLA breach detection — runs every 30 minutes",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "sla", "alerts"],
) as dag:

    task_breaches = PythonOperator(
        task_id="check_sla_breaches",
        python_callable=check_sla_breaches,
    )

    task_approaching = PythonOperator(
        task_id="check_approaching_sla",
        python_callable=check_approaching_sla,
    )

    task_breaches >> task_approaching
