"""
DAG: dag_etl — ETL pipeline: DW quality checks, MTTR/SLA recomputation, dim table sync.
Schedule: daily at 02:00
"""
from __future__ import annotations
import logging, os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

PG_CONN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "itsm_dw"),
    "user": os.getenv("POSTGRES_USER", "itsm"),
    "password": os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
}

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def quality_check(**context):
    """Run basic data quality checks on fact_tickets."""
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    checks = []

    cur.execute("SELECT COUNT(*) FROM fact_tickets;")
    total = cur.fetchone()[0]
    checks.append(f"Total tickets: {total}")

    cur.execute("SELECT COUNT(*) FROM fact_tickets WHERE glpi_ticket_id IS NULL;")
    nulls = cur.fetchone()[0]
    checks.append(f"Tickets with NULL glpi_id: {nulls}")

    cur.execute("SELECT COUNT(*) FROM fact_tickets WHERE mttr_hours < 0;")
    negatives = cur.fetchone()[0]
    checks.append(f"Tickets with negative MTTR: {negatives}")

    cur.execute("""
        SELECT priority_id, COUNT(*) FROM fact_tickets
        GROUP BY priority_id ORDER BY priority_id;
    """)
    dist = cur.fetchall()
    checks.append(f"Priority distribution: {dist}")

    conn.close()
    for c in checks:
        logger.info("[QUALITY] %s", c)
    return {"total": total, "null_ids": nulls, "negative_mttr": negatives}


def populate_dim_date(**context):
    """Populate dim_date for the next 365 days if not already present."""
    from datetime import date, timedelta as td
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    today = date.today()
    rows = []
    for i in range(-365, 365):
        d = today + td(days=i)
        rows.append((
            d,
            d.weekday(),
            d.day,
            d.isocalendar()[1],
            d.month,
            (d.month - 1) // 3 + 1,
            d.year,
            d.weekday() >= 5,
        ))
    cur.executemany("""
        INSERT INTO dim_date (full_date, day_of_week, day_of_month, week_of_year,
                              month, quarter, year, is_weekend)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (full_date) DO NOTHING;
    """, rows)
    conn.commit()
    conn.close()
    logger.info("dim_date populated with %d rows", len(rows))


def recompute_sla(**context):
    """Recompute SLA compliance for tickets missing SLA records."""
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("""
        SELECT ft.ticket_id, ft.date_creation, ft.date_resolution, dp.label
        FROM fact_tickets ft
        JOIN dim_priority dp ON ft.priority_id = dp.priority_id
        LEFT JOIN fact_ticket_sla fts ON ft.ticket_id = fts.ticket_id
        WHERE fts.sla_id IS NULL
          AND ft.date_resolution IS NOT NULL;
    """)
    rows = cur.fetchall()
    sla_limits = {"Very High": 4, "High": 8, "Medium": 24, "Low": 72, "Very Low": 168}
    inserted = 0
    for ticket_id, date_creation, date_resolution, priority_label in rows:
        limit = sla_limits.get(priority_label, 24)
        from datetime import timedelta
        deadline = date_creation + timedelta(hours=limit) if date_creation else None
        mttr = round((date_resolution - date_creation).total_seconds() / 3600, 2) if date_creation else None
        delay = None
        sla_ok = None
        if deadline and date_resolution:
            delay = round((date_resolution - deadline).total_seconds() / 3600, 2)
            sla_ok = delay <= 0
        cur.execute("""
            INSERT INTO fact_ticket_sla (ticket_id, sla_deadline, resolution_date, sla_respected, delay_hours, mttr_hours)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """, (ticket_id, deadline, date_resolution, sla_ok, delay, mttr))
        inserted += 1
    conn.commit()
    conn.close()
    logger.info("Recomputed SLA for %d tickets", inserted)


with DAG(
    dag_id="dag_etl",
    description="Daily ETL: quality checks, dim_date population, SLA recomputation",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "etl", "quality"],
) as dag:

    t1 = PythonOperator(task_id="populate_dim_date", python_callable=populate_dim_date)
    t2 = PythonOperator(task_id="quality_check",     python_callable=quality_check)
    t3 = PythonOperator(task_id="recompute_sla",     python_callable=recompute_sla)

    t1 >> t2 >> t3
