"""
DAG: dag_lake_to_dw — Batch Layer (Lambda Architecture)
Reads raw JSON batches from MinIO Data Lake (bronze layer),
enriches tickets, and upserts them into the PostgreSQL Data Warehouse.

This is the BATCH PATH of the Lambda architecture:
  MinIO bronze/tickets/** → enrich → PostgreSQL fact_tickets
"""
from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "itsm_minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "itsm_minio_secret_2026")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "itsm-data-lake")

PG_CONN = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "itsm_dw"),
    "user":     os.getenv("POSTGRES_USER",     "itsm"),
    "password": os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
}

PRIORITY_MAP      = {1: "Very High", 2: "High", 3: "Medium", 4: "Low", 5: "Very Low"}
SLA_LIMITS_HOURS  = {"Very High": 4, "High": 8, "Medium": 24, "Low": 72, "Very Low": 168}

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── helpers ───────────────────────────────────────────────────────────────────
def _minio():
    from minio import Minio
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def _parse_dt(val):
    if not val or val == "NULL":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def _enrich(ticket: dict) -> dict:
    date_creation   = _parse_dt(ticket.get("date_creation"))
    date_resolution = _parse_dt(ticket.get("solvedate"))
    date_close      = _parse_dt(ticket.get("closedate"))
    priority_code   = ticket.get("priority", 3)
    priority_label  = PRIORITY_MAP.get(priority_code, "Medium")
    sla_limit       = SLA_LIMITS_HOURS[priority_label]
    mttr_hours      = None
    if date_creation and date_resolution:
        mttr_hours = round((date_resolution - date_creation).total_seconds() / 3600, 2)
    sla_respected = (mttr_hours <= sla_limit) if mttr_hours is not None else None
    return {
        "glpi_ticket_id":  ticket.get("id"),
        "title":           ticket.get("name", ""),
        "description":     ticket.get("content", ""),
        "priority_code":   priority_code,
        "priority_label":  priority_label,
        "status_code":     ticket.get("status", 1),
        "urgency":         ticket.get("urgency", 3),
        "impact":          ticket.get("impact", 3),
        "date_creation":   date_creation,
        "date_resolution": date_resolution,
        "date_close":      date_close,
        "mttr_hours":      mttr_hours,
        "sla_respected":   sla_respected,
        "sla_limit_hours": sla_limit,
        "category_name":   ticket.get("itilcategories_id", "unknown"),
        "group_name":      ticket.get("_groups_id_assign", ""),
        "source":          "lake_batch",
    }


def _get_or_create(cur, table, col, val, extra_col=None, extra_val=None):
    id_col = f"{table.split('_')[1]}_id" if "_" in table else f"{table}_id"
    cur.execute(f"SELECT {id_col} FROM {table} WHERE {col} = %s LIMIT 1", (val,))
    row = cur.fetchone()
    if row:
        return row[0]
    if extra_col:
        cur.execute(f"INSERT INTO {table} ({col}, {extra_col}) VALUES (%s, %s) RETURNING {id_col}",
                    (val, extra_val))
    else:
        cur.execute(f"INSERT INTO {table} ({col}) VALUES (%s) RETURNING {id_col}", (val,))
    return cur.fetchone()[0]


def _upsert(cur, enriched: dict):
    name = str(enriched.get("category_name", "unknown")).lower().strip() or "unknown"
    cur.execute("SELECT category_id FROM dim_category WHERE name=%s LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        cat_id = row[0]
    else:
        cur.execute("INSERT INTO dim_category (name,itil_type) VALUES (%s,'incident') RETURNING category_id", (name,))
        cat_id = cur.fetchone()[0]

    grp_name = str(enriched.get("group_name", "")).strip()
    grp_id   = None
    if grp_name:
        cur.execute("SELECT group_id FROM dim_group WHERE name=%s LIMIT 1", (grp_name,))
        row = cur.fetchone()
        grp_id = row[0] if row else None
        if not grp_id:
            cur.execute("INSERT INTO dim_group (name,team_type) VALUES (%s,'support') RETURNING group_id", (grp_name,))
            grp_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO fact_tickets (
            glpi_ticket_id, title, description, priority_id, status_id,
            category_id, group_id, urgency, impact,
            date_creation, date_resolution, date_close,
            mttr_hours, sla_respected, source, ingested_at, updated_at
        ) VALUES (
            %(glpi_ticket_id)s, %(title)s, %(description)s,
            (SELECT priority_id FROM dim_priority WHERE code=%(priority_code)s LIMIT 1),
            (SELECT status_id  FROM dim_status   WHERE code=%(status_code)s  LIMIT 1),
            %(category_id)s, %(group_id)s,
            %(urgency)s, %(impact)s,
            %(date_creation)s, %(date_resolution)s, %(date_close)s,
            %(mttr_hours)s, %(sla_respected)s,
            %(source)s, NOW(), NOW()
        )
        ON CONFLICT (glpi_ticket_id) DO UPDATE SET
            title           = EXCLUDED.title,
            description     = EXCLUDED.description,
            priority_id     = EXCLUDED.priority_id,
            status_id       = EXCLUDED.status_id,
            category_id     = EXCLUDED.category_id,
            date_resolution = EXCLUDED.date_resolution,
            date_close      = EXCLUDED.date_close,
            mttr_hours      = EXCLUDED.mttr_hours,
            sla_respected   = EXCLUDED.sla_respected,
            updated_at      = NOW()
        RETURNING ticket_id;
    """, {**enriched, "category_id": cat_id, "group_id": grp_id})
    return cur.fetchone()


# ── tasks ─────────────────────────────────────────────────────────────────────
def list_new_files(**context):
    """List unprocessed bronze files and push to XCom."""
    mc = _minio()
    prefix = "bronze/tickets/"
    objects = list(mc.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    new_files = [o.object_name for o in objects
                 if not o.object_name.startswith("processed/")]
    logger.info("Found %d new files in bronze layer", len(new_files))
    context["ti"].xcom_push(key="files", value=new_files)
    return len(new_files)


def process_files(**context):
    """Read each bronze file, enrich tickets, upsert to PostgreSQL DW."""
    files = context["ti"].xcom_pull(task_ids="list_new_files", key="files") or []
    if not files:
        logger.info("No new files to process.")
        return 0

    mc   = _minio()
    conn = psycopg2.connect(**PG_CONN)
    total_processed = 0
    processed_files = []

    for obj_name in files:
        try:
            response = mc.get_object(MINIO_BUCKET, obj_name)
            messages = json.loads(response.read().decode("utf-8"))
            response.close()

            count = 0
            with conn.cursor() as cur:
                for msg in messages:
                    ticket = msg.get("data", {}) if isinstance(msg, dict) else {}
                    if not ticket or not ticket.get("id"):
                        continue
                    enriched = _enrich(ticket)
                    _upsert(cur, enriched)
                    count += 1
            conn.commit()
            total_processed += count
            processed_files.append(obj_name)
            logger.info("Processed %d tickets from %s", count, obj_name)
        except Exception as e:
            logger.error("Failed to process %s: %s", obj_name, e)
            conn.rollback()

    conn.close()
    context["ti"].xcom_push(key="processed_files", value=processed_files)
    logger.info("Batch complete — %d tickets from %d files", total_processed, len(processed_files))
    return total_processed


def archive_files(**context):
    """Move processed files to processed/ prefix in MinIO."""
    files = context["ti"].xcom_pull(task_ids="process_files", key="processed_files") or []
    mc    = _minio()
    for obj_name in files:
        dest = obj_name.replace("bronze/", "processed/", 1)
        try:
            mc.copy_object(MINIO_BUCKET, dest,
                           f"/{MINIO_BUCKET}/{obj_name}")
            mc.remove_object(MINIO_BUCKET, obj_name)
        except Exception as e:
            logger.warning("Archive failed for %s: %s", obj_name, e)
    logger.info("Archived %d files", len(files))


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_lake_to_dw",
    description="Batch Layer — MinIO Data Lake → PostgreSQL DW (hourly)",
    schedule_interval="0 * * * *",   # every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "datalake", "batch", "lambda"],
) as dag:

    t_list = PythonOperator(
        task_id="list_new_files",
        python_callable=list_new_files,
    )

    t_process = PythonOperator(
        task_id="process_files",
        python_callable=process_files,
        execution_timeout=timedelta(minutes=30),
    )

    t_archive = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_files,
    )

    t_list >> t_process >> t_archive
