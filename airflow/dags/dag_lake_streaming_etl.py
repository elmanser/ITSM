"""
DAG: dag_lake_streaming_etl
Streaming path of the Lambda architecture:

  MinIO bronze/streaming/  →  clean + enrich  →  MinIO silver/streaming/  →  PostgreSQL DW

Schedule: every 15 minutes (micro-batch)
"""
from __future__ import annotations

import io, json, logging, os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "itsm_minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "itsm_minio_secret_2026")
BUCKET           = os.getenv("MINIO_BUCKET",      "itsm-datalake")
BRONZE_PREFIX    = "bronze/streaming"
SILVER_PREFIX    = "silver/streaming"

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "itsm_dw"),
    user=os.getenv("POSTGRES_USER", "itsm"),
    password=os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
)

PRIORITY_MAP     = {1: "Very High", 2: "High", 3: "Medium", 4: "Low", 5: "Very Low"}
SLA_LIMITS_HOURS = {"Very High": 4, "High": 8, "Medium": 24, "Low": 72, "Very Low": 168}

DEFAULT_ARGS = {
    "owner": "itsm",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def _mc():
    from minio import Minio
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def _parse_dt(val):
    if not val or val in ("NULL", "None", ""):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(str(val), fmt)
        except ValueError:
            continue
    return None


def _clean_record(raw: dict) -> dict | None:
    """Extract and clean one raw Kafka record from the bronze file."""
    ticket = raw.get("data", raw)
    tid = ticket.get("id") or ticket.get("glpi_ticket_id")
    if not tid:
        return None
    priority_code  = int(ticket.get("priority", 3))
    priority_label = PRIORITY_MAP.get(priority_code, "Medium")
    sla_limit      = SLA_LIMITS_HOURS[priority_label]
    date_creation  = _parse_dt(ticket.get("date_creation"))
    date_resolution= _parse_dt(ticket.get("solvedate") or ticket.get("date_resolution"))
    mttr_hours     = None
    if date_creation and date_resolution:
        mttr_hours = round((date_resolution - date_creation).total_seconds() / 3600, 2)
    sla_ok = (mttr_hours <= sla_limit) if mttr_hours is not None else None
    return {
        "glpi_ticket_id":  int(tid),
        "title":           str(ticket.get("name", ""))[:500],
        "description":     str(ticket.get("content", ""))[:2000],
        "priority_code":   priority_code,
        "priority_label":  priority_label,
        "status_code":     int(ticket.get("status", 1)),
        "urgency":         int(ticket.get("urgency", 3)),
        "impact":          int(ticket.get("impact", 3)),
        "date_creation":   date_creation,
        "date_resolution": date_resolution,
        "date_close":      _parse_dt(ticket.get("closedate")),
        "mttr_hours":      mttr_hours,
        "sla_respected":   sla_ok,
        "sla_limit_hours": sla_limit,
        "category_name":   str(ticket.get("category_name") or ticket.get("itilcategories_id") or "unknown").lower().strip(),
        "group_name":      str(ticket.get("group_name") or ticket.get("_groups_id_assign") or "").strip(),
        "source":          "lake_streaming",
    }


def _upsert_ticket(cur, t: dict):
    name = t["category_name"] or "unknown"
    cur.execute("SELECT category_id FROM dim_category WHERE name=%s LIMIT 1", (name,))
    row = cur.fetchone()
    cat_id = row[0] if row else None
    if not cat_id:
        cur.execute("INSERT INTO dim_category (name,itil_type) VALUES (%s,'incident') RETURNING category_id", (name,))
        cat_id = cur.fetchone()[0]

    grp_id = None
    if t["group_name"]:
        cur.execute("SELECT group_id FROM dim_group WHERE name=%s LIMIT 1", (t["group_name"],))
        row = cur.fetchone()
        grp_id = row[0] if row else None
        if not grp_id:
            cur.execute("INSERT INTO dim_group (name,team_type) VALUES (%s,'support') RETURNING group_id", (t["group_name"],))
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
            (SELECT status_id   FROM dim_status   WHERE code=%(status_code)s  LIMIT 1),
            %(category_id)s, %(group_id)s,
            %(urgency)s, %(impact)s,
            %(date_creation)s, %(date_resolution)s, %(date_close)s,
            %(mttr_hours)s, %(sla_respected)s,
            %(source)s, NOW(), NOW()
        )
        ON CONFLICT (glpi_ticket_id) DO UPDATE SET
            title           = EXCLUDED.title,
            date_resolution = EXCLUDED.date_resolution,
            mttr_hours      = EXCLUDED.mttr_hours,
            sla_respected   = EXCLUDED.sla_respected,
            source          = EXCLUDED.source,
            updated_at      = NOW()
    """, {**t, "category_id": cat_id, "group_id": grp_id})


# ── Task 1: list bronze/streaming files ───────────────────────────────────────
def list_bronze_streaming(**ctx):
    mc = _mc()
    objs = list(mc.list_objects(BUCKET, prefix=BRONZE_PREFIX + "/", recursive=True))
    files = [o.object_name for o in objs]
    logger.info("bronze/streaming: %d files to process", len(files))
    ctx["ti"].xcom_push(key="files", value=files)
    return len(files)


# ── Task 2: process bronze → silver + load DW ─────────────────────────────────
def process_streaming(**ctx):
    files = ctx["ti"].xcom_pull(task_ids="list_bronze_streaming", key="files") or []
    if not files:
        logger.info("Nothing to process.")
        return 0

    mc   = _mc()
    conn = psycopg2.connect(**PG)
    processed = []
    total_loaded = 0

    for obj_name in files:
        try:
            resp = mc.get_object(BUCKET, obj_name)
            blob = json.loads(resp.read().decode("utf-8"))
            resp.close()

            # Each file is a dict with "records" list
            raw_records = blob.get("records", []) if isinstance(blob, dict) else blob
            cleaned = []
            for raw in raw_records:
                c = _clean_record(raw)
                if c:
                    cleaned.append(c)

            # Load to DW
            with conn.cursor() as cur:
                for t in cleaned:
                    _upsert_ticket(cur, t)
            conn.commit()
            total_loaded += len(cleaned)

            # Write cleaned to silver/streaming/
            silver_key = obj_name.replace(BRONZE_PREFIX, SILVER_PREFIX, 1)
            silver_payload = json.dumps({
                "zone":         SILVER_PREFIX,
                "processed_at": datetime.utcnow().isoformat(),
                "source_file":  obj_name,
                "count":        len(cleaned),
                "records":      cleaned,
            }, default=str).encode("utf-8")
            mc.put_object(BUCKET, silver_key, io.BytesIO(silver_payload),
                          length=len(silver_payload), content_type="application/json")

            processed.append(obj_name)
            logger.info("Processed %d records from %s → silver", len(cleaned), obj_name)

        except Exception as e:
            logger.error("Failed %s: %s", obj_name, e)
            conn.rollback()

    conn.close()
    ctx["ti"].xcom_push(key="processed", value=processed)
    logger.info("Streaming ETL done — %d tickets loaded to DW", total_loaded)
    return total_loaded


# ── Task 3: remove processed bronze files ─────────────────────────────────────
def archive_bronze_streaming(**ctx):
    files = ctx["ti"].xcom_pull(task_ids="process_streaming", key="processed") or []
    mc = _mc()
    for obj_name in files:
        try:
            mc.remove_object(BUCKET, obj_name)
        except Exception as e:
            logger.warning("Could not remove %s: %s", obj_name, e)
    logger.info("Archived (removed) %d bronze/streaming files", len(files))


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_lake_streaming_etl",
    description="Lambda Streaming: bronze/streaming → silver/streaming → PostgreSQL DW",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "datalake", "streaming", "lambda"],
) as dag:

    t1 = PythonOperator(task_id="list_bronze_streaming", python_callable=list_bronze_streaming)
    t2 = PythonOperator(task_id="process_streaming",     python_callable=process_streaming,
                        execution_timeout=timedelta(minutes=10))
    t3 = PythonOperator(task_id="archive_bronze_streaming", python_callable=archive_bronze_streaming)

    t1 >> t2 >> t3
