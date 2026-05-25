"""
DAG: dag_lake_batch_etl
Batch path of the Lambda architecture:

  Step 1 — INGEST:  PostgreSQL DW (source CSV data already loaded) → MinIO bronze/batch/
  Step 2 — PROCESS: MinIO bronze/batch/ → clean + enrich → MinIO silver/batch/
  Step 3 — LOAD:    MinIO silver/batch/ → PostgreSQL DW (upsert with source=lake_batch)

This demonstrates the full Lambda batch layer:
  raw data lands in the Data Lake first, then flows to the DW via ETL.

Schedule: daily at 03:00
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
BRONZE_PREFIX    = "bronze/batch"
SILVER_PREFIX    = "silver/batch"

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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _mc():
    from minio import Minio
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def _parse_dt(val):
    if not val or str(val) in ("NULL", "None", ""):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(val), fmt)
        except ValueError:
            continue
    return None


# ── Task 1: Extract raw tickets from DW and store in MinIO bronze/batch/ ──────
def ingest_to_bronze(**ctx):
    """
    Pull raw ticket data from PostgreSQL and write it as raw JSON
    to MinIO bronze/batch/ — simulating a CSV/source ingestion to the Data Lake.
    In production, this step reads directly from CSV files or source systems.
    """
    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()

    today     = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    cur.execute("""
        SELECT
            ft.glpi_ticket_id,
            ft.title,
            ft.description,
            dp.code       AS priority_code,
            dp.label      AS priority_label,
            ds.code       AS status_code,
            dc.name       AS category_name,
            dg.name       AS group_name,
            ft.urgency,
            ft.impact,
            ft.date_creation,
            ft.date_resolution,
            ft.date_close,
            ft.mttr_hours,
            ft.sla_respected,
            ft.source
        FROM fact_tickets ft
        LEFT JOIN dim_priority dp ON dp.priority_id = ft.priority_id
        LEFT JOIN dim_status   ds ON ds.status_id   = ft.status_id
        LEFT JOIN dim_category dc ON dc.category_id = ft.category_id
        LEFT JOIN dim_group    dg ON dg.group_id    = ft.group_id
        WHERE ft.date_creation >= %s
          AND ft.date_creation <  %s
    """, (yesterday, today))

    cols    = [d[0] for d in cur.description]
    records = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not records:
        logger.info("No records for %s — nothing to ingest to bronze/batch", yesterday)
        ctx["ti"].xcom_push(key="bronze_key", value=None)
        return 0

    ts  = datetime.utcnow()
    key = (f"{BRONZE_PREFIX}/year={ts.year}/month={ts.month:02d}/"
           f"day={ts.day:02d}/raw_batch_{ts.strftime('%H%M%S')}.json")

    payload = json.dumps({
        "zone":        BRONZE_PREFIX,
        "ingested_at": ts.isoformat(),
        "source_date": str(yesterday),
        "count":       len(records),
        "records":     records,
    }, default=str).encode("utf-8")

    mc = _mc()
    mc.put_object(BUCKET, key, io.BytesIO(payload),
                  length=len(payload), content_type="application/json")

    logger.info("bronze/batch ← %d raw records → %s", len(records), key)
    ctx["ti"].xcom_push(key="bronze_key", value=key)
    return len(records)


# ── Task 2: Process bronze/batch → silver/batch ───────────────────────────────
def process_bronze_to_silver(**ctx):
    """Clean, validate, and enrich raw batch records. Write to silver/batch/."""
    bronze_key = ctx["ti"].xcom_pull(task_ids="ingest_to_bronze", key="bronze_key")
    if not bronze_key:
        logger.info("No bronze file to process.")
        ctx["ti"].xcom_push(key="silver_key", value=None)
        return 0

    mc   = _mc()
    resp = mc.get_object(BUCKET, bronze_key)
    blob = json.loads(resp.read().decode("utf-8"))
    resp.close()

    raw_records = blob.get("records", [])
    cleaned = []

    for r in raw_records:
        # Quality checks
        if not r.get("glpi_ticket_id"):
            continue
        if not r.get("date_creation"):
            continue

        priority_code = int(r.get("priority_code") or 3)
        priority_label = r.get("priority_label") or PRIORITY_MAP.get(priority_code, "Medium")
        sla_limit = SLA_LIMITS_HOURS.get(priority_label, 24)

        # Recompute MTTR for data quality
        date_creation  = _parse_dt(r.get("date_creation"))
        date_resolution= _parse_dt(r.get("date_resolution"))
        mttr_hours     = None
        if date_creation and date_resolution:
            mttr_hours = round((date_resolution - date_creation).total_seconds() / 3600, 2)
        sla_ok = (mttr_hours <= sla_limit) if mttr_hours is not None else None

        cleaned.append({
            "glpi_ticket_id":  int(r["glpi_ticket_id"]),
            "title":           str(r.get("title", ""))[:500],
            "description":     str(r.get("description", ""))[:2000],
            "priority_code":   priority_code,
            "priority_label":  priority_label,
            "status_code":     int(r.get("status_code") or 1),
            "urgency":         int(r.get("urgency") or 3),
            "impact":          int(r.get("impact") or 3),
            "date_creation":   str(r.get("date_creation") or ""),
            "date_resolution": str(r.get("date_resolution") or ""),
            "date_close":      str(r.get("date_close") or ""),
            "mttr_hours":      mttr_hours,
            "sla_respected":   sla_ok,
            "sla_limit_hours": sla_limit,
            "category_name":   str(r.get("category_name") or "unknown").lower().strip(),
            "group_name":      str(r.get("group_name") or "").strip(),
            "dq_valid":        True,
        })

    ts          = datetime.utcnow()
    silver_key  = bronze_key.replace(BRONZE_PREFIX, SILVER_PREFIX, 1).replace("raw_batch_", "clean_batch_")
    silver_data = json.dumps({
        "zone":          SILVER_PREFIX,
        "processed_at":  ts.isoformat(),
        "source_bronze": bronze_key,
        "count":         len(cleaned),
        "records":       cleaned,
    }, default=str).encode("utf-8")

    mc.put_object(BUCKET, silver_key, io.BytesIO(silver_data),
                  length=len(silver_data), content_type="application/json")

    logger.info("silver/batch ← %d clean records → %s", len(cleaned), silver_key)
    ctx["ti"].xcom_push(key="silver_key", value=silver_key)
    ctx["ti"].xcom_push(key="count",      value=len(cleaned))
    return len(cleaned)


# ── Task 3: Load silver/batch into PostgreSQL DW ──────────────────────────────
def load_silver_to_dw(**ctx):
    """Read cleaned records from silver/batch and upsert into PostgreSQL DW."""
    silver_key = ctx["ti"].xcom_pull(task_ids="process_bronze_to_silver", key="silver_key")
    if not silver_key:
        logger.info("No silver file to load.")
        return 0

    mc   = _mc()
    resp = mc.get_object(BUCKET, silver_key)
    blob = json.loads(resp.read().decode("utf-8"))
    resp.close()

    records = blob.get("records", [])
    if not records:
        return 0

    conn = psycopg2.connect(**PG)
    loaded = 0

    for r in records:
        try:
            with conn.cursor() as cur:
                name = r["category_name"] or "unknown"
                cur.execute("SELECT category_id FROM dim_category WHERE name=%s LIMIT 1", (name,))
                row = cur.fetchone()
                cat_id = row[0] if row else None
                if not cat_id:
                    cur.execute("INSERT INTO dim_category (name,itil_type) VALUES (%s,'incident') RETURNING category_id", (name,))
                    cat_id = cur.fetchone()[0]

                grp_id = None
                if r["group_name"]:
                    cur.execute("SELECT group_id FROM dim_group WHERE name=%s LIMIT 1", (r["group_name"],))
                    row = cur.fetchone()
                    grp_id = row[0] if row else None
                    if not grp_id:
                        cur.execute("INSERT INTO dim_group (name,team_type) VALUES (%s,'support') RETURNING group_id", (r["group_name"],))
                        grp_id = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO fact_tickets (
                        glpi_ticket_id, title, description, priority_id, status_id,
                        category_id, group_id, urgency, impact,
                        date_creation, date_resolution, date_close,
                        mttr_hours, sla_respected, source, ingested_at, updated_at
                    ) VALUES (
                        %(tid)s, %(title)s, %(description)s,
                        (SELECT priority_id FROM dim_priority WHERE code=%(pcode)s LIMIT 1),
                        (SELECT status_id   FROM dim_status   WHERE code=%(scode)s  LIMIT 1),
                        %(cat_id)s, %(grp_id)s,
                        %(urgency)s, %(impact)s,
                        %(date_creation)s::timestamp, %(date_resolution)s::timestamp,
                        %(date_close)s::timestamp,
                        %(mttr_hours)s, %(sla_respected)s,
                        'lake_batch', NOW(), NOW()
                    )
                    ON CONFLICT (glpi_ticket_id) DO UPDATE SET
                        mttr_hours      = EXCLUDED.mttr_hours,
                        sla_respected   = EXCLUDED.sla_respected,
                        date_resolution = EXCLUDED.date_resolution,
                        source          = EXCLUDED.source,
                        updated_at      = NOW()
                """, {
                    "tid":             r["glpi_ticket_id"],
                    "title":           r["title"],
                    "description":     r["description"],
                    "pcode":           r["priority_code"],
                    "scode":           r["status_code"],
                    "cat_id":          cat_id,
                    "grp_id":          grp_id,
                    "urgency":         r["urgency"],
                    "impact":          r["impact"],
                    "date_creation":   r["date_creation"] or None,
                    "date_resolution": r["date_resolution"] or None,
                    "date_close":      r["date_close"] or None,
                    "mttr_hours":      r["mttr_hours"],
                    "sla_respected":   r["sla_respected"],
                })
            conn.commit()
            loaded += 1
        except Exception as e:
            logger.error("Failed to load ticket %s: %s", r.get("glpi_ticket_id"), e)
            conn.rollback()

    conn.close()
    logger.info("Batch ETL done — %d tickets loaded to DW from silver/batch", loaded)
    return loaded


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_lake_batch_etl",
    description="Lambda Batch: source → bronze/batch → silver/batch → PostgreSQL DW",
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["itsm", "datalake", "batch", "lambda"],
) as dag:

    t1 = PythonOperator(task_id="ingest_to_bronze",        python_callable=ingest_to_bronze,
                        execution_timeout=timedelta(minutes=15))
    t2 = PythonOperator(task_id="process_bronze_to_silver", python_callable=process_bronze_to_silver,
                        execution_timeout=timedelta(minutes=20))
    t3 = PythonOperator(task_id="load_silver_to_dw",       python_callable=load_silver_to_dw,
                        execution_timeout=timedelta(minutes=20))

    t1 >> t2 >> t3
