"""
Kafka Consumer — Reads from Kafka [itsm.tickets.raw], enriches tickets,
and loads them into the PostgreSQL Data Warehouse.
Flow: Kafka → Validation & Enrichment → PostgreSQL DW (fact_tickets, fact_ticket_sla)
"""
import json, logging, os, signal, sys, time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC_RAW", "itsm.tickets.raw")
KAFKA_GROUP    = os.getenv("KAFKA_GROUP_ID", "itsm-consumer-group")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "itsm_dw")
PG_USER = os.getenv("POSTGRES_USER", "itsm")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("kafka_consumer")

_running = True
def _shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal received. Stopping…")
    _running = False
signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# --- Priority mapping GLPI code → label ---
PRIORITY_MAP = {1: "Very High", 2: "High", 3: "Medium", 4: "Low", 5: "Very Low"}
SLA_LIMITS_HOURS = {"Very High": 4, "High": 8, "Medium": 24, "Low": 72, "Very Low": 168}


def get_pg_conn(retries=10):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                     user=PG_USER, password=PG_PASS)
            conn.autocommit = False
            logger.info("PostgreSQL connected")
            return conn
        except psycopg2.OperationalError as e:
            wait = min(2 ** attempt, 60)
            logger.warning("PostgreSQL not ready (attempt %d/%d): %s. Retrying in %ds…", attempt, retries, e, wait)
            time.sleep(wait)
    logger.critical("Cannot connect to PostgreSQL. Exiting.")
    sys.exit(1)


def create_kafka_consumer(broker, topic, group, retries=10):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                group_id=group,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=100,
            )
            logger.info("Kafka consumer connected: topic=%s group=%s", topic, group)
            return consumer
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 60)
            logger.warning("Kafka not ready (attempt %d/%d). Retrying in %ds…", attempt, retries, wait)
            time.sleep(wait)
    logger.critical("Cannot connect to Kafka. Exiting.")
    sys.exit(1)


def parse_glpi_datetime(val):
    """Parse GLPI datetime string to Python datetime."""
    if not val or val == "NULL":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def enrich_ticket(ticket: dict) -> dict:
    """Compute MTTR, SLA compliance, and normalise fields."""
    date_creation   = parse_glpi_datetime(ticket.get("date_creation"))
    date_resolution = parse_glpi_datetime(ticket.get("solvedate"))
    date_close      = parse_glpi_datetime(ticket.get("closedate"))

    mttr_hours = None
    if date_creation and date_resolution:
        delta = date_resolution - date_creation
        mttr_hours = round(delta.total_seconds() / 3600, 2)

    priority_code  = ticket.get("priority", 3)
    priority_label = PRIORITY_MAP.get(priority_code, "Medium")
    sla_limit      = SLA_LIMITS_HOURS.get(priority_label, 24)
    sla_respected  = None
    if mttr_hours is not None:
        sla_respected = mttr_hours <= sla_limit

    return {
        "glpi_ticket_id":   ticket.get("id"),
        "title":            ticket.get("name", ""),
        "description":      ticket.get("content", ""),
        "priority_code":    priority_code,
        "priority_label":   priority_label,
        "status_code":      ticket.get("status", 1),
        "urgency":          ticket.get("urgency", 3),
        "impact":           ticket.get("impact", 3),
        "date_creation":    date_creation,
        "date_resolution":  date_resolution,
        "date_close":       date_close,
        "mttr_hours":       mttr_hours,
        "sla_respected":    sla_respected,
        "sla_limit_hours":  sla_limit,
        "category_name":    ticket.get("itilcategories_id", "Uncategorized"),
        "user_name":        ticket.get("_users_id_requester", ""),
        "group_name":       ticket.get("_groups_id_assign", ""),
        "source":           "glpi_api",
    }


def get_or_create_category(cur, category_name: str) -> int | None:
    """Return category_id, creating the row if it doesn't exist."""
    if not category_name or category_name in ("Uncategorized", ""):
        return None
    name = str(category_name).lower().strip()
    cur.execute("SELECT category_id FROM dim_category WHERE name = %s LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO dim_category (name, itil_type) VALUES (%s, 'incident') RETURNING category_id",
        (name,)
    )
    return cur.fetchone()[0]


def get_or_create_group(cur, group_name: str) -> int | None:
    """Return group_id, creating the row if it doesn't exist."""
    if not group_name or group_name == "":
        return None
    name = str(group_name).strip()
    cur.execute("SELECT group_id FROM dim_group WHERE name = %s LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO dim_group (name, team_type) VALUES (%s, 'support') RETURNING group_id",
        (name,)
    )
    return cur.fetchone()[0]


def upsert_ticket(cur, enriched: dict):
    """Upsert enriched ticket into fact_tickets including category and group."""
    category_id = get_or_create_category(cur, enriched.get("category_name", ""))
    group_id    = get_or_create_group(cur, enriched.get("group_name", ""))

    sql = """
        INSERT INTO fact_tickets (
            glpi_ticket_id, title, description, priority_id, status_id,
            category_id, group_id,
            urgency, impact, date_creation, date_resolution, date_close,
            mttr_hours, sla_respected, source, ingested_at, updated_at
        )
        VALUES (
            %(glpi_ticket_id)s,
            %(title)s,
            %(description)s,
            (SELECT priority_id FROM dim_priority WHERE code = %(priority_code)s LIMIT 1),
            (SELECT status_id  FROM dim_status   WHERE code = %(status_code)s  LIMIT 1),
            %(category_id)s, %(group_id)s,
            %(urgency)s, %(impact)s,
            %(date_creation)s, %(date_resolution)s, %(date_close)s,
            %(mttr_hours)s, %(sla_respected)s,
            %(source)s, NOW(), NOW()
        )
        ON CONFLICT (glpi_ticket_id)
        DO UPDATE SET
            title           = EXCLUDED.title,
            description     = EXCLUDED.description,
            priority_id     = EXCLUDED.priority_id,
            status_id       = EXCLUDED.status_id,
            category_id     = EXCLUDED.category_id,
            group_id        = EXCLUDED.group_id,
            date_resolution = EXCLUDED.date_resolution,
            date_close      = EXCLUDED.date_close,
            mttr_hours      = EXCLUDED.mttr_hours,
            sla_respected   = EXCLUDED.sla_respected,
            updated_at      = NOW()
        RETURNING ticket_id;
    """
    enriched["category_id"] = category_id
    enriched["group_id"]    = group_id
    cur.execute(sql, enriched)
    row = cur.fetchone()
    return row[0] if row else None


def upsert_sla(cur, ticket_id: int, enriched: dict):
    """Insert/update SLA record for a ticket."""
    if enriched["date_resolution"] is None:
        return
    deadline = None
    if enriched["date_creation"] and enriched["sla_limit_hours"]:
        from datetime import timedelta
        deadline = enriched["date_creation"] + timedelta(hours=enriched["sla_limit_hours"])

    delay = None
    if deadline and enriched["date_resolution"]:
        delta = enriched["date_resolution"] - deadline
        delay = round(delta.total_seconds() / 3600, 2)

    sql = """
        INSERT INTO fact_ticket_sla (ticket_id, sla_deadline, resolution_date, sla_respected, delay_hours, mttr_hours)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(sql, (ticket_id, deadline, enriched["date_resolution"],
                      enriched["sla_respected"], delay, enriched["mttr_hours"]))


def process_message(conn, msg_value: dict):
    """Process a single Kafka message: enrich + upsert to DW."""
    ticket = msg_value.get("data", {})
    if not ticket or not ticket.get("id"):
        return

    enriched = enrich_ticket(ticket)
    with conn.cursor() as cur:
        ticket_id = upsert_ticket(cur, enriched)
        if ticket_id:
            upsert_sla(cur, ticket_id, enriched)
    conn.commit()


def main():
    logger.info("Kafka Consumer starting — topic=%s", KAFKA_TOPIC)
    conn     = get_pg_conn()
    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP)
    processed = 0

    try:
        while _running:
            records = consumer.poll(timeout_ms=2000)
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        process_message(conn, msg.value)
                        processed += 1
                        if processed % 50 == 0:
                            logger.info("Processed %d tickets total", processed)
                    except Exception as e:
                        logger.error("Error processing message offset=%s: %s", msg.offset, e)
                        conn.rollback()
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer stopped. Processed %d messages.", processed)

if __name__ == "__main__":
    main()
