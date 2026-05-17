"""
GLPI API Producer — Polls GLPI REST API and publishes tickets to Kafka.
Flow: GLPI REST API → Python Producer → Kafka [itsm.tickets.raw]
"""
import json, logging, os, random, signal, sys, time
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

GLPI_BASE_URL   = os.getenv("GLPI_BASE_URL", "http://glpi:80/apirest.php")
GLPI_APP_TOKEN  = os.getenv("GLPI_APP_TOKEN", "")
GLPI_USER_TOKEN = os.getenv("GLPI_USER_TOKEN", "")
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_RAW", "itsm.tickets.raw")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "30"))
BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "50"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("glpi_producer")

_running = True


def _shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal received. Stopping…")
    _running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ── Mock data configuration ───────────────────────────────────────────────────
_CATEGORIES   = ["network", "hardware", "software", "security", "access", "database", "email"]
_STATUS_OPEN  = [1, 2, 3, 4]        # New, Processing (assigned/planned), Pending
_STATUS_DONE  = [5, 6]              # Solved, Closed
# GLPI priority: 1=Very High, 2=High, 3=Medium, 4=Low, 5=Very Low
_PRIORITY_DIST = [0.05, 0.15, 0.40, 0.30, 0.10]
_PRIORITIES    = [1, 2, 3, 4, 5]
# Correlated urgency/impact per priority (mirrors train.py synthetic distribution)
_URGENCY_IMPACT = {
    1: {"choices": [5, 5, 4, 4, 3], "weights": [0.40, 0.30, 0.20, 0.07, 0.03]},  # Very High
    2: {"choices": [4, 4, 3, 3, 5], "weights": [0.30, 0.25, 0.25, 0.15, 0.05]},  # High
    3: {"choices": [3, 3, 2, 4, 2], "weights": [0.30, 0.25, 0.20, 0.15, 0.10]},  # Medium
    4: {"choices": [2, 2, 1, 3, 1], "weights": [0.30, 0.25, 0.25, 0.15, 0.05]},  # Low
    5: {"choices": [1, 1, 2, 1, 2], "weights": [0.40, 0.30, 0.20, 0.08, 0.02]},  # Very Low
}
# Resolution SLA limits per priority (hours) — used to generate realistic MTTRs
_SLA_LIMITS   = {1: 4, 2: 8, 3: 24, 4: 72, 5: 168}


class GLPIClient:
    """Thin wrapper around the GLPI REST API."""

    def __init__(self, base_url, app_token, user_token):
        self.base_url     = base_url.rstrip("/")
        self.app_token    = app_token
        self.user_token   = user_token
        self.session_token = None

    def init_session(self):
        headers = {
            "Content-Type": "application/json",
            "App-Token": self.app_token,
            "Authorization": f"user_token {self.user_token}",
        }
        try:
            resp = requests.get(f"{self.base_url}/initSession", headers=headers, timeout=10)
            if resp.status_code == 200:
                self.session_token = resp.json().get("session_token")
                logger.info("GLPI session initialized")
                return True
            logger.error("GLPI auth failed [%s]: %s", resp.status_code, resp.text)
            return False
        except requests.RequestException as e:
            logger.error("GLPI connection error: %s", e)
            return False

    def kill_session(self):
        if self.session_token:
            try:
                requests.get(f"{self.base_url}/killSession", headers=self._headers(), timeout=5)
            except Exception:
                pass
            self.session_token = None

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "App-Token": self.app_token,
            "Session-Token": self.session_token or "",
        }

    def get_tickets(self, offset=0, limit=50):
        params = {
            "range": f"{offset}-{offset + limit - 1}",
            "order": "ASC",
            "sort": "id",
            "expand_dropdowns": "true",
        }
        try:
            resp = requests.get(f"{self.base_url}/Ticket", headers=self._headers(),
                                params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else []
            if resp.status_code == 401:
                logger.warning("Session expired, re-authenticating…")
                self.init_session()
            return []
        except requests.RequestException as e:
            logger.error("Error fetching tickets: %s", e)
            return []


def _generate_mock_ticket(ticket_id: int) -> dict:
    """Generate a realistic mock ticket with correlated urgency/impact and proper timestamps."""
    priority = random.choices(_PRIORITIES, weights=_PRIORITY_DIST, k=1)[0]
    status   = random.choices(
        _STATUS_OPEN + _STATUS_DONE,
        weights=[10, 10, 5, 5, 35, 35],  # 50% resolved, 50% open
        k=1,
    )[0]

    # Urgency and impact correlated with priority for better ML signal
    ui_dist = _URGENCY_IMPACT[priority]
    urgency = random.choices(ui_dist["choices"], weights=ui_dist["weights"], k=1)[0]
    impact  = random.choices(ui_dist["choices"], weights=ui_dist["weights"], k=1)[0]

    # Creation time: somewhere in the past 7 days
    creation_hours_ago = random.uniform(1, 168)
    date_creation = datetime.now(timezone.utc) - timedelta(hours=creation_hours_ago)

    ticket = {
        "id": ticket_id,
        "name": f"[MOCK] Incident #{ticket_id} — {random.choice(_CATEGORIES).upper()}",
        "content": "Automated simulation ticket for real-time pipeline testing.",
        "priority": priority,
        "status":   status,
        "urgency":  urgency,
        "impact":   impact,
        "date_creation": date_creation.strftime("%Y-%m-%d %H:%M:%S"),
        "itilcategories_id": random.choice(_CATEGORIES),
        "_users_id_requester": f"user_{random.randint(1, 20)}",
        "_groups_id_assign":   f"group_{random.randint(1, 5)}",
    }

    if status in _STATUS_DONE:
        sla_limit = _SLA_LIMITS.get(priority, 24)
        # 70% chance of respecting SLA, 30% violation
        if random.random() < 0.70:
            resolve_hours = random.uniform(0.5, sla_limit * 0.9)
        else:
            resolve_hours = random.uniform(sla_limit * 1.1, sla_limit * 3)
        resolve_hours = min(resolve_hours, creation_hours_ago)
        date_resolution = date_creation + timedelta(hours=resolve_hours)
        ticket["solvedate"] = date_resolution.strftime("%Y-%m-%d %H:%M:%S")
        if status == 6:
            close_hours = resolve_hours + random.uniform(0.5, 4)
            close_hours = min(close_hours, creation_hours_ago)
            ticket["closedate"] = (date_creation + timedelta(hours=close_hours)).strftime("%Y-%m-%d %H:%M:%S")

    return ticket


def create_kafka_producer(broker, retries=10):
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info("Kafka producer connected to %s", broker)
            return p
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 60)
            logger.warning("Kafka not ready (attempt %d/%d). Retrying in %ds…", attempt, retries, wait)
            time.sleep(wait)
    logger.critical("Could not connect to Kafka. Exiting.")
    sys.exit(1)


def main():
    logger.info("GLPI Producer starting — polling %s every %ds", GLPI_BASE_URL, POLL_INTERVAL)

    glpi     = GLPIClient(GLPI_BASE_URL, GLPI_APP_TOKEN, GLPI_USER_TOKEN)
    use_mock = not GLPI_APP_TOKEN or not GLPI_USER_TOKEN

    if use_mock:
        logger.warning("GLPI tokens missing — running in MOCK mode (synthetic real-time data).")
    else:
        while _running and not glpi.init_session():
            logger.info("Waiting for GLPI to become available…")
            time.sleep(10)

    if not _running:
        return

    producer     = create_kafka_producer(KAFKA_BROKER)
    last_seen_id = 2000 if use_mock else 0
    total        = 0

    try:
        while _running:
            if use_mock:
                # Emit 1–4 realistic mock tickets each cycle
                new = []
                for _ in range(random.randint(1, 4)):
                    last_seen_id += 1
                    new.append(_generate_mock_ticket(last_seen_id))
            else:
                tickets = glpi.get_tickets(offset=last_seen_id, limit=BATCH_SIZE)
                new = [t for t in tickets if t.get("id", 0) > last_seen_id]

            for t in new:
                msg = {
                    "event_type": "ticket_ingested",
                    "source":     "glpi_api",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "data": t,
                }
                producer.send(KAFKA_TOPIC, key=str(t.get("id", "")), value=msg)
                total += 1

            if new:
                last_seen_id = max(t.get("id", 0) for t in new)
                producer.flush()
                logger.info("Published %d tickets (total=%d, last_id=%d)", len(new), total, last_seen_id)

            for _ in range(POLL_INTERVAL):
                if not _running:
                    break
                time.sleep(1)

    finally:
        producer.flush()
        producer.close()
        glpi.kill_session()
        logger.info("Producer stopped. Total published: %d", total)


if __name__ == "__main__":
    main()
