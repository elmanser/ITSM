"""
Data Lake Writer — Speed Layer
Kafka [itsm.tickets.raw] → MinIO bronze/streaming/

Architecture Lambda — Streaming path:
  Producer → Kafka → lake-writer → MinIO bronze/streaming/
                                        ↓
                              Airflow dag_lake_streaming_etl
                                        ↓
                              MinIO silver/streaming/  +  PostgreSQL DW
"""
import io, json, logging, os, signal, sys, time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio
from minio.error import S3Error

KAFKA_BROKER  = os.getenv("KAFKA_BROKER",     "kafka:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC_RAW",  "itsm.tickets.raw")
KAFKA_GROUP   = os.getenv("KAFKA_LAKE_GROUP", "itsm-lake-streaming-group")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "itsm_minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "itsm_minio_secret_2026")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "itsm-datalake")
LAKE_ZONE        = os.getenv("LAKE_ZONE",        "bronze/streaming")

BATCH_SIZE     = int(os.getenv("LAKE_BATCH_SIZE",    "20"))
FLUSH_INTERVAL = int(os.getenv("LAKE_FLUSH_INTERVAL","60"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("lake_writer")

_running = True


def _shutdown(signum, frame):
    global _running
    logger.info("Shutdown — flushing buffer…")
    _running = False


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def _minio():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def _ensure_bucket(mc):
    try:
        if not mc.bucket_exists(MINIO_BUCKET):
            mc.make_bucket(MINIO_BUCKET)
    except S3Error as e:
        logger.warning("Bucket check: %s", e)


def _object_key(ts: datetime, seq: int) -> str:
    """Partitioned: bronze/streaming/year=YYYY/month=MM/day=DD/HH/batch_SSffffff_NNNN.json"""
    return (
        f"{LAKE_ZONE}/"
        f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}/"
        f"batch_{ts.strftime('%S%f')}_{seq:04d}.json"
    )


def _flush(mc, buffer: list, seq: int) -> int:
    if not buffer:
        return seq
    ts  = datetime.now(timezone.utc)
    key = _object_key(ts, seq)
    raw = json.dumps({
        "zone":       LAKE_ZONE,
        "written_at": ts.isoformat(),
        "count":      len(buffer),
        "records":    buffer,
    }, default=str, ensure_ascii=False).encode("utf-8")
    data = io.BytesIO(raw)
    try:
        mc.put_object(MINIO_BUCKET, key, data, length=len(raw),
                      content_type="application/json")
        logger.info("bronze/streaming ← %d records → %s", len(buffer), key)
    except S3Error as e:
        logger.error("MinIO write failed: %s", e)
    return seq + 1


def _connect_kafka(retries=12):
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=50,
            )
            logger.info("Kafka connected (group=%s)", KAFKA_GROUP)
            return c
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 60)
            logger.warning("Kafka not ready (%d/%d) — retry in %ds", attempt, retries, wait)
            time.sleep(wait)
    logger.critical("Cannot connect to Kafka. Exiting.")
    sys.exit(1)


def main():
    logger.info("Lake Writer starting — Kafka[%s] → MinIO[%s/%s]",
                KAFKA_TOPIC, MINIO_BUCKET, LAKE_ZONE)
    mc       = _minio()
    _ensure_bucket(mc)
    consumer = _connect_kafka()

    buffer: list      = []
    last_flush: float = time.time()
    seq: int          = 0
    total: int        = 0

    try:
        while _running:
            records = consumer.poll(timeout_ms=2000)
            for _tp, messages in records.items():
                for msg in messages:
                    buffer.append(msg.value)
                    total += 1

            elapsed = time.time() - last_flush
            if len(buffer) >= BATCH_SIZE or (buffer and elapsed >= FLUSH_INTERVAL):
                seq        = _flush(mc, buffer, seq)
                buffer     = []
                last_flush = time.time()

        if buffer:
            _flush(mc, buffer, seq)
    finally:
        consumer.close()
        logger.info("Lake Writer stopped. Total records written: %d", total)


if __name__ == "__main__":
    main()
