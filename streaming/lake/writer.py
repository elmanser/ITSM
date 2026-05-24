"""
Data Lake Writer — Kafka → MinIO (Bronze Layer)
Consumes raw tickets from Kafka and writes batched JSON files to MinIO.

Architecture (Lambda):
  Speed path : Kafka → Consumer     → PostgreSQL DW  (real-time)
  Batch path : Kafka → Lake Writer  → MinIO          → Airflow → PostgreSQL DW
"""
import io, json, logging, os, signal, sys, time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio
from minio.error import S3Error

KAFKA_BROKER  = os.getenv("KAFKA_BROKER",       "kafka:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC_RAW",    "itsm.tickets.raw")
KAFKA_GROUP   = os.getenv("KAFKA_LAKE_GROUP",   "itsm-lake-group")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "itsm_minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "itsm_minio_secret_2026")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "itsm-data-lake")

BATCH_SIZE     = int(os.getenv("LAKE_BATCH_SIZE",    "20"))   # flush every N messages
FLUSH_INTERVAL = int(os.getenv("LAKE_FLUSH_INTERVAL","60"))   # or every N seconds

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("lake_writer")

_running = True


def _shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal — draining buffer…")
    _running = False


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def _minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def _ensure_bucket(client: Minio):
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logger.info("Created bucket: %s", MINIO_BUCKET)
    except S3Error as e:
        logger.warning("Bucket check failed: %s", e)


def _object_path(batch_ts: datetime, seq: int) -> str:
    """Partitioned path: bronze/tickets/year=YYYY/month=MM/day=DD/batch_HHMMSSµµµ_NNN.json"""
    return (
        f"bronze/tickets/"
        f"year={batch_ts.year}/"
        f"month={batch_ts.month:02d}/"
        f"day={batch_ts.day:02d}/"
        f"batch_{batch_ts.strftime('%H%M%S%f')}_{seq:04d}.json"
    )


def _flush(client: Minio, buffer: list, seq: int) -> int:
    if not buffer:
        return seq
    ts  = datetime.now(timezone.utc)
    obj = _object_path(ts, seq)
    payload = json.dumps(buffer, default=str, ensure_ascii=False).encode("utf-8")
    data    = io.BytesIO(payload)
    try:
        client.put_object(
            MINIO_BUCKET, obj, data, length=len(payload),
            content_type="application/json",
        )
        logger.info("Flushed %d records → s3://%s/%s", len(buffer), MINIO_BUCKET, obj)
    except S3Error as e:
        logger.error("MinIO write failed: %s", e)
    return seq + 1


def _create_consumer(retries: int = 12) -> KafkaConsumer:
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
            logger.info("Kafka consumer connected (group=%s)", KAFKA_GROUP)
            return c
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 60)
            logger.warning("Kafka not ready (attempt %d/%d) — retry in %ds", attempt, retries, wait)
            time.sleep(wait)
    logger.critical("Cannot connect to Kafka. Exiting.")
    sys.exit(1)


def main():
    logger.info("Lake Writer starting — topic=%s → minio://%s/bronze/tickets/",
                KAFKA_TOPIC, MINIO_BUCKET)

    client   = _minio_client()
    _ensure_bucket(client)
    consumer = _create_consumer()

    buffer: list       = []
    last_flush: float  = time.time()
    seq: int           = 0
    total: int         = 0

    try:
        while _running:
            records = consumer.poll(timeout_ms=2000)
            for _tp, messages in records.items():
                for msg in messages:
                    buffer.append(msg.value)
                    total += 1

            elapsed = time.time() - last_flush
            if len(buffer) >= BATCH_SIZE or (buffer and elapsed >= FLUSH_INTERVAL):
                seq        = _flush(client, buffer, seq)
                buffer     = []
                last_flush = time.time()

        # Final flush on shutdown
        if buffer:
            _flush(client, buffer, seq)

    finally:
        consumer.close()
        logger.info("Lake Writer stopped. Total messages written: %d", total)


if __name__ == "__main__":
    main()
