import argparse
import json
import logging
import uuid
from pathlib import Path
from typing import Any
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from parser import parse_pdf_content, get_debug_log_dir

try:
    from confluent_kafka.cimpl import Producer, KafkaException
    KAFKA_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    Producer = None
    KafkaException = Exception
    KAFKA_IMPORT_ERROR = exc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Statement parser API")

logger.info(f"Debug extraction logs directory: {get_debug_log_dir()}")


KAFKA_CONFIG_PATH = Path(__file__).with_name("kafka.properties")
producer: Any | None = None
KAFKA_CONFIG: dict[str, str] = {}
KAFKA_CONFIG_ERROR: Exception | None = None
KAFKA_BROKER: str | None = None
KAFKA_TOPIC: str | None = None
KAFKA_CLIENT_ID: str = "fastapi-pdf-parser"


def load_properties(path: Path) -> dict[str, str]:
    properties: dict[str, str] = {}

    with path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()

            if not line or line.startswith(("#", ";")):
                continue

            if "=" not in line:
                raise ValueError(
                    f"Invalid kafka.properties entry at line {line_number}: {raw_line.rstrip()}"
                )

            key, value = line.split("=", 1)
            properties[key.strip()] = value.strip()

    return properties


def load_kafka_config() -> dict[str, str]:
    config = load_properties(KAFKA_CONFIG_PATH)

    missing_keys = [key for key in ("bootstrap.servers", "topic") if not config.get(key)]
    if missing_keys:
        missing = ", ".join(missing_keys)
        raise ValueError(f"Missing required Kafka config keys in {KAFKA_CONFIG_PATH.name}: {missing}")

    return config


def configure_kafka(config_path: Path | None = None) -> None:
    global KAFKA_CONFIG_PATH, KAFKA_CONFIG, KAFKA_CONFIG_ERROR
    global KAFKA_BROKER, KAFKA_TOPIC, KAFKA_CLIENT_ID

    if config_path is not None:
        KAFKA_CONFIG_PATH = config_path

    try:
        KAFKA_CONFIG = load_kafka_config()
        KAFKA_CONFIG_ERROR = None
    except (OSError, ValueError) as exc:
        KAFKA_CONFIG = {}
        KAFKA_CONFIG_ERROR = exc

    KAFKA_BROKER = KAFKA_CONFIG.get("bootstrap.servers")
    KAFKA_TOPIC = KAFKA_CONFIG.get("topic")
    KAFKA_CLIENT_ID = KAFKA_CONFIG.get("client.id", "fastapi-pdf-parser")


configure_kafka()

def kafka_error_callback(err):
    logger.error(f"Kafka underlying error: {err}")


def get_kafka_producer() -> Any:
    global producer

    if producer is not None:
        return producer

    if Producer is None:
        raise RuntimeError(
            "Kafka support is unavailable because 'confluent-kafka' is not installed. "
            "Install dependencies from phonepe-parser/requirements.txt in the active virtual environment."
        ) from KAFKA_IMPORT_ERROR

    if KAFKA_CONFIG_ERROR is not None:
        raise RuntimeError(
            f"Kafka configuration could not be loaded from {KAFKA_CONFIG_PATH}: {KAFKA_CONFIG_ERROR}"
        ) from KAFKA_CONFIG_ERROR

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": KAFKA_CLIENT_ID,
        "error_cb": kafka_error_callback,
    })
    return producer

def test_kafka_connection():

    try:
        kafka_producer = get_kafka_producer()
        logger.info(f"Attempting to ping Kafka broker at {KAFKA_BROKER}...")
        cluster_metadata = kafka_producer.list_topics(timeout=5.0)
        logger.info(f"Successfully connected to Kafka! Found {len(cluster_metadata.topics)} topics.")
    except RuntimeError as e:
        logger.warning(str(e))
    except KafkaException as e:
        logger.error(f"Failed to connect to Kafka broker. Is it running? Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error connecting to Kafka: {e}")

test_kafka_connection()


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Statement parser API")
    parser.add_argument(
        "--kafka-config",
        type=Path,
        default=KAFKA_CONFIG_PATH,
        help="Path to Kafka properties file",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def process_and_publish(file_bytes: bytes, password: str, job_id: str):

    try:
        kafka_producer = get_kafka_producer()
        logger.info(f"Job {job_id}: Starting PDF parsing...")
        result = parse_pdf_content(file_bytes, password, job_id=job_id)
        transactions = result.get("transactions", [])

        logger.info(f"Job {job_id}: Found {len(transactions)} transactions. Publishing to Kafka...")

        for txn in transactions:

            txn['job_id'] = job_id

 
            kafka_producer.produce(
                topic=KAFKA_TOPIC,
                key=job_id.encode('utf-8'), 
                value=json.dumps(txn).encode('utf-8'),
                callback=delivery_report
            )
 
            kafka_producer.poll(0)

        final_message = {
            "type": "JOB_COMPLETED",
            "job_id": job_id,
            "total_records": len(transactions),
        }

        kafka_producer.produce(
            topic=KAFKA_TOPIC,
            key=job_id.encode('utf-8'),   
            value=json.dumps(final_message).encode('utf-8'),
            callback=delivery_report
        )
        kafka_producer.flush()
        logger.info(f"Job {job_id}: Successfully published all transactions.")

    except Exception as e:
        logger.error(f"Job {job_id}: Failed to process or publish - {str(e)}")


@app.post("/parse-async", tags=["Parsing"])
async def parse_statement_async(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        mobile_number: str = Form(..., pattern=r"^\d{10}$")
):
    try:
        get_kafka_producer()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only 'application/pdf' is supported.")

    file_bytes = await file.read()
    job_id = str(uuid.uuid4())

    background_tasks.add_task(process_and_publish, file_bytes, mobile_number, job_id)

    return {
        "status": "Accepted",
        "message": "File is being processed. Transactions will be streamed to Kafka.",
        "job_id": job_id
    }


if __name__ == "__main__":
    import uvicorn

    args = parse_cli_args()
    configure_kafka(args.kafka_config)
    test_kafka_connection()
    uvicorn.run(app, host=args.host, port=args.port)
