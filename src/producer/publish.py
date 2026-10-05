import logging
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaError, Message

import producer
from streaming_config import load_config

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config.json")
SCHEMA_PATH = Path("events.avsc")
EVENT_COUNT = 1000
STARTUP_DELAY_SECONDS = 120
SECONDS_BETWEEN_EVENTS = 3


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Log whether a message was delivered, and where."""
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info(
            "Message delivered to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def main() -> None:
    """Publish EVENT_COUNT synthetic events, one every few seconds."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logger.info("Reading config file for producer at %s ...", CONFIG_PATH)
    client = producer.MockAvroProducer(load_config(CONFIG_PATH), SCHEMA_PATH)
    avro_producer = client.avro_producer()

    # allow spark container time to initialize before messages arrive
    logger.info("Waiting 2 minutes before producing messages ...")
    time.sleep(STARTUP_DELAY_SECONDS)

    logger.info("Starting data stream to %s", client.config.kafka_broker_url)
    for i in range(EVENT_COUNT):
        event = client.generate_data(i)
        avro_producer.produce(
            topic=client.topic, value=event.model_dump(), callback=delivery_report
        )
        time.sleep(SECONDS_BETWEEN_EVENTS)
        avro_producer.poll(0)

    avro_producer.flush()


if __name__ == "__main__":
    main()
