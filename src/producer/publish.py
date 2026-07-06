import logging
import sys
import time
from typing import Any

import producer
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def delivery_report(err: Exception | None, msg: Any) -> None:
    """Log the result of a message delivery attempt.

    Args:
        err: Delivery error, or None on success.
        msg: The delivered confluent_kafka Message object.
    """
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info(
            "Message delivered to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


if __name__ == "__main__":
    settings = get_settings()
    schema_path = "events.avsc"

    client = producer.MockAvroProducer(settings, schema_path)
    avro_producer = client.avro_producer()

    # allow spark container time to initialize before messages arrive
    logger.info("Waiting 2 minutes before producing messages ...")
    time.sleep(120)

    logger.info("Starting data stream to %s", settings.kafka_broker_url)
    for i in range(1000):
        record = client.generate_data(i)
        avro_producer.produce(
            topic=settings.kafka_topic, value=record, callback=delivery_report
        )
        time.sleep(3)
        avro_producer.poll(0)

    avro_producer.flush()
