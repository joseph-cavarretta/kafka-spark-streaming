import sys
import time
import logging
import producer

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)

# Delivery report callback
def delivery_report(err, msg):
    """Callback function for message delivery reports."""
    if err is not None:
        logger.ERROR(f"Message delivery failed: {err}")
    else:
        logger.INFO(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


if __name__ == '__main__':
    client = producer.MockAvroProducer()
    avro_producer = client.avro_producer()

    # Produce messages
    for i in range(1000):
        record = client.generate_data(i)
        # Send the message using Avro serialization
        avro_producer.produce(
            topic=client.topic,
            value=record,
            callback=delivery_report
        )
        time.sleep(2)
        # Poll to handle delivery reports
        avro_producer.poll(0)

    # Wait for all messages to be delivered
    avro_producer.flush()