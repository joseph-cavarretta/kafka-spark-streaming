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
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


if __name__ == '__main__':
    config_path = 'config.json'
    schema_path = 'events.avsc'
    
    client = producer.MockAvroProducer(config_path, schema_path)
    avro_producer = client.avro_producer()

    # Give spark container time to startup
    logger.info("Waiting 2 minutes before producing messages ...")
    time.sleep(120)

    # Produce messages
    logger.info(f"Starting data stream to {client.config['kafka_broker_url']}")
    for i in range(1000):
        record = client.generate_data(i)
        # Send the message using Avro serialization
        avro_producer.produce(
            topic=client.topic,
            value=record,
            callback=delivery_report
        )
        time.sleep(3)
        # Poll to handle delivery reports
        avro_producer.poll(0)

    # Wait for all messages to be delivered
    avro_producer.flush()