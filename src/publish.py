import sys
import time
import logging


schema_registry_url = 'https://172.18.0.4:8081'
kafka_broker_url = 'https://172.18.0.3:9092'
# Kafka topic
topic = 'avro-topic'

# Kafka and Schema Registry configuration
config = {
    'bootstrap.servers': kafka_broker_url,
    'schema.registry.url': schema_registry_url
}

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout)


def get_config():
    pass

def register_schema(schema_client):
    with open("events.avsc", "r") as f:
        schema_str = f.read()
        schema = avro.loads(schema_str)
    schema_id = schema_client.register_schema('test-topic', schema)
    return schema_id


def get_schema(schema_client, schema_id):
    return schema_client.get_schema(schema_id)


def get_producer(schema_client, schema, config):
    return AvroProducer(
        {'bootstrap.servers': config['bootstrap.servers']},
        schema_registry = schema_client,
        default_value_schema = schema
    )

# Delivery report callback
def delivery_report(err, msg):
    """Callback function for message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


if __name__ == '__main__':
    
    
    # Produce Avro messages
    for i in range(1000):
        value = {'id': i, 'name': f'User {i}', 'email': None if i % 2 == 0 else f'user{i}@example.com'}
        
        # Send the message using Avro serialization
        avro_producer.produce(
            topic=topic,
            value=value,
            callback=delivery_report
        )
        time.sleep(2)
        # Poll to handle delivery reports
        avro_producer.poll(0)

    # Wait for all messages to be delivered
    avro_producer.flush()