import time
import uuid
import random
from confluent_kafka import avro
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer


class MockAvroProducer:
    def __init__(self, config_path, topic):
        self.config_path = config_path
        self.topic = topic
        self.config = self.__get_config()
        self.schema_client = self.__get_schema_client()
        self.schema_id = self.__register_schema()
        self.schema = self.__get_schema()


    def __get_config(self):
        pass


    def __get_schema_client(self):
        return CachedSchemaRegistryClient({'url': self.config['schema_registry_url']})
    

    def __register_schema(self):
        with open(self.config_path, "r") as f:
            schema_str = f.read()
            schema = avro.loads(schema_str)
        schema_id = self.schema_client.register_schema(self.topic, schema)
        return schema_id


    def __get_schema(self, schema_client, schema_id):
        return schema_client.get_schema(schema_id)


    def avro_producer(self):
        return AvroProducer(
            {'bootstrap.servers': self.config['kafka_broker_url']},
            schema_registry = self.schema_client,
            default_value_schema = self.schema
        )
    
    
    def generate_data(self, i):
        devices = ['mobile', 'tablet', 'laptop']
        events = ['click', 'pageview', 'login', 'download']
        return {
            'event_timestamp': time.time(),
            'event_id': str(i),
            'event_type': random.choice(events),
            'device_type': random.choice(devices),
            'user_id': str(uuid.uuid4())
        }