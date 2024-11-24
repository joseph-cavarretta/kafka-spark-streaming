import os
import sys
import time
import uuid
import random
import logging
import json
from confluent_kafka import avro
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)


class MockAvroProducer:
    def __init__(self, config_path, schema_path):
        self.logger = logging.getLogger(__name__)
        self.config_path = config_path
        self.schema_path = schema_path
        self.config = self.__get_config()
        self.topic = self.__get_topic()
        self.schema_client = self.__get_schema_client()
        self.schema_id = self.__register_schema()
        self.schema = self.__get_schema()
        

    def __get_config(self):
        if os.path.isfile(self.schema_path):
            self.logger.info(f"Reading config file for producer at {self.config_path}")
            with open(self.config_path) as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"No config file found at {self.config_path}. Config file is required.")


    def __get_topic(self):
        if isinstance(self.config['kafka_topic'], list):
            raise TypeError("Lists of topics not supported. Topic must be a string")
        
        if not isinstance(self.config['kafka_topic'], str):
            raise TypeError("Topic must be a string")

        return self.config['kafka_topic']


    def __get_schema_client(self):
        self.logger.info(f"Retrieving cached schema registry client for {self.config['schema_registry_url']}")
        return CachedSchemaRegistryClient({'url': self.config['schema_registry_url']})
    

    def __register_schema(self):
        if os.path.isfile(self.schema_path):
            self.logger.info(f"Reading and registering schema from {self.schema_path}")
            with open(self.schema_path, "r") as f:
                schema_str = f.read()
                schema = avro.loads(schema_str)
            try:
                schema_id = self.schema_client.register(self.topic, schema)
                return schema_id
            except avro.error.ClientError: # couldnt reach registry
                # retry
                raise
        else:
            raise FileNotFoundError(f"No schema file found at {self.schema_path}. Schema file is required.")


    def __get_schema(self):
        self.logger.info(f"Fetching registered schema with id {self.schema_id}")
        return self.schema_client.get_by_id(self.schema_id)


    def avro_producer(self):
        self.logger.info(
            f"""Instantiating Avro Producer for kafka bootstrap servers {self.config['kafka_broker_url']}, 
            using schema client at {self.config['schema_registry_url']}""")
        return AvroProducer(
            {'bootstrap.servers': self.config['kafka_broker_url']},
            schema_registry = self.schema_client,
            default_value_schema = self.schema
        )
    

    def generate_data(self, i):
        devices = ['mobile', 'tablet', 'laptop']
        events = ['click', 'pageview', 'login', 'download']
        data = {
            'event_timestamp': time.time(),
            'event_id': i,
            'event_type': random.choice(events),
            'device_type': random.choice(devices),
            'user_id': str(uuid.uuid4())
        }
        self.logger.info(f"Message generated: {data}")
        return data