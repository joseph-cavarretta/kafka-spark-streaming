from confluent_kafka import avro
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer


class MockProducer:
    def __init__(self, config_path, topic):
        self.config_path = config_path
        self.topic = topic
        self.config = self.__get_config()
        self.schema_id = self.__register_schema()
        self.schema = self.__get_schema()

    def __get_config(self):
        pass

    def __register_schema(self, schema_client, config):
        # Create a client instance
        client = CachedSchemaRegistryClient({'url': config['schema_registry_url']})

        with open(self.config_path, "r") as f:
            schema_str = f.read()
            schema = avro.loads(schema_str)
        schema_id = schema_client.register_schema(self.topic, schema)
        return schema_id


    def __get_schema(self, schema_client, schema_id):
        return schema_client.get_schema(schema_id)


    def get_producer(self, schema_client, schema, config):
        return AvroProducer(
            {'bootstrap.servers': config['bootstrap.servers']},
            schema_registry = schema_client,
            default_value_schema = schema
        )