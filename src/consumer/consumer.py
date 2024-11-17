import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)


class SparkAvroConsumer:
    def __init__(self, spark_session, config_path, topic):
        self.logger = logging.getLogger(__name__)
        self.spark = spark_session
        self.config_path = config_path
        self.topic = topic
        self.config = self.__get_config()
        self.schema_client = self.__get_schema_client()
        self.schema = self.__get_schema()
        self.deserializer = self.__get_deserializer()
        self.streaming_df = self.__create_df()


    def __get_config(self):
        if os.path.isfile(self.schema_path):
            self.logger.info(f"Reading config file at {self.config_path}")
            with open(self.config_path) as f:
                self.config = json.load(f)
        else:
            raise FileNotFoundError(f"No config file found at {self.config_path}. Config file is required.")
        

    def __get_schema_client(self):
        self.logger.info(f"Retrieving schema registry client for {self.config['schema_registry_url']}")
        return SchemaRegistryClient({'url': self.config['schema_registry_url']})
    

    def __get_schema(self):
        subject = f"{self.topic}-value"
        return self.schema_registry_client.get_latest_version(subject).schema


    def __get_deserializer(self):
        return AvroDeserializer(
            schema_str = self.schema.schema_str,
            schema_registry_client = self.schema_registry_client
        )


    def __deserialize_avro(self, avro_message):
        return self.avro_deserializer(
            avro_message, 
            SerializationContext(self.topic, MessageField.VALUE)
        )
    

    def __create_df(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_bootstrap_servers']) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()


    def __process_message(self, message):
        avro_data = message.value
        deserialized_message = self.__deserialize_avro(avro_data)
        print(deserialized_message)


    def process_stream(self):
        # Cast binary data to string format for deserialization
        df = self.streaming_df.withColumn("value", col("value").cast("binary"))

        # Start the streaming query
        query = df.writeStream \
            .foreach(self.__process_message()) \
            .start()

        query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Spark Avro Consumer") \
            .getOrCreate()

    config_path = ''
    topic = 'your_topic_name'

    consumer = SparkAvroConsumer(spark, config_path, topic)
    consumer.process_stream()