import os
import sys
import json
import logging
#from pyspark.sql import SparkSession
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
    def __init__(self, spark_session, config_path):
        self.logger = logging.getLogger(__name__)
        self.spark = spark_session
        self.config_path = config_path
        self.config = self.__get_config()
        self.topic = self.__get_topic()
        self.schema_client = self.__get_schema_client()
        self.schema = self.__get_schema()
        self.schema_str = self.schema.schema_str
        self.deserializer = self.__get_deserializer()
        self.streaming_df = self.__create_streaming_df()


    def __get_config(self):
        if os.path.isfile(self.config_path):
            self.logger.info(f"Reading config file at {self.config_path}")
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
        self.logger.info(f"Retrieving schema registry client for {self.config['schema_registry_url']}")
        return SchemaRegistryClient({'url': self.config['schema_registry_url']})
    

    def __get_schema(self):
        subject = self.topic
        schema = self.schema_client.get_latest_version(subject).schema
        self.logger.info(f"Using schema: {str(schema)}")
        return schema


    def __get_deserializer(self):
        self.logger.info("Getting avro deserializer")
        return AvroDeserializer(
            schema_str = self.schema.schema_str,
            schema_registry_client = self.schema_client
        )
    

    def __create_streaming_df(self):
        self.logger.info("Initializing spark dataframe")
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_broker_url']) \
            .option("subscribe", self.topic) \
            .option("group.id", self.config['kafka_group_id']) \
            .option("startingOffsets", "earliest") \
            .load()

    @staticmethod
    def __process_message(row, schema_str, schema_registry_url):
        """ Static method used in Spark's 'foreach' avoids using 
        instance attributes to enable parallel processing"""
        schema_client = SchemaRegistryClient({'url': schema_registry_url})
        deserializer = AvroDeserializer(schema_str=schema_str, schema_registry_client=schema_client)
        deserialized_message = deserializer(row)
        print(f"Processed Message: {deserialized_message}")


    def process_stream(self):
        self.logger.info("Beginning stream processing")
        # Cast binary data to string format for deserialization
        df = self.streaming_df.withColumn("value", col("value").cast("binary"))

        schema_str = self.schema_str, 
        schema_registry_url = self.config['schema_registry_url']
        
        # Start the streaming query
        query = df.writeStream \
            .foreach(
                lambda row: self.__process_message(
                    row['value'], 
                    schema_str, 
                    schema_registry_url
                    )
            ) \
            .start()

        query.awaitTermination()