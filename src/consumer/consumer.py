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
    def __init__(self, spark_session, config_path):
        self.logger = logging.getLogger(__name__)
        self.spark = spark_session
        self.config_path = config_path
        self.config = self.__get_config()
        self.topic = self.__get_topic()
        self.schema_client = self.__get_schema_client()
        self.schema = self.__get_schema()
        self.deserializer = self.__get_deserializer()
        self.streaming_df = self.__create_streaming_df()


    def __get_config(self):
        if os.path.isfile(self.schema_path):
            self.logger.info(f"Reading config file at {self.config_path}")
            with open(self.config_path) as f:
                self.config = json.load(f)
        else:
            raise FileNotFoundError(f"No config file found at {self.config_path}. Config file is required.")
        

    def __get_topic(self):
        if isinstance(self.config['topic'], list):
            raise TypeError("Lists of topics not supported. Topic must be a string")
        
        if not isinstance(self.config['topic'], str):
            raise TypeError("Topic must be a string")

        return self.config['topic']
    

    def __get_schema_client(self):
        self.logger.info(f"Retrieving schema registry client for {self.config['schema_registry_url']}")
        return SchemaRegistryClient({'url': self.config['schema_registry_url']})
    

    def __get_schema(self):
        subject = f"{self.topic}-value"
        schema = self.schema_registry_client.get_latest_version(subject).schema
        self.logger.info(f"Using schema: {str(schema)}")
        return schema


    def __get_deserializer(self):
        self.logger.info("Getting avro deserializer")
        return AvroDeserializer(
            schema_str = self.schema.schema_str,
            schema_registry_client = self.schema_registry_client
        )


    def __deserialize_avro(self, avro_message):
        return self.avro_deserializer(
            avro_message, 
            SerializationContext(self.topic, MessageField.VALUE)
        )
    

    def __create_streaming_df(self):
        self.logger.info("Initializing spark dataframe")
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_bootstrap_servers']) \
            .option("subscribe", self.topic) \
            .option("group.id", self.config['kafka_group_id']) \
            .option("startingOffsets", "earliest") \
            .load()


    def __process_message(self, message):
        avro_data = message.value
        deserialized_message = self.__deserialize_avro(avro_data)
        print(deserialized_message)


    def process_stream(self):
        self.logger.info("Beginning stream processing")
        # Cast binary data to string format for deserialization
        df = self.streaming_df.withColumn("value", col("value").cast("binary"))

        # Start the streaming query
        query = df.writeStream \
            .foreach(self.__process_message()) \
            .start()

        query.awaitTermination()