import os
import sys
import json
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as sql
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

logger = logging.getLogger(__name__)


def get_spark_session(session_name):
    return SparkSession.builder \
            .appName(session_name) \
            .getOrCreate()


def get_config(config_path):
    if os.path.isfile(config_path):
        logger.info(f"Reading config file at {config_path}")
        with open(config_path) as f:
            json_str = json.load(f)
            validate_config(json_str)
            return json_str
    else:
        raise FileNotFoundError(f"No config file found at {config_path}. Config file is required.")
    

def validate_config(config):
    if not config['kafka_topic']:
        raise KeyError(f"kafka_topic key not found in config.")
    
    if isinstance(config['kafka_topic'], list):
        raise TypeError("Lists of topics not supported. Topic must be a string")
    
    if not isinstance(config['kafka_topic'], str):
        raise TypeError("Topic must be a string")


def get_latest_schema(schema_registry_url, topic):
    """Fetch RegisteredSchema obj from the Schema Registry."""
    subject = topic
    schema_client = SchemaRegistryClient({"url": schema_registry_url})
    latest_schema = schema_client.get_latest_version(subject)
    return latest_schema


def get_avro_deserializer(schema_registry_url, topic):
    """Create and return an Avro deserializer."""
    subject = topic
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    schema = schema_registry_client.get_latest_version(subject).schema

    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema.schema_str
    )
    return avro_deserializer, schema_registry_client


def create_streaming_df(kafka_topic, kafka_broker_url, kafka_group_id):
    logger.info("Initializing spark dataframe")
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker_url) \
        .option("subscribe", kafka_topic) \
        .option("group.id", kafka_group_id) \
        .option("startingOffsets", "earliest") \
        .load()


def deserialize_message(row, schema_client, schema_str, topic):
    """Process a single Kafka message."""
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_client, schema_str=schema_str
    )
    avro_data = row.value
    deserialized_message = avro_deserializer(
        avro_data,
        SerializationContext(topic, MessageField.VALUE)
    )
    return deserialized_message


if __name__ == "__main__":
    config_path = 'config.json'
    config = get_config(config_path)

    schema_registry_url = config.get("schema_registry_url", False)
    kafka_broker_url = config.get("kafka_broker_url", False)
    kafka_topic = config.get("kafka_topic", False)
    kafka_group_id = config.get("kafka_group_id", False)
    
    spark = get_spark_session("Spark Avro Consumer")
    spark.sparkContext.setLogLevel("WARN") 
    # Get latest schema from schema registry
    latest_schema = get_latest_schema(schema_registry_url, kafka_topic)
    
    # Broadcast the schema and topic to all executors
    schema_broadcast = spark.sparkContext.broadcast((latest_schema.schema.schema_str, kafka_topic))

    df = create_streaming_df(kafka_topic, kafka_broker_url, kafka_group_id)

    def write_to_stdout(row):
        schema_registry_client = SchemaRegistryClient({"url": "http://schema-registry:8081"})
        schema_str, topic = schema_broadcast.value
        deserialized_message = deserialize_message(
            row,
            schema_registry_client,
            schema_str,
            topic
        )
        print(f"Deserialized Message: {deserialized_message}")

    def write_to_cassandra(df):
        df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="user_actions", keyspace="events") \
        .save()
    
    # Cast the "value" column to binary
    processed_df = df.withColumn("value", sql.col("value").cast("binary"))

    # Start the streaming query
    query = processed_df.writeStream \
        .foreach(write_to_cassandra) \
        .start() \
        .awaitTermination()