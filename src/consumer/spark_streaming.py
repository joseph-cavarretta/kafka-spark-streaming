from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Consumer
import json

# Configuration for Kafka and Schema Registry
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
KAFKA_TOPIC = 'your_topic_name'
GROUP_ID = 'your_consumer_group'

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Kafka Avro Consumer") \
    .getOrCreate()

# Schema Registry Client setup
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Retrieve the latest schema for the topic
subject = f"{KAFKA_TOPIC}-value"
schema = schema_registry_client.get_latest_version(subject).schema

avro_deserializer = AvroDeserializer(schema_str=schema.schema_str, schema_registry_client=schema_registry_client)


# Define Kafka source with PySpark
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Define a function to deserialize Avro messages
def deserialize_avro_message(avro_data):
    return avro_deserializer(avro_data, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))

# Convert binary to string for deserialization
streaming_df = streaming_df.withColumn("value", col("value").cast("binary"))
streaming_df = streaming_df.select(col("key").cast("string"), col("value").alias("avro_value"))

# Process messages (replace `your_schema_path` with the actual schema if needed)
def process_row(row):
    avro_data = row.avro_value
    deserialized_message = deserialize_avro_message(avro_data)
    print(deserialized_message)

# Start the streaming query
query = streaming_df \
    .writeStream \
    .foreach(process_row) \
    .start()

query.awaitTermination()
