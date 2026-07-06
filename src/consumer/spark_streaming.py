import logging
import sys
from typing import Any

import pyspark.sql.functions as sql
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyspark.sql import DataFrame, SparkSession

from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def get_spark_session(session_name: str) -> SparkSession:
    """Create or retrieve the active SparkSession."""
    return SparkSession.builder.appName(session_name).getOrCreate()


def get_latest_schema(schema_registry_url: str, topic: str) -> Any:
    """Fetch the latest registered schema version for a topic.

    Args:
        schema_registry_url: URL of the Confluent Schema Registry.
        topic: Kafka topic name used as the schema subject.

    Returns:
        RegisteredSchema object from the registry.
    """
    client = SchemaRegistryClient({"url": schema_registry_url})
    return client.get_latest_version(topic)


def create_streaming_df(
    spark: SparkSession,
    kafka_topic: str,
    kafka_broker_url: str,
    kafka_group_id: str,
) -> DataFrame:
    """Create a Spark Structured Streaming DataFrame reading from Kafka.

    Args:
        spark: Active SparkSession.
        kafka_topic: Topic to subscribe to.
        kafka_broker_url: Kafka bootstrap server address.
        kafka_group_id: Consumer group ID.

    Returns:
        Streaming DataFrame with Kafka metadata columns.
    """
    logger.info("Initializing Spark streaming DataFrame")
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker_url)
        .option("subscribe", kafka_topic)
        .option("group.id", kafka_group_id)
        .option("startingOffsets", "earliest")
        .load()
    )


def deserialize_message(
    row: Any,
    schema_client: SchemaRegistryClient,
    schema_str: str,
    topic: str,
) -> dict[str, Any]:
    """Deserialize a single Avro-encoded Kafka message value.

    Args:
        row: A Spark Row with a binary value column.
        schema_client: Initialized SchemaRegistryClient.
        schema_str: Avro schema string for deserialization.
        topic: Kafka topic name for serialization context.

    Returns:
        Deserialized message as a dict.
    """
    deserializer = AvroDeserializer(
        schema_registry_client=schema_client, schema_str=schema_str
    )
    message: dict[str, Any] = deserializer(
        row.value, SerializationContext(topic, MessageField.VALUE)
    )
    return message


def write_to_cassandra(batch_df: DataFrame) -> None:
    """Write a micro-batch DataFrame to the Cassandra events table."""
    batch_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="user_actions", keyspace="events"
    ).save()


if __name__ == "__main__":
    settings = get_settings()

    schema_registry_url = settings.schema_registry_url
    kafka_broker_url = settings.kafka_broker_url
    kafka_topic = settings.kafka_topic
    kafka_group_id = settings.kafka_group_id

    spark = get_spark_session("Spark Avro Consumer")
    spark.sparkContext.setLogLevel("WARN")

    latest_schema = get_latest_schema(schema_registry_url, kafka_topic)
    schema_broadcast = spark.sparkContext.broadcast(
        (latest_schema.schema.schema_str, kafka_topic)
    )

    df = create_streaming_df(spark, kafka_topic, kafka_broker_url, kafka_group_id)
    processed_df = df.withColumn("value", sql.col("value").cast("binary"))

    processed_df.writeStream.foreachBatch(
        lambda batch_df, _: write_to_cassandra(batch_df)
    ).start().awaitTermination()
