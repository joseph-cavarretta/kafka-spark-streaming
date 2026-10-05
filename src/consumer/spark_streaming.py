import logging
import sys
from pathlib import Path

import pyspark.sql.functions as sql
from confluent_kafka.schema_registry import RegisteredSchema, SchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession

from streaming_config import load_config

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config.json")


def get_spark_session(session_name: str) -> SparkSession:
    """Create or retrieve the active SparkSession."""
    return SparkSession.builder.appName(session_name).getOrCreate()


def get_latest_schema(schema_registry_url: str, topic: str) -> RegisteredSchema:
    """Fetch the latest registered schema version for a topic (its subject)."""
    client = SchemaRegistryClient({"url": schema_registry_url})
    return client.get_latest_version(topic)


def create_streaming_df(
    spark: SparkSession,
    kafka_topic: str,
    kafka_broker_url: str,
    kafka_group_id: str,
) -> DataFrame:
    """Create a Structured Streaming DataFrame over the topic, from the earliest offset.

    The frame carries Kafka's metadata columns alongside the binary message value.
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


def write_to_cassandra(batch_df: DataFrame) -> None:
    """Write a micro-batch DataFrame to the Cassandra events table."""
    batch_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="user_actions", keyspace="events"
    ).save()


def main() -> None:
    """Stream the topic's raw messages into Cassandra until the job is stopped."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logger.info("Reading config file at %s", CONFIG_PATH)
    config = load_config(CONFIG_PATH)

    spark = get_spark_session("Spark Avro Consumer")
    spark.sparkContext.setLogLevel("WARN")

    # Fails fast when the topic has no registered schema; the stream itself writes
    # the raw Avro bytes.
    get_latest_schema(config.schema_registry_url, config.kafka_topic)

    df = create_streaming_df(
        spark, config.kafka_topic, config.kafka_broker_url, config.kafka_group_id
    )
    processed_df = df.withColumn("value", sql.col("value").cast("binary"))
    processed_df.writeStream.foreachBatch(
        lambda batch_df, _: write_to_cassandra(batch_df)
    ).start().awaitTermination()


if __name__ == "__main__":
    main()
