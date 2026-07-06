from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    schema_registry_url: str = Field(
        default="http://schema-registry:8081",
        description="Confluent Schema Registry URL",
    )
    kafka_broker_url: str = Field(
        default="kafka:9092", description="Kafka bootstrap server address"
    )
    kafka_topic: str = Field(default="events-topic", description="Kafka topic name")
    kafka_group_id: str = Field(
        default="events-group-1", description="Kafka consumer group ID"
    )


def get_settings() -> KafkaSettings:
    """Return a KafkaSettings instance loaded from the environment."""
    return KafkaSettings()
