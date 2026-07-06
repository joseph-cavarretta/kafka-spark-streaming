import logging
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_incrementing,
)

from config import KafkaSettings

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


class MockAvroProducer:
    """Kafka Avro producer that generates and publishes synthetic user events."""

    def __init__(self, settings: KafkaSettings, schema_path: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.settings = settings
        self.schema_path = Path(schema_path)
        self.schema_client = self._get_schema_client()
        self.schema_id = self._register_schema()
        self.schema = self._get_schema()

    def _get_schema_client(self) -> CachedSchemaRegistryClient:
        """Instantiate a cached schema registry client."""
        self.logger.info(
            "Retrieving cached schema registry client for %s",
            self.settings.schema_registry_url,
        )
        return CachedSchemaRegistryClient({"url": self.settings.schema_registry_url})

    def _register_schema(self) -> int:
        """Load and register the Avro schema with the schema registry.

        Returns:
            The integer schema ID assigned by the registry.

        Raises:
            FileNotFoundError: If the schema file does not exist.
            avro.error.ClientError: If all registration attempts fail.
        """
        if not self.schema_path.is_file():
            raise FileNotFoundError(
                f"No schema file found at {self.schema_path}. Schema file is required."
            )
        self.logger.info("Registering schema from %s ...", self.schema_path)
        schema = avro.loads(self.schema_path.read_text())
        return self._register_with_retry(schema)

    @retry(
        retry=retry_if_exception_type(avro.error.ClientError),
        stop=stop_after_attempt(3),
        wait=wait_incrementing(start=1, increment=1),
        reraise=True,
    )
    def _register_with_retry(self, schema: object) -> int:
        """Register the schema, retrying on transient registry errors."""
        schema_id: int = self.schema_client.register(self.settings.kafka_topic, schema)
        return schema_id

    def _get_schema(self) -> object:
        """Fetch the registered schema object by ID."""
        self.logger.info("Fetching registered schema with id %d ...", self.schema_id)
        return self.schema_client.get_by_id(self.schema_id)

    def avro_producer(self) -> AvroProducer:
        """Build and return a configured AvroProducer."""
        self.logger.info(
            "Setting up Avro Producer for %s ...", self.settings.kafka_broker_url
        )
        return AvroProducer(
            {"bootstrap.servers": self.settings.kafka_broker_url},
            schema_registry=self.schema_client,
            default_value_schema=self.schema,
        )

    def generate_data(self, event_id: int) -> dict[str, Any]:
        """Generate a single synthetic user event record.

        Args:
            event_id: Sequential identifier for the event.

        Returns:
            Dict matching the events Avro schema.
        """
        devices = ["mobile", "tablet", "laptop"]
        events = ["click", "pageview", "login", "download"]
        users = [str(uuid.uuid4()) for _ in range(100)]
        data = {
            "event_timestamp": int(time.time()),
            "event_id": event_id,
            "event_type": random.choice(events),
            "device_type": random.choice(devices),
            "user_id": random.choice(users),
        }
        self.logger.info("Message generated: %s", data)
        return data
