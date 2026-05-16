import json
import logging
import os
import sys
import time
import uuid
import random

from confluent_kafka import avro
from confluent_kafka.avro import CachedSchemaRegistryClient, AvroProducer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


class MockAvroProducer:
    """Kafka Avro producer that generates and publishes synthetic user events."""

    def __init__(self, config_path: str, schema_path: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.config_path = config_path
        self.schema_path = schema_path
        self.config = self._get_config()
        self.topic = self._get_topic()
        self.schema_client = self._get_schema_client()
        self.schema_id = self._register_schema()
        self.schema = self._get_schema()

    def _get_config(self) -> dict:
        """Load producer configuration from the JSON config file."""
        if not os.path.isfile(self.config_path):
            raise FileNotFoundError(
                f"No config file found at {self.config_path}. Config file is required."
            )
        self.logger.info("Reading config file for producer at %s ...", self.config_path)
        with open(self.config_path) as f:
            return json.load(f)

    def _get_topic(self) -> str:
        """Validate and return the Kafka topic name from config."""
        topic = self.config["kafka_topic"]
        if isinstance(topic, list):
            raise TypeError("Lists of topics not supported. Topic must be a string.")
        if not isinstance(topic, str):
            raise TypeError("Topic must be a string.")
        return topic

    def _get_schema_client(self) -> CachedSchemaRegistryClient:
        """Instantiate a cached schema registry client."""
        self.logger.info(
            "Retrieving cached schema registry client for %s",
            self.config["schema_registry_url"],
        )
        return CachedSchemaRegistryClient({"url": self.config["schema_registry_url"]})

    def _register_schema(self, max_retries: int = 2) -> int:
        """Register the Avro schema with the schema registry, with retries.

        Args:
            max_retries: Number of additional attempts after the first failure.

        Returns:
            The integer schema ID assigned by the registry.

        Raises:
            FileNotFoundError: If the schema file does not exist.
            avro.error.ClientError: If all registration attempts fail.
        """
        if not os.path.isfile(self.schema_path):
            raise FileNotFoundError(
                f"No schema file found at {self.schema_path}. Schema file is required."
            )
        self.logger.info("Registering schema from %s ...", self.schema_path)
        with open(self.schema_path) as f:
            schema = avro.loads(f.read())

        for attempt in range(max_retries + 1):
            try:
                self.logger.info("Schema registration attempt #%d ...", attempt + 1)
                return self.schema_client.register(self.topic, schema)
            except avro.error.ClientError as err:
                self.logger.error(err)
                if attempt < max_retries:
                    backoff = attempt + 1
                    self.logger.info("Retrying in %d seconds", backoff)
                    time.sleep(backoff)
                else:
                    raise

    def _get_schema(self) -> object:
        """Fetch the registered schema object by ID."""
        self.logger.info("Fetching registered schema with id %d ...", self.schema_id)
        return self.schema_client.get_by_id(self.schema_id)

    def avro_producer(self) -> AvroProducer:
        """Build and return a configured AvroProducer."""
        self.logger.info(
            "Setting up Avro Producer for %s ...", self.config["kafka_broker_url"]
        )
        return AvroProducer(
            {"bootstrap.servers": self.config["kafka_broker_url"]},
            schema_registry=self.schema_client,
            default_value_schema=self.schema,
        )

    def generate_data(self, event_id: int) -> dict:
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
            "event_timestamp": time.time(),
            "event_id": event_id,
            "event_type": random.choice(events),
            "device_type": random.choice(devices),
            "user_id": random.choice(users),
        }
        self.logger.info("Message generated: %s", data)
        return data
