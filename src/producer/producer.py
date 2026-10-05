import logging
import random
import time
import uuid
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from pydantic import BaseModel, ConfigDict
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_incrementing,
)

from streaming_config import StreamingConfig, require_file

logger = logging.getLogger(__name__)

DEVICES = ("mobile", "tablet", "laptop")
EVENTS = ("click", "pageview", "login", "download")
USERS_PER_EVENT = 100
# One first attempt plus two retries, waiting 1s then 2s.
SCHEMA_REGISTRATION_ATTEMPTS = 3


class UserEvent(BaseModel):
    """One synthetic user event, shaped like the events.avsc schema."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    event_timestamp: int
    event_id: int
    event_type: str
    device_type: str
    user_id: str


class MockAvroProducer:
    """Kafka Avro producer that generates and publishes synthetic user events."""

    def __init__(self, config: StreamingConfig, schema_path: Path) -> None:
        self.config = config
        self.topic = config.kafka_topic
        logger.info(
            "Retrieving cached schema registry client for %s",
            config.schema_registry_url,
        )
        self.schema_client = CachedSchemaRegistryClient(
            {"url": config.schema_registry_url}
        )
        self.schema_id = self._register_schema(schema_path)
        logger.info("Fetching registered schema with id %d ...", self.schema_id)
        self.schema = self.schema_client.get_by_id(self.schema_id)

    def _register_schema(self, schema_path: Path) -> int:
        """Register the Avro schema with the registry and return its id."""
        require_file(schema_path, "schema")
        logger.info("Registering schema from %s ...", schema_path)
        schema = avro.loads(schema_path.read_text())

        @retry(
            retry=retry_if_exception_type(avro.error.ClientError),
            stop=stop_after_attempt(SCHEMA_REGISTRATION_ATTEMPTS),
            wait=wait_incrementing(start=1, increment=1),
            before_sleep=before_sleep_log(logger, logging.ERROR),
            reraise=True,
        )
        def register() -> int:
            schema_id: int = self.schema_client.register(self.topic, schema)
            return schema_id

        return register()

    def avro_producer(self) -> AvroProducer:
        """Build and return a configured AvroProducer."""
        logger.info("Setting up Avro Producer for %s ...", self.config.kafka_broker_url)
        return AvroProducer(
            {"bootstrap.servers": self.config.kafka_broker_url},
            schema_registry=self.schema_client,
            default_value_schema=self.schema,
        )

    def generate_data(self, event_id: int) -> UserEvent:
        """Generate one synthetic event with a random type, device, and user."""
        users = [str(uuid.uuid4()) for _ in range(USERS_PER_EVENT)]
        # Mock data, so non-cryptographic randomness is fine.
        event = UserEvent(
            event_timestamp=int(time.time()),
            event_id=event_id,
            event_type=random.choice(EVENTS),  # noqa: S311
            device_type=random.choice(DEVICES),  # noqa: S311
            user_id=random.choice(users),  # noqa: S311
        )
        logger.info("Message generated: %s", event.model_dump())
        return event
