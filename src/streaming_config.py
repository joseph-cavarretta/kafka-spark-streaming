from pathlib import Path

from pydantic import BaseModel, ConfigDict


class StreamingError(Exception):
    """Base for every error raised by the producer and consumer."""


class ConfigNotFoundError(StreamingError):
    """A required config or schema file is missing. Permanent: fix the deployment."""


class StreamingConfig(BaseModel):
    """Connection settings shared by the producer and consumer (config.json)."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    schema_registry_url: str
    kafka_broker_url: str
    kafka_topic: str
    kafka_group_id: str


def require_file(path: Path, what: str) -> Path:
    """Return path, or raise ConfigNotFoundError naming the missing file."""
    if not path.is_file():
        raise ConfigNotFoundError(f"No {what} file found at {path}. It is required.")
    return path


def load_config(path: Path) -> StreamingConfig:
    """Parse and validate config.json; a list or missing kafka_topic is rejected."""
    return StreamingConfig.model_validate_json(require_file(path, "config").read_text())
