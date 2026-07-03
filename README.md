# Kafka Spark Streaming

[![CI](https://github.com/joseph-cavarretta/kafka-spark-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/joseph-cavarretta/kafka-spark-streaming/actions/workflows/ci.yml)

A real-time event streaming pipeline that ingests mock user behavior events, processes them with Apache Spark Structured Streaming, and persists results to Apache Cassandra.

## Architecture

```
┌──────────────────┐     Avro      ┌─────────────────┐     Structured    ┌───────────────┐
│  Python Producer │ ─────────── ► │  Apache Kafka   │ ─── Streaming ──► │   Cassandra   │
│  (mock events)   │               │  + Schema Reg.  │                   │  (warehouse)  │
└──────────────────┘               └─────────────────┘                   └───────────────┘
                                          ▲
                                   ┌──────┴──────┐
                                   │  Zookeeper  │
                                   └─────────────┘
```

## Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka (Bitnami) |
| Schema registry | Confluent Schema Registry |
| Schema format | Apache Avro |
| Stream processor | Apache Spark 3 Structured Streaming |
| Data warehouse | Apache Cassandra |
| Orchestration | Docker Compose |

## Event Schema

Events are serialized with Avro using the schema defined in `src/events.avsc`:

| Field | Type | Description |
|---|---|---|
| `event_id` | int | Sequential event identifier |
| `event_timestamp` | long | Unix timestamp of the event |
| `event_type` | string | One of: `click`, `pageview`, `login`, `download` |
| `device_type` | string | One of: `mobile`, `tablet`, `laptop` |
| `user_id` | string | UUID identifying the user |

## Quick Start

```bash
# start all services
make up

# tail logs from the producer and spark consumer
make logs

# stop all services and remove volumes
make down
```

Services start in dependency order: Zookeeper → Kafka → Schema Registry → Spark + Cassandra → Producer.
The producer waits 2 minutes after startup before emitting events to allow Spark to initialize.

## Development

Dependencies are managed with [uv](https://docs.astral.sh/uv/):

```bash
make install     # uv sync
make check       # ruff lint + format check + mypy
```

## Services

| Service | Port | Description |
|---|---|---|
| Zookeeper | 2181 | Kafka coordination |
| Kafka broker | 9092 | Message broker |
| Schema Registry | 8081 | Avro schema management |
| Spark master | 8080 | Spark UI |
| Spark master | 7077 | Spark cluster port |
| Cassandra | 9042 | CQL query interface |

## Cassandra Schema

Events are written to the `events` keyspace:

```sql
CREATE TABLE user_actions (
    event_timestamp BIGINT,
    event_id        INT,
    event_type      TEXT,
    device_type     TEXT,
    user_id         TEXT,
    PRIMARY KEY ((user_id), event_timestamp)
) WITH CLUSTERING ORDER BY (event_timestamp DESC);
```

Partitioned by `user_id` with events ordered by recency per user.

## Querying Results

Connect to Cassandra and query the sink table:

```bash
docker exec -it cassandra cqlsh

USE events;
SELECT * FROM user_actions LIMIT 10;
SELECT * FROM user_actions WHERE user_id = '<uuid>';
```

## Project Structure

```
├── docker-compose.yml        # full stack orchestration
├── src/
│   ├── events.avsc           # Avro schema definition
│   ├── config.json           # broker and registry URLs
│   ├── producer/
│   │   ├── producer.py       # MockAvroProducer class
│   │   ├── publish.py        # entrypoint: generates and sends events
│   │   └── Dockerfile
│   ├── consumer/
│   │   ├── spark_streaming.py # Spark Structured Streaming consumer
│   │   └── Dockerfile
│   └── cassandra/
│       └── setup.sql         # keyspace and table DDL
```
