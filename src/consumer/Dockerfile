FROM bitnamilegacy/spark:3
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

USER root
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --only-group consumer

COPY src/consumer/spark_streaming.py src/streaming_config.py src/config.json \
     src/events.avsc /app/

ENV PATH="/app/.venv/bin:$PATH" PYSPARK_PYTHON=/app/.venv/bin/python

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0", "/app/spark_streaming.py"]
