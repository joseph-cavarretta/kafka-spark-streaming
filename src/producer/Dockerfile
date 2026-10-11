FROM python:3.12.1-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --only-group producer

COPY src/producer/producer.py src/producer/publish.py src/streaming_config.py \
     src/config.json src/events.avsc /app/

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT [ "python3", "/app/publish.py" ]
