.PHONY: install lint format check type-check up down logs ps cassandra-shell

# --- dev environment ---

install:
	uv sync

lint:
	uv run ruff check .

format:
	uv run ruff format .

type-check:
	uv run mypy .

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run python ../agent-dev-harness/python-styleguide/docstring_length.py .
	uv run mypy .

# --- stack ---

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f python-producer spark

ps:
	docker compose ps

cassandra-shell:
	docker exec -it cassandra cqlsh
