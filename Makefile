.PHONY: up down logs ps cassandra-shell

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
