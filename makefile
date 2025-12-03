### ----------------------
### Config
### ----------------------
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=news_match
DB_HOST=localhost
DB_PORT=5432

CONTAINER_NAME=pgvector

MIGRATIONS_DIR=./migrations

### ----------------------
### Help
### ----------------------
.PHONY: help
help:
	@echo "DB Commands:"
	@echo "  make db-up          Start pgvector DB container"
	@echo "  make db-down        Stop database"
	@echo "  make db-logs        View logs"
	@echo "  make db-psql        Open psql shell"
	@echo "  make migrate        Apply migrations"
	@echo "  make migrate-new name=<name>   Create new migration file"
	@echo "  make db-reset       Drop + recreate database + migrations"

### ----------------------
### Docker Commands
### ----------------------

db-up:
	docker compose up -d pgvector

db-down:
	docker compose stop pgvector

db-logs:
	docker compose logs -f pgvector

db-psql:
	docker exec -it $(CONTAINER_NAME) psql -U $(DB_USER) -d $(DB_NAME)

### ----------------------
### Migrations
### ----------------------

migrate:
	@echo "Applying migrations..."
	docker exec -i $(CONTAINER_NAME) psql -U $(DB_USER) -d $(DB_NAME) < $(MIGRATIONS_DIR)/0001_init.sql
	@echo "Migrations applied."

migrate-new:
	@if [ -z "$(name)" ]; then \
		echo "Usage: make migrate-new name=add-xyz"; exit 1; \
	fi
	@timestamp=$$(date +%s); \
	filename="$(MIGRATIONS_DIR)/$${timestamp}_$(name).sql"; \
	echo "-- Migration: $(name)" > $$filename; \
	echo "Created $$filename"

db-reset:
	@echo "🔻 Dropping database..."
	docker exec $(CONTAINER_NAME) dropdb -U $(DB_USER) --if-exists $(DB_NAME)
	@echo "🔺 Creating database..."
	docker exec $(CONTAINER_NAME) createdb -U $(DB_USER) $(DB_NAME)
	make migrate
