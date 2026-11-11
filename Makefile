.PHONY: build test test-integration vet up down migrate run worker chaos load

build:
	go build -o bin/api ./cmd/api
	go build -o bin/migrate ./cmd/migrate
	go build -o bin/worker ./cmd/worker
	go build -o bin/chaos ./cmd/chaos
	go build -o bin/load ./cmd/load

test:
	go test ./internal/...

test-integration:
	@test -n "$$TEST_DATABASE_URL" || (echo 'TEST_DATABASE_URL must point to a disposable PostgreSQL database' >&2; exit 1)
	go test -count=1 -v ./migrations ./internal/jobs

vet:
	go vet ./...

up:
	docker compose up --build

down:
	docker compose down

migrate:
	go run ./cmd/migrate

run:
	go run ./cmd/api

worker:
	go run ./cmd/worker

chaos: build
	./bin/chaos

load: build
	./bin/load

# Requires Python 3.11+; preserves existing archives and checks their contents.
.PHONY: evidence
evidence:
	python3 scripts/package-evidence.py

# Isolated project: integration tests and startup/restart/persistence checks.
.PHONY: verify-deployment
verify-deployment:
	python3 scripts/verify-deployment.py
