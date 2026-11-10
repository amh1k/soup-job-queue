.PHONY: build test test-integration vet up down migrate run

build:
	go build -o bin/api ./cmd/api
	go build -o bin/migrate ./cmd/migrate

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
