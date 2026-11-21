.PHONY: test lint up down migrate e2e

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

up:
	docker compose up -d --build

down:
	docker compose down -v

migrate:
	docker compose run --rm migrate

# Starts the full stack with fast retries and runs the end-to-end tests
# against it. Stop it afterwards with `make down`.
e2e:
	RETRY_BASE=50ms docker compose up -d --build
	go test -tags e2e -count=1 -v ./e2e/
