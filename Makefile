.PHONY: test lint up down migrate

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
