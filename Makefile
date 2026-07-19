.PHONY: build test test-integration tidy redis-up redis-down

build:
	go build ./...

test:
	go test ./... -v

test-integration: redis-up
	go test ./... -tags=integration -v

tidy:
	go mod tidy

redis-up:
	docker compose -f deploy/docker-compose.yml up -d
	sleep 1

redis-down:
	docker compose -f deploy/docker-compose.yml down