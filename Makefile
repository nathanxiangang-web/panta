.PHONY: build test db-migrate db-status test-integration

build:
	go build -o bin/panta ./cmd/panta

test:
	go test ./...

db-migrate:
	go run ./cmd/panta-db migrate

db-status:
	go run ./cmd/panta-db status

test-integration:
	test -n "$$PANTA_TEST_DATABASE_URL"
	go test ./internal/store/postgres -run Postgres -count=1
