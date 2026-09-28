.PHONY: build test

build:
	go build -o bin/panta ./cmd/panta

test:
	go test ./...
