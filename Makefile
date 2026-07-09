.PHONY: build test tidy

build:
		go build ./...

test:
		go test ./... -v

tidy:
		go mod tidy
