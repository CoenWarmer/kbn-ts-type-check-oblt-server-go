.PHONY: build run test clean tidy

BIN := bin/server

build:
	go build -o $(BIN) .

run:
	DESTINATION_TYPE=local DESTINATION_PATH=./data/artifacts go run .

test:
	go test ./...

tidy:
	go mod tidy

clean:
	rm -rf $(BIN) bin/
