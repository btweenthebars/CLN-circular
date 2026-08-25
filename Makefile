BINARY=circular
MAIN=./cmd/circular

.PHONY: all build test clean

all: build

build:
	go build -o $(BINARY) $(MAIN)

test:
	go test -v ./...

clean:
	rm -f $(BINARY)
