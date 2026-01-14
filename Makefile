all: lint test build

test: lint
	go test -timeout=10s -cover -race -bench=. -benchmem ./...

build:
	CGO_ENABLED=0 go build -ldflags="-w -s" -o iam-t1-exporter .

lint: bin/golangci-lint
	go fmt ./...
	go vet ./...
	bin/golangci-lint run ./...
	go mod tidy

bin/golangci-lint: bin
	GOBIN=$(PWD)/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.8.0

bin:
	mkdir -p bin

setup: bin/golangci-lint
	go mod download

image-build:
	docker build -t iam-t1-exporter .

clean:
	rm -rf bin iam-t1-exporter

.PHONY: all test build lint setup image-build clean
