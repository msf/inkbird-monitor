all: lint test inkbird-monitor
.PHONY: all test lint setup image-build clean

test: lint
	go test -timeout=10s -cover -race -bench=. -benchmem ./...

inkbird-monitor: main.go go.mod Makefile
	CGO_ENABLED=0 go build -ldflags="-w -s" -o inkbird-monitor .

lint: bin/golangci-lint
	go fmt ./...
	go vet ./...
	bin/golangci-lint run ./...
	go mod tidy

GOLANGCI_LINT_VERSION := v2.10.1

bin/golangci-lint: Makefile | bin
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b bin $(GOLANGCI_LINT_VERSION)

bin:
	mkdir -p bin

setup: bin/golangci-lint
	go mod download

image-build:
	docker build -t inkbird-monitor .

clean:
	rm -rf bin inkbird-monitor

