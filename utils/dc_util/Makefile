# Minimalistic Makefile for dc_util
# Builds both x86_64 and ARM64 binaries for testing

BINARY_NAME = dc_util
GO_FILE = dc_util.go

# Default target
all: build-amd64 build-arm64

# Build for Linux x86_64/amd64
build-amd64:
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux-amd64 $(GO_FILE)

# Build for Linux ARM64
build-arm64:
	GOOS=linux GOARCH=arm64 go build -o $(BINARY_NAME)-linux-arm64 $(GO_FILE)

# Clean built binaries
clean:
	rm -f $(BINARY_NAME)-linux-amd64 $(BINARY_NAME)-linux-arm64

# Test
test:
	go test -v

# Build for current platform (development)
dev:
	go build -o $(BINARY_NAME) $(GO_FILE)

.PHONY: all build-amd64 build-arm64 clean test dev
