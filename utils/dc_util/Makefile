# Consolidated Makefile for dc_util
# Handles Go binary building, Docker image creation, and Kubernetes deployment

# Variables
BINARY_NAME = dc_util
GO_FILE = dc_util.go
REGISTRY = cloud.registry.cr8.net
IMAGE_NAME = dc-util-fileserver
TAG ?= latest
VERSIONS ?= latest
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(TAG)

# Default target
.PHONY: all
all: build-binaries

# Go binary targets
.PHONY: build-binaries
build-binaries: build-amd64 build-arm64

# Build for Linux x86_64/amd64
.PHONY: build-amd64
build-amd64:
	@echo "Building Linux AMD64 binary..."
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux-amd64 $(GO_FILE)

# Build for Linux ARM64
.PHONY: build-arm64
build-arm64:
	@echo "Building Linux ARM64 binary..."
	GOOS=linux GOARCH=arm64 go build -o $(BINARY_NAME)-linux-arm64 $(GO_FILE)

# Test Go code
.PHONY: test
test:
	@echo "Running Go tests..."
	go test -v

# Docker targets
.PHONY: docker-build
docker-build: build-binaries
	@echo "Copying binaries to deploy directory..."
	cp $(BINARY_NAME)-linux-amd64 deploy/
	cp $(BINARY_NAME)-linux-arm64 deploy/
	@echo "Building container image..."
	cd deploy && docker build -t $(FULL_IMAGE) .

# Multi-version Docker build
.PHONY: docker-build-multi
docker-build-multi:
	@echo "Building multi-version container with versions: $(VERSIONS)"
	docker build \
		--no-cache \
		--build-arg VERSIONS="$(VERSIONS)" \
		--build-arg GITHUB_REPO="https://github.com/crate/crate-operator.git" \
		-f deploy/Dockerfile.multi \
		-t $(REGISTRY)/$(IMAGE_NAME):multi \
		.

# List available git tags for multi-version builds
.PHONY: list-versions
list-versions:
	@echo "Available versions from git:"
	@cd deploy && ./manage-container.sh list-tags

# Build and push multi-version container
.PHONY: docker-deploy-multi
docker-deploy-multi: docker-build-multi docker-push-multi

# Push the container image
.PHONY: docker-push
docker-push:
	@echo "Pushing container image to registry..."
	docker push $(FULL_IMAGE)

# Push multi-version container
.PHONY: docker-push-multi
docker-push-multi:
	@echo "Pushing multi-version container to registry..."
	docker push $(REGISTRY)/$(IMAGE_NAME):multi

# Convenience target: build and push multi-version container
.PHONY: multi-deploy
multi-deploy: docker-build-multi docker-push-multi k8s-rollout-status

# Check rollout status of the fileserver deployment
.PHONY: k8s-rollout-status
k8s-rollout-status:
	@echo "Checking rollout status of dc-util-fileserver deployment..."
	kubectl rollout restart deployment dc-util-fileserver -n dcutil || true
	kubectl rollout status deployment dc-util-fileserver -n dcutil

# Convenience target: rebuild everything and deploy
.PHONY: rebuild-multi
rebuild-multi:
	@echo "Rebuilding and deploying multi-version container with versions: $(VERSIONS)"
	$(MAKE) docker-build-multi VERSIONS="$(VERSIONS)"
	$(MAKE) docker-push-multi
	$(MAKE) k8s-rollout-status

# Clean targets
.PHONY: clean-binaries
clean-binaries:
	@echo "Cleaning Go binaries..."
	rm -f $(BINARY_NAME)-linux-amd64 $(BINARY_NAME)-linux-arm64 $(BINARY_NAME)
	rm -f deploy/$(BINARY_NAME)-linux-amd64 deploy/$(BINARY_NAME)-linux-arm64

.PHONY: clean-docker
clean-docker:
	@echo "Removing local container images..."
	docker rmi $(FULL_IMAGE) || true

.PHONY: clean
clean: clean-binaries clean-docker

# Info and help
.PHONY: info
info:
	@echo "=== dc_util Build Information ==="
	@echo "Binary name: $(BINARY_NAME)"
	@echo "Go file: $(GO_FILE)"
	@echo "Registry: $(REGISTRY)"
	@echo "Image: $(IMAGE_NAME)"
	@echo "Tag: $(TAG)"
	@echo "Full image: $(FULL_IMAGE)"
	@echo ""
	@echo "=== Available binaries ==="
	@ls -la $(BINARY_NAME)-linux-* 2>/dev/null || echo "No binaries found (run 'make build-binaries')"

.PHONY: help
help:
	@echo "=== dc_util Makefile Help ==="
	@echo ""
	@echo "Go Binary Targets:"
	@echo "  build-binaries  - Build both AMD64 and ARM64 Linux binaries"
	@echo "  build-amd64     - Build Linux AMD64 binary"
	@echo "  build-arm64     - Build Linux ARM64 binary"
	@echo "  dev             - Build binary for current platform"
	@echo "  test            - Run Go tests"
	@echo ""
	@echo "Docker Targets:"
	@echo "  docker-build    - Build container image (includes binary build)"
	@echo "  docker-push     - Push container image to registry"
	@echo "  docker-deploy   - Build and push container image"
	@echo ""
	@echo "Complete Workflow:"
	@echo "  deploy          - Full deployment (build, push, apply)"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean-binaries  - Remove Go binaries"
	@echo "  clean-docker    - Remove local container images"
	@echo "  clean           - Clean everything"
	@echo ""
	@echo "Info:"
	@echo "  info            - Show build information"
	@echo "  help            - Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  TAG             - Image tag (default: latest)"
	@echo "  VERSIONS        - Comma-separated versions for multi-build (default: latest)"
	@echo ""
	@echo "Multi-version Targets:"
	@echo "  docker-build-multi  - Build multi-version container"
	@echo "  docker-push-multi   - Push multi-version container"
	@echo "  docker-deploy-multi - Build and push multi-version container"
	@echo "  multi-deploy        - Build, push, and rollout multi-version container"
	@echo "  rebuild-multi       - Complete rebuild and deploy with rollout status"
	@echo "  k8s-rollout-status  - Restart and check rollout status"
	@echo ""
	@echo "Examples:"
	@echo "  make build-binaries"
	@echo "  make docker-deploy TAG=v1.0.0"
	@echo "  make docker-build-multi VERSIONS='v0.0.1=crate-operator-crds-2.53.0,latest'"
	@echo "  make docker-push-multi"
	@echo "  make multi-deploy VERSIONS='v0.0.1=crate-operator-crds-2.53.0,latest'"
	@echo "  make rebuild-multi VERSIONS='latest'"
	@echo "  make k8s-rollout-status"
	@echo "  make deploy"
