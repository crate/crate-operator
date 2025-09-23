#!/bin/bash

# DC Util Container Management Script
# This script orchestrates Docker container builds, registry operations, and version management
# for multi-version dc_util deployments

set -e

# Configuration
REGISTRY="${REGISTRY:-cloud.registry.cr8.net}"
IMAGE_NAME="${IMAGE_NAME:-dc-util-fileserver}"
GITHUB_REPO="${GITHUB_REPO:-https://github.com/crate/crate-operator.git}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
DC Util Container Management Script

Usage: $0 [OPTIONS] COMMAND

Commands:
  list-tags          List available git tags from repository
  build              Build multi-version container
  push               Push container to registry
  deploy             Build and push container
  clean              Clean local images

Options:
  -v, --versions     Comma-separated list of versions (default: latest)
  -t, --tag          Container tag (default: multi)
  -r, --registry     Container registry (default: cloud.registry.cr8.net)
  -h, --help         Show this help

Examples:
  # Build with latest only
  $0 build

  # Build with specific versions
  $0 -v "v0.0.1,v0.0.2,latest" build

  # Build and push with custom tag
  $0 -v "v0.0.1,v0.0.2,latest" -t "v1.0.0" deploy

  # List available versions from git
  $0 list-tags

Environment Variables:
  REGISTRY          Container registry (default: cloud.registry.cr8.net)
  IMAGE_NAME        Image name (default: dc-util-fileserver)
  GITHUB_REPO       Git repository URL
EOF
}

list_git_tags() {
    log_info "Fetching available tags from repository..."

    # Check if git is available
    if ! command -v git &> /dev/null; then
        log_error "Git is not installed"
        exit 1
    fi

    # Create temp directory
    TEMP_DIR=$(mktemp -d)
    trap "rm -rf $TEMP_DIR" EXIT

    # Clone repo and list tags
    log_info "Cloning repository to check tags..."
    if git clone --quiet --bare "$GITHUB_REPO" "$TEMP_DIR/repo.git"; then
        log_info "Available versions (git tags):"
        cd "$TEMP_DIR/repo.git"
        git tag --sort=-version:refname | head -20 | while read -r tag; do
            echo "  - $tag"
        done

        echo ""
        log_info "Latest commits on main branch:"
        git log --oneline -5 main 2>/dev/null || git log --oneline -5 master 2>/dev/null || echo "  No main/master branch found"
    else
        log_error "Failed to clone repository"
        exit 1
    fi
}

build_container() {
    local versions="$1"
    local tag="$2"
    local full_image="$REGISTRY/$IMAGE_NAME:$tag"

    log_info "Building multi-version container with versions: $versions"
    log_info "Target image: $full_image"

    # Check if Dockerfile.multi exists
    if [[ ! -f "Dockerfile.multi" ]]; then
        log_error "Dockerfile.multi not found in current directory"
        exit 1
    fi

    # Build the container
    log_info "Starting Docker build..."
    if docker build \
        --build-arg VERSIONS="$versions" \
        --build-arg GITHUB_REPO="$GITHUB_REPO" \
        -f Dockerfile.multi \
        -t "$full_image" \
        .; then
        log_success "Container built successfully: $full_image"

        # Show image size
        IMAGE_SIZE=$(docker images --format "table {{.Size}}" "$full_image" | tail -1)
        log_info "Image size: $IMAGE_SIZE"

        # Show contained versions
        log_info "Verifying container contents..."
        docker run --rm "$full_image" ls -la /srv/binaries/ || log_warning "Could not list container contents"

    else
        log_error "Failed to build container"
        exit 1
    fi
}

push_container() {
    local tag="$1"
    local full_image="$REGISTRY/$IMAGE_NAME:$tag"

    log_info "Pushing container: $full_image"

    if docker push "$full_image"; then
        log_success "Container pushed successfully"
    else
        log_error "Failed to push container"
        exit 1
    fi
}

clean_images() {
    log_info "Cleaning local images..."

    # Remove all versions of the image
    docker images --format "{{.Repository}}:{{.Tag}}" | grep "$REGISTRY/$IMAGE_NAME" | while read -r image; do
        log_info "Removing image: $image"
        docker rmi "$image" || log_warning "Failed to remove $image"
    done

    log_success "Cleanup completed"
}

# Parse command line arguments
VERSIONS="latest"
TAG="multi"
COMMAND=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--versions)
            VERSIONS="$2"
            shift 2
            ;;
        -t|--tag)
            TAG="$2"
            shift 2
            ;;
        -r|--registry)
            REGISTRY="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        list-tags|build|push|deploy|clean)
            COMMAND="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate command
if [[ -z "$COMMAND" ]]; then
    log_error "No command specified"
    show_help
    exit 1
fi

# Execute command
case $COMMAND in
    list-tags)
        list_git_tags
        ;;
    build)
        build_container "$VERSIONS" "$TAG"
        ;;
    push)
        push_container "$TAG"
        ;;
    deploy)
        build_container "$VERSIONS" "$TAG"
        push_container "$TAG"
        ;;
    clean)
        clean_images
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        exit 1
        ;;
esac

log_success "Container operation completed successfully"
