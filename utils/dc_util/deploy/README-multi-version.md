# DC Util Multi-Version Deployment Guide

This guide explains how to build and deploy a multi-version dc_util fileserver that can serve different versions of the dc_util binary, enabling version pinning, rollbacks, and testing with different releases.

## 🎯 Overview

The multi-version system allows you to:

- **Serve multiple dc_util versions** from a single container
- **Pin StatefulSets to specific versions** for stability
- **Test new versions** without affecting production workloads
- **Roll back quickly** if issues are discovered
- **Maintain compatibility** across different CrateDB clusters

> **⚠️ IMPORTANT: Understanding "latest" Version**
>
> The **"latest"** version is **NOT** the highest version number from git tags. Instead:
>
> - **"latest"** = The source code from your **current working directory**
> - **Git tags** (e.g., `dcutil-0.0.1`) = Specific releases from the repository
>
> **Examples:**
>
> - `VERSIONS="dcutil-0.0.1,testing,latest"` → 3 versions: stable release, testing tag, + your working code
> - `VERSIONS="dcutil-0.0.1,testing"` → 2 versions: stable release + testing tag (no "latest")
>
> Use **"latest"** for development/testing your current changes before tagging a release.

## 📁 File Structure

```
dc_util/
├── deploy/
│   ├── Dockerfile.multi          # Multi-version container build
│   ├── manage-container.sh       # Container orchestration script
│   └── scripts/
│       └── compile-binaries.sh   # Binary compilation script
│   └── README-multi-version.md   # This file
├── sts-hooks-versioned.toml      # Version-aware configuration
└── Makefile                      # Updated with multi-version targets
```

## 🏗️ Container Structure

The multi-version container organizes binaries by version:

```
/srv/binaries/
├── v0.0.1/                       # Clean name, mapped from git tag "crate-operator-crds-2.53.0"
│   ├── dc_util-linux-amd64
│   ├── dc_util-linux-arm64
│   ├── dc_util-linux-amd64.sha256
│   └── dc_util-linux-arm64.sha256
├── latest/                       # Built from your working directory
│   ├── dc_util-linux-amd64       # ⚠️ NOT highest version number!
│   ├── dc_util-linux-arm64       # ⚠️ Your current source code!
│   ├── dc_util-linux-amd64.sha256
│   └── dc_util-linux-arm64.sha256
└── index.html                    # Version browser
```

## 🚀 Quick Start

### 1. List Available Versions

First, check what git tags are available:

```bash
# Using the container management script
cd deploy
./manage-container.sh list-tags

# Or using Makefile
cd ..
make list-versions
```

### 2. Build Multi-Version Container

Build with specific versions:

```bash
# Clean, semantic version names (recommended)
make docker-build-multi VERSIONS="v0.0.1=crate-operator-crds-2.53.0,latest"

# Or with ugly git tag names directly
make docker-build-multi VERSIONS="crate-operator-crds-2.53.0,latest"

# Or use the container management script for more control
cd deploy
./manage-container.sh -v "v0.0.1=crate-operator-crds-2.53.0,latest" build
```

> **📝 Note: Version Mapping Workaround**
>
> The mapping `v0.0.1=crate-operator-crds-2.53.0` is needed because the logical version `v0.0.1` doesn't have a corresponding git tag with dc_util source code. The actual dc_util source exists in the `crate-operator-crds-2.53.0` git tag.
>
> **This workaround works for any git tag:**
>
> - `v0.0.2=some-other-tag`
> - `stable=crate-operator-2.44.0`
> - `beta=testing-branch`
>
> This allows you to create clean, semantic version names that map to any git reference containing dc_util source code.

### 3. Deploy Multi-Version Container

```bash
# Build and push with clean version names
make docker-deploy-multi VERSIONS="v0.0.1=crate-operator-crds-2.53.0,latest"

# Deploy to Kubernetes
make k8s-apply
```

## 📝 Configuration Examples

### Basic Version Pinning

Pin a StatefulSet to a specific version:

```toml
# sts-hooks-versioned.toml
[dcutil]
version = "v0.0.1"  # Pin to stable release (clean version name)
base_url = "http://dc-util-fileserver.dcutil.svc.cluster.local/{version}"
```

### Environment-Specific Versions

Different environments can use different versions:

```toml
# Production - stable version
[dcutil]
version = "v0.0.1"  # Use tagged release (clean semantic version)
base_url = "http://dc-util-fileserver.dcutil.svc.cluster.local/{version}"
```

```toml
# Staging - development version
[dcutil]
version = "latest"  # ⚠️ Uses your working directory code, not highest version!
base_url = "http://dc-util-fileserver.dcutil.svc.cluster.local/{version}"
```

## 🔄 Version Mapping System

The multi-version system supports clean semantic version names through version mapping:

### How Version Mapping Works

**Format**: `user_version=git_tag`

```bash
# Maps clean version name to actual git tag
make docker-build-multi VERSIONS="v0.0.1=crate-operator-crds-2.53.0,latest"
```

**Result**:

- Container directory: `/srv/binaries/v0.0.1/`
- Source fetched from: Git tag `crate-operator-crds-2.53.0`
- User sees: Clean URLs like `/v0.0.1/dc_util-linux-amd64`

### Common Mapping Scenarios

```bash
# Semantic versioning
VERSIONS="v1.0.0=crate-operator-crds-2.53.0,v1.1.0=crate-operator-crds-2.54.0"

# Environment-based naming
VERSIONS="stable=crate-operator-crds-2.53.0,beta=testing-branch"

# Feature-based naming
VERSIONS="performance-fix=feature-branch-123,hotfix=urgent-patch"

# Mixed approach
VERSIONS="v1.0.0=crate-operator-crds-2.53.0,dev=latest"
```

### Why Version Mapping Is Needed

- **Problem**: dc_util source exists in git tags with complex names like `crate-operator-crds-2.53.0`
- **Solution**: Map these to user-friendly names like `v0.0.1`
- **Benefit**: Users get clean, semantic versions while system fetches from correct git references

### Fallback Behavior

If no mapping is provided, the system uses the git tag name directly:

```bash
# This works but creates ugly directory names
VERSIONS="crate-operator-crds-2.53.0,latest"
# Results in: /srv/binaries/crate-operator-crds-2.53.0/
```

## 🔧 Build Script Usage

The `manage-container.sh` script provides comprehensive container orchestration:

### List Available Versions

```bash
./manage-container.sh list-tags
```

### Build Specific Versions

```bash
# Clean semantic version mapping (recommended)
./manage-container.sh -v "v0.0.1=crate-operator-crds-2.53.0,latest" build

# Direct git tag names (works but ugly)
./manage-container.sh -v "crate-operator-crds-2.53.0,latest" build

# Custom registry and tag
./manage-container.sh -v "v0.0.1=crate-operator-crds-2.53.0,latest" -r "my-registry.com" -t "prod" build
```

### Complete Deployment

```bash
# Build and push in one command
./manage-container.sh -v "v0.0.1=crate-operator-crds-2.53.0,latest" deploy
```

## 🏷️ Version Management Strategies

### 1. Semantic Versioning

Use semantic versioning tags in git:

```bash
git tag v0.1.0    # Major.Minor.Patch
git tag v0.1.1    # Bug fixes
git tag v0.2.0    # New features
```

### 2. Environment Promotion

Promote versions through environments:

```
Development → Staging → Production
    ↓            ↓         ↓
  latest     → v0.2.0  → v0.1.0
```

### 3. Rollback Strategy

Keep multiple versions available for quick rollbacks:

```bash
# Current production
version = "v0.2.0"

# Quick rollback option
version = "v0.1.0"
```

## 🌐 URL Patterns

The multi-version fileserver supports several URL patterns:

### Version-Specific URLs

```
http://dc-util-fileserver.dcutil.svc.cluster.local/v0.0.1/dc_util-linux-amd64
http://dc-util-fileserver.dcutil.svc.cluster.local/latest/dc_util-linux-amd64
```

> **💡 Clean URLs**: Thanks to version mapping, users see clean URLs like `/v0.0.1/` instead of `/crate-operator-crds-2.53.0/`

### API Endpoints

```
http://dc-util-fileserver.dcutil.svc.cluster.local/api/versions
# Returns: ["v0.0.1", "latest"]
```

### Web Interface

```
http://dc-util-fileserver.dcutil.svc.cluster.local/
# Shows browsable directory listing of all versions
```

## 🔍 Troubleshooting

### Version Build Failures

```bash
# Check if git tag exists
git ls-remote --tags https://github.com/crate/crate-operator.git

# Verify container contents
docker run --rm cloud.registry.cr8.net/dc-util-fileserver:multi ls -la /srv/binaries/

# Debug build process
docker build --no-cache --progress=plain -f Dockerfile.multi .
```

### Version Resolution Issues

```bash
# Test version URL directly
curl http://dc-util-fileserver.dcutil.svc.cluster.local/v0.0.1/

# Check container logs
kubectl logs -n dcutil -l app=dc-util-fileserver

# Verify configuration parsing
uv run dc_util_sts_hooks.py --config-file sts-hooks-versioned.toml --dry-run
```

### Performance Considerations

- **Container size**: Each version adds ~20MB (compressed)
- **Build time**: Scales linearly with number of versions
- **Memory usage**: Minimal impact on runtime (~5MB per version)

## 📊 Monitoring

### Version Usage Tracking

Monitor which versions are being used:

```bash
# Check Caddy access logs
kubectl logs -n dcutil -l app=dc-util-fileserver | grep "GET /"

# Version API usage
kubectl logs -n dcutil -l app=dc-util-fileserver | grep "/api/versions"
```

### Health Checks

The container includes health checks:

```bash
# Check container health
kubectl get pods -n dcutil -l app=dc-util-fileserver

# Manual health check
kubectl exec -n dcutil deployment/dc-util-fileserver -- wget -O- http://localhost/api/versions
```

## 🔐 Security Considerations

### Version Integrity

- SHA256 checksums are generated for each binary
- Git tags provide version authenticity
- Container scanning can detect vulnerabilities per version

### Access Control

- Use Kubernetes RBAC to control version deployment
- Consider separate namespaces for different version lifecycles
- Implement approval workflows for production version changes

## 🚀 Advanced Usage

### Custom Version Sources

Modify `Dockerfile.multi` to support additional sources:

```dockerfile
# Add support for downloading from GitHub releases
RUN if [ "$version" != "latest" ]; then \
      wget "https://github.com/crate/crate-operator/releases/download/$version/dc_util-linux-amd64"; \
    fi
```

### Version Metadata

Add version metadata to containers:

```dockerfile
LABEL dc_util.versions="v0.0.1,v0.0.2,latest"
LABEL dc_util.build_date="2024-01-15T10:30:00Z"
```

### Automated Version Updates

Create CI/CD pipelines to automatically build new versions:

```yaml
# .github/workflows/multi-version.yml
on:
  push:
    tags:
      - "v*"

jobs:
  build-multi-version:
    runs-on: ubuntu-latest
    steps:
      - name: Build and push multi-version
        run: |
          VERSIONS="$(git tag | tail -3 | tr '\n' ',' | sed 's/,$//'),latest"
          make docker-deploy-multi VERSIONS="$VERSIONS"
```

## 📈 Best Practices

1. **Version Naming**: Use semantic versioning (v1.2.3)
2. **Retention Policy**: Keep last 5-10 versions in production
3. **Testing**: Always test new versions in staging first
4. **Documentation**: Document breaking changes between versions
5. **Rollback Plan**: Have automated rollback procedures
6. **Monitoring**: Track version usage and performance metrics

## 🤝 Contributing

When adding new versions:

1. Tag releases properly in git
2. Test multi-version builds locally
3. Update documentation for breaking changes
4. Coordinate version deployments across environments

---

This multi-version system provides the flexibility to manage dc_util versions effectively while maintaining stability and enabling smooth upgrades and rollbacks.
