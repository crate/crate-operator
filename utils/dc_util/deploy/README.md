# DC Util Fileserver Deployment

This directory contains the deployment configuration for a minimal Caddy-based fileserver that serves the dc_util Linux binaries for both ARM64 and x86_64 architectures.

## Overview

The deployment consists of:

- **Dockerfile**: Minimal Caddy container serving dc_util binaries
- **k8s/namespace.yaml**: dcutil namespace
- **k8s/deployment.yaml**: Kubernetes deployment with 2 replicas
- **k8s/service.yaml**: ClusterIP service for internal access
- **k8s/imagepull-secret.yaml**: Template for private registry authentication
- **../Makefile**: Consolidated build and deployment automation

## Quick Start

### 1. Build and Push Container

From the dc_util root directory:

```bash
make docker-deploy
```

Or step by step:

```bash
make build-binaries
make docker-build
make docker-push
```

### 2. Configure Registry Secret

Edit `k8s/imagepull-secret.yaml` and replace the placeholder with your base64-encoded Docker config:

```bash
# Generate the Docker config JSON (replace with your credentials)
echo -n '{"auths":{"cloud.registry.cr8.net":{"username":"YOUR_USERNAME","password":"YOUR_PASSWORD","auth":"BASE64_OF_USERNAME:PASSWORD"}}}' | base64 -w 0
```

### 3. Deploy to Kubernetes

```bash
make k8s-apply
```

Or for complete deployment:

```bash
make deploy
```

## Configuration

### Environment Variables

The deployment uses minimal configuration:

- Caddy runs on port 80
- Files served from `/srv/binaries/`
- Automatic SHA256 checksum generation

### Resource Limits

- **Requests**: 10m CPU, 32Mi memory
- **Limits**: 100m CPU, 128Mi memory

### Security

- Runs as non-root user (65534)
- Read-only root filesystem disabled (Caddy needs to write temp files)
- Drops all capabilities
- No privilege escalation

## Usage

Once deployed, the service will be available within the dcutil namespace at:

```
http://dc-util-fileserver.dcutil.svc.cluster.local/dc_util-linux-amd64
http://dc-util-fileserver.dcutil.svc.cluster.local/dc_util-linux-arm64
http://dc-util-fileserver.dcutil.svc.cluster.local/dc_util-linux-amd64.sha256
http://dc-util-fileserver.dcutil.svc.cluster.local/dc_util-linux-arm64.sha256
```

Or if accessing from within the same namespace:

```
http://dc-util-fileserver/dc_util-linux-amd64
http://dc-util-fileserver/dc_util-linux-arm64
http://dc-util-fileserver/dc_util-linux-amd64.sha256
http://dc-util-fileserver/dc_util-linux-arm64.sha256
```

### Update sts-hooks.toml

Update your `sts-hooks.toml` base_url to use the service:

```toml
[dcutil]
version = "0.0.1"
base_url = "http://dc-util-fileserver.dcutil.svc.cluster.local"
```

Or if your dc_util hooks run in the same namespace:

```toml
[dcutil]
version = "0.0.1"
base_url = "http://dc-util-fileserver"
```

## Makefile Targets

Run these from the dc_util root directory:

- `make docker-build` - Build the container image
- `make docker-push` - Push to registry
- `make docker-deploy` - Build and push
- `make k8s-apply` - Deploy to Kubernetes
- `make k8s-delete` - Remove from Kubernetes
- `make deploy` - Complete workflow (build + push + apply)
- `make clean` - Remove local images and binaries
- `make info` - Show build details
- `make help` - Show all targets

## Customization

### Change Image Tag

```bash
make docker-deploy TAG=v1.0.0
```

### Different Registry

Edit the `REGISTRY` variable in the Makefile or override:

```bash
make docker-build REGISTRY=your-registry.com
```

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -l app=dc-util-fileserver -n dcutil
kubectl logs -l app=dc-util-fileserver -n dcutil
```

### Test Service

```bash
kubectl port-forward svc/dc-util-fileserver -n dcutil 8080:80
curl http://localhost:8080/
```

### Registry Authentication Issues

Verify the ImagePullSecret:

```bash
kubectl get secret cr8-registry-secret -n dcutil -o yaml
kubectl describe secret cr8-registry-secret -n dcutil
```

## Benefits

- **Lightweight**: Minimal Caddy Alpine image
- **Fast**: Direct binary serving without GitHub API limits
- **Reliable**: Kubernetes deployment with health checks
- **Secure**: Non-root execution with security context
- **Self-contained**: Checksums generated automatically
- **Namespaced**: Isolated in dcutil namespace
