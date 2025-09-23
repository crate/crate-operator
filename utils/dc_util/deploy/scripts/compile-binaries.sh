#!/bin/sh

# DC Util Binary Compilation Script
# This script compiles dc_util binaries from multiple git versions or current source
# Used internally by Dockerfile.multi during container builds

set -e

# Default values (can be overridden by environment variables)
VERSIONS=${VERSIONS:-"latest"}
GITHUB_REPO=${GITHUB_REPO:-"https://github.com/crate/crate-operator.git"}

echo "=== DC Util Binary Compiler ==="
echo "Versions: $VERSIONS"
echo "Repository: $GITHUB_REPO"
echo "======================================"

# Create output directory
mkdir -p /binaries

# Set IFS for comma-separated parsing
IFS=","

for version_spec in $VERSIONS; do
    echo
    echo "--- Processing version spec: $version_spec ---"

    # Parse version mapping: user_version=git_tag or just user_version
    if echo "$version_spec" | grep -q "="; then
        user_version=$(echo "$version_spec" | cut -d"=" -f1)
        git_tag=$(echo "$version_spec" | cut -d"=" -f2)
        echo "Version mapping: '$user_version' -> git tag '$git_tag'"
    else
        user_version="$version_spec"
        git_tag="$version_spec"
        echo "Direct version: '$user_version'"
    fi

    echo "Building version: $user_version (from git: $git_tag)"
    mkdir -p "/binaries/$user_version"

    # Determine source location
    if [ "$user_version" = "latest" ]; then
        echo "Building from current source..."
        cd /workspace
    else
        echo "Fetching git tag $git_tag..."
        cd /tmp
        rm -rf "crate-operator-$git_tag"

        echo "Cloning repository..."
        if ! git clone --depth 1 --branch "$git_tag" "$GITHUB_REPO" "crate-operator-$git_tag"; then
            echo "ERROR: Failed to clone git tag '$git_tag'"
            continue
        fi

        # Try to find dc_util source
        if [ -d "crate-operator-$git_tag/utils/dc_util" ]; then
            echo "Found dc_util in utils/dc_util directory"
            cd "crate-operator-$git_tag/utils/dc_util"
        else
            echo "WARNING: utils/dc_util not found in $git_tag, trying root directory"
            cd "crate-operator-$git_tag"
            if [ ! -f "dc_util.go" ]; then
                echo "ERROR: dc_util.go not found in $git_tag"
                echo "Available files:"
                ls -la
                continue
            fi
        fi
    fi

    # Verify we have the source file
    if [ ! -f "dc_util.go" ]; then
        echo "ERROR: dc_util.go not found in current directory for $user_version"
        echo "Current directory: $(pwd)"
        echo "Available files:"
        ls -la
        continue
    fi

    echo "Building binaries for $user_version..."

    # Build AMD64 binary
    echo "Building AMD64 binary..."
    if ! GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o "/binaries/$user_version/dc_util-linux-amd64" dc_util.go; then
        echo "ERROR: Failed to build AMD64 binary for $user_version"
        continue
    fi

    # Build ARM64 binary
    echo "Building ARM64 binary..."
    if ! GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o "/binaries/$user_version/dc_util-linux-arm64" dc_util.go; then
        echo "ERROR: Failed to build ARM64 binary for $user_version"
        continue
    fi

    # Generate checksums
    echo "Generating checksums..."
    cd "/binaries/$user_version"
    sha256sum dc_util-linux-amd64 > dc_util-linux-amd64.sha256
    sha256sum dc_util-linux-arm64 > dc_util-linux-arm64.sha256

    # Create symlinks for uname -m architecture names and corresponding SHA256 files
    echo "Creating architecture symlinks..."
    ln -sf dc_util-linux-amd64 dc_util-linux-x86_64
    ln -sf dc_util-linux-arm64 dc_util-linux-aarch64

    # Create separate SHA256 files with correct filenames for symlinked binaries
    echo "Creating SHA256 files for symlinked binaries..."
    sed 's/dc_util-linux-amd64/dc_util-linux-x86_64/g' dc_util-linux-amd64.sha256 > dc_util-linux-x86_64.sha256
    sed 's/dc_util-linux-arm64/dc_util-linux-aarch64/g' dc_util-linux-arm64.sha256 > dc_util-linux-aarch64.sha256

    # Set executable permissions
    chmod +x dc_util-linux-*

    echo "✅ Version $user_version built successfully"
    echo "   AMD64 size: $(ls -lh dc_util-linux-amd64 | awk '{print $5}')"
    echo "   ARM64 size: $(ls -lh dc_util-linux-arm64 | awk '{print $5}')"
done

echo
echo "=== Creating version index ==="
cd /binaries

# Create HTML index
cat > index.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>DC Util Versions</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        ul { list-style-type: none; padding: 0; }
        li { margin: 10px 0; }
        a { text-decoration: none; color: #007acc; font-weight: bold; }
        a:hover { text-decoration: underline; }
        .version { background: #f5f5f5; padding: 10px; border-radius: 4px; }
    </style>
</head>
<body>
    <h1>DC Util Binary Versions</h1>
    <p>Available versions of dc_util binaries:</p>
    <ul>
EOF

# Add version links
for version_spec in $VERSIONS; do
    if echo "$version_spec" | grep -q "="; then
        user_version=$(echo "$version_spec" | cut -d"=" -f1)
    else
        user_version="$version_spec"
    fi

    if [ -d "$user_version" ]; then
        echo "        <li class=\"version\"><a href=\"/$user_version/\">$user_version</a></li>" >> index.html
    fi
done

cat >> index.html << 'EOF'
    </ul>
    <hr>
    <p><small>Generated by DC Util Binary Compiler</small></p>
</body>
</html>
EOF

echo "✅ Created HTML index with $(echo "$VERSIONS" | tr ',' '\n' | wc -l) version(s)"

echo
echo "=== Build Summary ==="
echo "Built versions:"
for dir in */; do
    if [ -d "$dir" ] && [ "$dir" != "./" ]; then
        version=${dir%/}
        echo "  📦 $version"
        echo "     - AMD64: $(ls -lh "$version/dc_util-linux-amd64" 2>/dev/null | awk '{print $5}' || echo 'missing')"
        echo "     - ARM64: $(ls -lh "$version/dc_util-linux-arm64" 2>/dev/null | awk '{print $5}' || echo 'missing')"
    fi
done

echo
echo "🎉 Binary compilation completed successfully!"
