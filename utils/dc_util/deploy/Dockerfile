FROM caddy:2-alpine

# Create directory for binaries
RUN mkdir -p /srv/binaries

# Copy the Linux binaries
COPY dc_util-linux-amd64 /srv/binaries/
COPY dc_util-linux-arm64 /srv/binaries/

# Generate checksums
RUN cd /srv/binaries && \
    sha256sum dc_util-linux-amd64 > dc_util-linux-amd64.sha256 && \
    sha256sum dc_util-linux-arm64 > dc_util-linux-arm64.sha256

# Make binaries executable
RUN chmod +x /srv/binaries/dc_util-linux-*

# Expose port 80
EXPOSE 80

# Run Caddy in file server mode
CMD ["caddy", "file-server", "--root", "/srv/binaries", "--listen", ":80"]
