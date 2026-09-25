FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml README.md /app/
COPY vault/ /app/vault/
COPY scripts/ /app/scripts/

# Install Vault package
RUN pip install --no-cache-dir -e .

# Expose S3 API and Dashboard port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8000/api/cluster/status || exit 1

# Default launch command: 3-node cluster with Web Dashboard
CMD ["python", "-m", "vault.cli", "serve", "--port", "8000", "--nodes", "3"]
