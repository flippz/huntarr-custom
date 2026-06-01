FROM python:3.11-slim

# Install build tools needed for C-extension wheels (sabyenc3, libtorrent)
# These are only used at build time; the final image keeps only what's needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Create persistent data directories
RUN mkdir -p /config /downloads

EXPOSE 9705

CMD ["python", "main.py"]
