FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Copy all application code (needed for uv sync to find README.md, models, etc.)
COPY . .

# Install dependencies
RUN uv sync --frozen --no-dev

ENV ENABLE_WEB_INTERFACE=true

EXPOSE 8000

CMD ["uv", "run", "python", "-m", "server.app"]
