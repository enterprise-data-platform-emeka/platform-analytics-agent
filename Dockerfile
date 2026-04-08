# Stage 1: build dependencies
FROM python:3.11.8-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt


# Stage 2: runtime image
FROM python:3.11.8-slim AS runtime

# Non-root user for security
RUN groupadd --gid 1001 agent && \
    useradd --uid 1001 --gid agent --no-create-home agent

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY agent/ ./agent/

# Set ownership
RUN chown -R agent:agent /app

USER agent

# Default: run the CLI. Override QUESTION env var or entrypoint for HTTP mode.
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python", "-m", "agent.main"]
