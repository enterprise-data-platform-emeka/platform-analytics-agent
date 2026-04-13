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
COPY ui/ ./ui/
COPY entrypoint.sh .

# Make entrypoint executable and set ownership before dropping to non-root user.
RUN chmod +x entrypoint.sh && chown -R agent:agent /app

USER agent

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# Streamlit writes config to $HOME/.streamlit. Set HOME to the app dir so the
# non-root user (no home directory) can write it without permission errors.
ENV HOME=/app

# entrypoint.sh starts uvicorn (background, port 8080) then streamlit (foreground, port 8501).
CMD ["./entrypoint.sh"]
