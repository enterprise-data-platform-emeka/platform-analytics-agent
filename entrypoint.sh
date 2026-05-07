#!/bin/bash
# Starts both processes in the same container.
#
# FastAPI runs in the background on port 8080 (internal — ALB routes /ask here).
# Streamlit runs in the foreground on port 8501 (ALB routes browser traffic here).
#
# The script waits for FastAPI to pass its health check before starting Streamlit
# so the UI never serves a page while the backend is still loading schemas.

set -e

echo "Starting FastAPI backend on port 8080..."
uvicorn agent.main:app --host 0.0.0.0 --port 8080 &
FASTAPI_PID=$!

echo "Waiting for FastAPI to be ready..."
STARTED_AT=$(date +%s)
STARTUP_TIMEOUT_SECONDS=${STARTUP_TIMEOUT_SECONDS:-180}
until python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" > /dev/null 2>&1; do
    if ! kill -0 "$FASTAPI_PID" > /dev/null 2>&1; then
        echo "FastAPI exited before becoming healthy."
        wait "$FASTAPI_PID"
    fi
    NOW=$(date +%s)
    if [ $((NOW - STARTED_AT)) -ge "$STARTUP_TIMEOUT_SECONDS" ]; then
        echo "FastAPI did not become healthy within ${STARTUP_TIMEOUT_SECONDS}s."
        kill "$FASTAPI_PID" > /dev/null 2>&1 || true
        wait "$FASTAPI_PID" || true
        exit 1
    fi
    sleep 1
done
echo "FastAPI is ready."

echo "Starting Streamlit UI on port 8501..."
exec streamlit run ui/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false
