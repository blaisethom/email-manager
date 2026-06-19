#!/usr/bin/env bash
set -e

echo "Waiting for Prefect server..."
until prefect work-pool ls > /dev/null 2>&1; do
    sleep 3
done

echo "Creating work pool (idempotent)..."
prefect work-pool create email-manager-pool --type process --overwrite 2>/dev/null || true

echo "Creating AI concurrency limit (idempotent)..."
prefect concurrency-limit create ai-llm 3 2>/dev/null || true

echo "Deploying flows..."
cd /app/email-analyser
prefect deploy --all

echo "Starting worker..."
exec prefect worker start --pool email-manager-pool
