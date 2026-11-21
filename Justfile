set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the producer's dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Consume company-events into Postgres + pgvector.
consumer:
    uv run python -m company_rec.consumer

# Serve the similarity API on localhost:8000.
service:
    uv run uvicorn company_rec.service:app --host 127.0.0.1 --port 8000

# Run the test suite.
test:
    uv run python -m pytest -q

# Check DLQ depth against baseline; exits 1 on spike (for cron / CI).
dlq-check:
    uv run python -m company_rec.dlq_check
