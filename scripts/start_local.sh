#!/bin/bash
# Start local development environment
set -e

echo "Starting PostgreSQL..."
docker-compose up -d postgres
sleep 3

echo "Generating sample data (10K rows)..."
docker-compose run --rm generator --sample

echo "Running pipeline..."
docker-compose --profile pipeline run --rm pipeline --load-csv

echo ""
echo "Local environment ready!"
echo "  PostgreSQL: localhost:5433 (user: analytics, pass: analytics_pass, db: marketing)"
echo ""
echo "To generate full 5M+ dataset: docker-compose --profile generate run --rm generator"
echo "To run quality checks: docker-compose --profile pipeline run --rm pipeline --quality"
echo "  Observability: http://localhost:8503 (--profile observability)"
echo "To stop: docker-compose down -v"
