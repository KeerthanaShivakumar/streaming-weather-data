#!/bin/bash

# Weather Streaming Pipeline - Startup Script
# Starts Docker infrastructure, validates environment, and provides next steps.

set -euo pipefail
trap 'echo "❌ Unexpected error on line $LINENO"; exit 1' ERR

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_success(){ echo -e "${GREEN}✅ $1${NC}"; }
print_warning(){ echo -e "${YELLOW}⚠️  $1${NC}"; }
print_error(){ echo -e "${RED}❌ $1${NC}"; }
print_info(){ echo -e "ℹ️  $1"; }

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "=========================================="
echo "Weather Streaming Pipeline - Startup"
echo "=========================================="
echo ""

# Ensure .env exists; create from example if missing
if [ ! -f .env ]; then
  print_warning ".env not found -> creating from .env.example"
  if [ -f .env.example ]; then
    cp .env.example .env
    print_success ".env created from .env.example"
    print_warning "Please edit .env and set OPENWEATHER_API_KEY before continuing"
    echo ""
    exit 1
  else
    print_error ".env.example not found. Create .env manually."
    exit 1
  fi
fi

# Verify Docker CLI and daemon
print_info "Checking Docker CLI..."
if ! command -v docker >/dev/null 2>&1; then
  print_error "Docker CLI not found. Install Docker Desktop or Docker Engine."
  exit 1
fi

print_info "Checking Docker daemon..."
if ! docker info >/dev/null 2>&1; then
  print_error "Cannot connect to Docker daemon."
  echo " - On macOS/Windows: start Docker Desktop"
  echo " - On Linux: sudo systemctl start docker"
  echo ""
  exit 1
fi

# Detect Compose command (prefer `docker compose`)
if docker compose version >/dev/null 2>&1; then
  DC_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC_CMD="docker-compose"
else
  print_error "Docker Compose not found. Install Compose or Docker Desktop."
  exit 1
fi

print_success "Docker & Compose OK ($DC_CMD)"
echo ""

# Quick diagnostics
print_info "Docker version:"
docker version --format '{{.Server.Version}}' 2>/dev/null || docker version || true
echo ""
print_info "Compose info:"
$DC_CMD version || true
echo ""
print_info "Active containers (docker ps):"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" || true
echo ""

# Stop any existing stack (use Compose command)
print_info "Stopping any existing compose stack..."
$DC_CMD down 2>/dev/null || true
echo ""

# Start the stack
print_info "Starting application stack (this may take a few minutes)..."
$DC_CMD up -d --build
print_success "Compose started; waiting for services to become healthy..."
echo ""

# Wait for core infra: zookeeper, kafka, weather-postgres
max_wait=120
elapsed=0
zk_name="zookeeper"
kafka_name="kafka"
pg_name="weather-postgres"

while [ $elapsed -lt $max_wait ]; do
  zk_health=$(docker inspect --format='{{.State.Health.Status}}' ${zk_name} 2>/dev/null || echo "starting")
  kafka_health=$(docker inspect --format='{{.State.Health.Status}}' ${kafka_name} 2>/dev/null || echo "starting")
  pg_health=$(docker inspect --format='{{.State.Health.Status}}' ${pg_name} 2>/dev/null || echo "starting")

  if [ "$zk_health" = "healthy" ] && [ "$kafka_health" = "healthy" ] && [ "$pg_health" = "healthy" ]; then
    print_success "Zookeeper, Kafka and PostgreSQL are healthy"
    break
  fi

  echo "  Zookeeper: $zk_health | Kafka: $kafka_health | PostgreSQL: $pg_health"
  sleep 5
  elapsed=$((elapsed + 5))
done

if [ $elapsed -ge $max_wait ]; then
  print_error "Infrastructure did not become healthy in time (waited ${max_wait}s)"
  echo "Check logs: $DC_CMD logs --no-color"
  exit 1
fi

echo ""
# Create Kafka topic (best-effort)
print_info "Ensuring Kafka topic 'weather-data' exists..."
if docker exec "${kafka_name}" kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic weather-data \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists >/dev/null 2>&1; then
  print_success "Kafka topic 'weather-data' ready"
else
  print_warning "Could not create topic from container; topic creation was attempted (or it already exists)"
fi
echo ""

# Wait for dashboard health (container -> service)
print_info "Waiting for dashboard health at http://localhost:8000/health"
elapsed=0
max_wait_dashboard=90
while [ $elapsed -lt $max_wait_dashboard ]; do
  if curl -sSf http://localhost:8000/health >/dev/null 2>&1; then
    print_success "Dashboard healthy at http://localhost:8000"
    break
  fi
  printf "  Waiting for dashboard... (%ds/%ds)\r" "$elapsed" "$max_wait_dashboard"
  sleep 5
  elapsed=$((elapsed + 5))
done
echo ""

if [ $elapsed -ge $max_wait_dashboard ]; then
  print_warning "Dashboard did not become healthy in time. Check logs: $DC_CMD logs dashboard"
fi

# Show service status
echo "=========================================="
echo "Service Status (compose)"
echo "=========================================="
$DC_CMD ps || true
echo ""

# Helpful commands
echo "=========================================="
echo "Next Steps / Useful Commands"
echo "=========================================="
echo ""
echo "View all logs:"
echo "  $DC_CMD logs -f"
echo ""
echo "View specific service logs (example):"
echo "  $DC_CMD logs -f spark-consumer"
echo "  $DC_CMD logs -f dashboard"
echo ""
echo "Stop all services:"
echo "  $DC_CMD down"
echo ""
echo "Stop and remove volumes:"
echo "  $DC_CMD down -v"
echo ""
echo "Run producer or dashboard locally (optional):"
echo "  python producer.py    # runs producer locally against KAFKA_BROKER in .env"
echo "  python dashboard.py   # runs dashboard locally (Flask) at http://localhost:8000"
echo ""
print_success "Setup complete! 🚀"
echo ""