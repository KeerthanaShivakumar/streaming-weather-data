# Copilot / AI Agent Instructions for streaming-weather-data

Purpose: give an AI coding agent the minimal, concrete context needed to be productive editing, testing, and extending this repo.

- Big picture
  - This project is a small streaming pipeline implementing a Lambda-style architecture:
    - `producer.py` — Python Kafka producer: polls OpenWeather API, emits JSON messages to Kafka topic `weather-data`. City list is in `CITIES`.
    - Kafka & Zookeeper — started via `docker-compose.yml` (services `kafka`, `zookeeper`). Producer targets `kafka:29092` inside compose; host-mapped port `9092` is exposed.
    - `consumer.py` — Spark streaming consumer: reads from Kafka, performs 5-minute windowed aggregations and writes two sinks to PostgreSQL: `weather_raw` (raw events) and `weather_aggregated` (windowed stats). Checkpoint locations: `/tmp/spark-checkpoint*`.
    - `dashboard.py` — Flask app: serves UI (`templates/weather_dashboard.html`) and REST APIs (`/api/current`, `/api/history/<city>`, `/api/aggregated/<city>`, `/api/stats`, `/health`) backed by PostgreSQL.
    - Database initialization lives in `init-db.sql` (creates `weather_raw` and `weather_aggregated`).

- How to run locally / dev workflow (canonical)
  - Primary startup helper: `./start.sh` — validates environment, starts `docker compose up -d --build`, waits for infra health, creates topic `weather-data` (best-effort), and reports useful `docker compose` commands.
  - Manual alternatives:
    - `docker compose up -d --build` (or `docker-compose up -d --build` if old CLI)
    - Run services locally for debugging:
      - `python producer.py` — runs the producer against `KAFKA_BROKER` from `.env`.
      - `python dashboard.py` — runs Flask on `PORT` (default 8000).

- Important files & quick notes (use these when making changes)
  - `producer.py`: topic name `weather-data`; uses `OPENWEATHER_API_KEY` from `.env`. Uses city name as Kafka message key for partitioning.
  - `consumer.py`: Spark app — includes `spark.jars.packages` for Kafka + Postgres connectors; window length is 5 minutes; writes raw and aggregated streams with `foreachBatch` to Postgres tables `weather_raw` and `weather_aggregated`.
  - `dashboard.py`: queries `weather_raw` and `weather_aggregated`. Look for timestamp handling (conversions to ISO) and DB connection via `psycopg` / `DB_CONFIG`.
  - `init-db.sql`: schema and sample data used by the `postgres` service on first boot.
  - `docker-compose.yml`: service names used by scripts and the `start.sh` health checks (e.g., `zookeeper`, `kafka`, `weather-postgres`, `spark-consumer`, `producer`, `dashboard`). Use these exact names when scripting or running `docker exec`.

- Environment variables observed (check `.env.example` / `.env` before running)
  - `OPENWEATHER_API_KEY` — required for `producer.py` (the startup script exits if `.env` was created and key not set).
  - `KAFKA_BROKER` — inside compose services use `kafka:29092`; host-facing address is `localhost:9092`.
  - `POSTGRES_*` variables — used by `consumer.py` and `dashboard.py`. `consumer` expects a JDBC URL `POSTGRES_URL` for Spark writes.

- Patterns & conventions to follow when editing code
  - Keep Kafka topic name `weather-data` consistent across code and compose config.
  - Producer uses city name as message key — preserve this when refactoring to maintain partitioning behavior.
  - Spark streaming uses small local resources (`local[*]`, `2g` driver/executor) — tests and CI should respect these constraints to avoid OOMs.
  - Checkpoint locations are hard-coded to `/tmp/...` in `consumer.py` and `docker-compose.yml`. If you change them, update both the consumer code and `docker-compose` volume mounts.

- Integration & external dependencies
  - External services: OpenWeather API (requires API key), Kafka/Zookeeper (Confluent images), PostgreSQL (official image), Spark (Bitnami image in compose or `spark-submit` locally). The Spark job auto-downloads `org.apache.spark:spark-sql-kafka-0-10` and Postgres JDBC jars via `--packages`.
  - JARs/Packages are declared inside Spark config in `consumer.py` and in the `spark-submit` command in `docker-compose.yml` for `spark-consumer`.

- Troubleshooting signals (things I discovered while reading the code)
  - `start.sh` is the recommended entrypoint; it includes helpful health checks and topic creation commands — reference it when adding infra changes.
  - `dashboard.py` mixes `psycopg` row factory and `RealDictCursor` usage; be careful editing DB code — run the dashboard locally to validate DB cursor/row handling.
  - If Spark fails to download packages the first run, allow extra time (consumer logs show package downloads).

- When writing code, tests, or changes include these examples in PRs:
  - Example: When referencing the Kafka topic, use `weather-data` (see `producer.py`, `consumer.py`, `docker-compose.yml`).
  - Example: When adding a new consumer sink, write to a new table and mirror the checkpoint config (`/tmp/spark-checkpoint-...`) and the Postgres JDBC options used in `consumer.py`.

If anything above is unclear or you want me to expand a particular section (run commands, environment matrix, or fix the `psycopg` cursor mismatch), tell me which part to iterate on.
