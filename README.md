# Streaming Weather Data — Lambda-style Streaming Pipeline

Project statement
- Small end-to-end streaming pipeline that ingests current weather from OpenWeather, publishes to Kafka, processes streams with Spark (windowed aggregations), stores raw & aggregated data in PostgreSQL, and serves a real-time dashboard.

Tech stack
- Kafka / Zookeeper: message ingestion ([docker-compose.yml](docker-compose.yml))
- Apache Spark (PySpark): streaming consumer and windowed aggregation ([consumer.py](consumer.py)) — class: [`WeatherStreamProcessor`](consumer.py)
- PostgreSQL: storage and serving layer (initial schema in [init-db.sql](init-db.sql))
- Python services:
  - Producer: [`WeatherProducer`](producer.py) -> publishes to Kafka ([producer.py](producer.py), [Dockerfile.producer](Dockerfile.producer))
  - Dashboard: Flask API & UI ([dashboard.py](dashboard.py), [Dockerfile.dashboard](Dockerfile.dashboard), template: [templates/weather_dashboard.html](templates/weather_dashboard.html))
- Orchestration: Docker Compose ([docker-compose.yml](docker-compose.yml))
- Local helper: startup + health checks ([start.sh](start.sh))
- Env management: [.env.example](.env.example) (do NOT commit secrets)

System-level architecture (high level)
- Flow:
  Producer (Python) -> Kafka (topic: `weather-data`) -> Spark Consumer (stream & windowed agg) -> PostgreSQL -> Dashboard (Flask)
- ASCII diagram:
  Producer -> Kafka/Zookeeper -> Spark Consumer --> Postgres --> Dashboard (UI/API)
- Key storage:
  - Raw stream: table `weather_raw` ([init-db.sql](init-db.sql))
  - Aggregates: table `weather_aggregated` ([init-db.sql](init-db.sql))

Quick start (local)
1. Copy `.env.example` → `.env` and set OPENWEATHER_API_KEY and any DB creds: see [.env.example](.env.example).  
2. Start everything:
   - ./start.sh (runs `docker compose up -d --build` and health checks) — [start.sh](start.sh)
3. Dashboard: http://localhost:8000/ (health: /health) — implemented in [`get_db_connection`](dashboard.py) / [`dashboard.py`](dashboard.py)

Important files & entry points
- Orchestration: [docker-compose.yml](docker-compose.yml)
- Spark consumer: [consumer.py](consumer.py) — [`WeatherStreamProcessor`](consumer.py)
- Producer: [producer.py](producer.py) — [`WeatherProducer`](producer.py)
- Dashboard: [dashboard.py](dashboard.py) — API and UI
- DB init: [init-db.sql](init-db.sql)
- Dockerfiles: [Dockerfile.producer](Dockerfile.producer), [Dockerfile.dashboard](Dockerfile.dashboard)
- Env template: [.env.example](.env.example)
- UI template: [templates/weather_dashboard.html](templates/weather_dashboard.html)

Notes & safety
- Never commit real secrets. Use [.env.example](.env.example) as the template and set secrets locally or in CI/hosting provider secret stores.
- Kafka topic name is `weather-data` — keep consistent across files: [`producer.py`](producer.py), [`consumer.py`](consumer.py), [docker-compose.yml](docker-compose.yml).