# 🛒 Real-Time Ecommerce Clickstream Pipeline

A real-time data engineering project that simulates ecommerce user activity, processes it through a streaming pipeline, and visualizes it on a live dashboard — fully orchestrated with Apache Airflow.

---

## 🏗️ Architecture

```
producer.py
    │
    ▼
 Kafka (user_clicks topic)
    │
    ▼
Spark Structured Streaming
    │
    ▼
PostgreSQL (clickstream_events)
    │
    ▼
Streamlit Dashboard (localhost:8501)

        ⬆ All orchestrated by Apache Airflow (localhost:8080)
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Message Broker | Apache Kafka + Zookeeper |
| Stream Processing | Apache Spark Structured Streaming |
| Storage | PostgreSQL |
| Dashboard | Streamlit + Plotly |
| Orchestration | Apache Airflow (Docker-based) |
| Containerization | Docker + Docker Compose |

---

## 📁 Project Structure

```
clickstream_pipeline/
├── dags/
│   └── clickstream_etl_dag.py   # Airflow DAG
├── logs/                         # Airflow logs
├── docker-compose.yml            # All services
├── init-airflow-db.sql           # Creates Airflow metadata DB
├── producer.py                   # Kafka event producer
├── spark_consumer.py             # Spark Structured Streaming job
├── dashboard.py                  # Streamlit dashboard
└── jars/                         # Spark connector JARs
```

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop running
- Python 3.11+
- Git

### 1. Clone the repo
```bash
git clone https://github.com/nabha03/clickstream_pipeline.git
cd clickstream_pipeline
```

### 2. Create required folders
```bash
mkdir dags logs
```

### 3. Start all containers
```bash
docker-compose up -d
```

### 4. Start the Kafka producer
```bash
python producer.py
```

### 5. Open the UIs

| App | URL | Credentials |
|-----|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Streamlit | http://localhost:8501 | — |

---

## 🔁 Airflow DAG — `clickstream_etl_pipeline`

Runs every 10 minutes and orchestrates the full pipeline:

```
check_kafka_health ──┐
                     ├──► run_kafka_producer_batch ──► submit_spark_etl_job ──► verify_postgres_data ──► pipeline_complete
check_postgres_health ┘
```

| Task | Description |
|------|-------------|
| `check_kafka_health` | Verifies Kafka broker is reachable |
| `check_postgres_health` | Verifies PostgreSQL is reachable |
| `run_kafka_producer_batch` | Runs producer for 60-second batch |
| `submit_spark_etl_job` | Submits Spark streaming job |
| `verify_postgres_data` | Confirms rows exist in `clickstream_events` |
| `pipeline_complete` | Marks successful run |

---

## 📊 Dashboard Features

- Live KPI cards — Total Events, Clicks, Purchases, Views
- Bar chart — Top Products by Action
- Line chart — Actions Over Time (IST)
- Real-time events table (latest 50 rows)
- Auto-refreshes every 5 seconds

---


