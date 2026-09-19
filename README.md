# 🚦 Real-Time & Batch Hybrid Traffic Monitoring Pipeline

A containerized hybrid data engineering platform for ingesting, processing, validating, and analyzing simulated traffic telemetry.

The platform demonstrates two independent processing paths built around a shared **Apache Kafka** event stream:
- **Real-Time Path** for low-latency traffic monitoring and alerts.
- **Batch Path** for scheduled historical processing, data quality checks, and analytics.

---

## 🏗️ Architecture

```mermaid
graph TD
    %% Define Styles
    classDef source fill:#2c3e50,stroke:#34495e,stroke-width:2px,color:#fff
    classDef kafka fill:#d35400,stroke:#e67e22,stroke-width:2px,color:#fff
    classDef realtime fill:#27ae60,stroke:#2ecc71,stroke-width:2px,color:#fff
    classDef batch fill:#2980b9,stroke:#3498db,stroke-width:2px,color:#fff
    classDef storage fill:#8e44ad,stroke:#9b59b6,stroke-width:2px,color:#fff
    classDef ui fill:#c0392b,stroke:#e74c3c,stroke-width:2px,color:#fff

    %% Components
    Sim["🚗 Traffic Simulator<br/>(Python Producer)"]:::source
    Kafka["⚡ Apache Kafka<br/>(traffic-telemetry topic)"]:::kafka
    
    subgraph "Real-Time Processing Path"
        RTC["⏱️ Real-Time Consumer<br/>(Kafka Consumer Group)"]:::realtime
        RT_Metrics["📊 60-Sec Window Aggregation"]:::realtime
    end

    subgraph "Batch Processing Path (Apache Airflow Orchestrated)"
        Extract["📥 Raw JSON Extraction<br/>(Airflow Task)"]:::batch
        Spark["⚙️ PySpark Transformations<br/>(Data Cleaning & Aggregation)"]:::batch
        DQ["✅ Data Quality Checks<br/>(Airflow Task)"]:::batch
    end

    PG_RT[("🗄️ PostgreSQL<br/>(Real-Time Tables)")]:::storage
    PG_DWH[("🗄️ PostgreSQL DWH<br/>(Historical Tables)")]:::storage
    
    Streamlit["🌐 Streamlit Dashboard<br/>(Live Traffic Monitoring)"]:::ui

    %% Connections
    Sim -->|Publishes Events| Kafka
    Kafka -->|Consumed by 'traffic-realtime-consumer'| RTC
    Kafka -->|Consumed by 'traffic-batch-consumer'| Extract
    
    RTC --> RT_Metrics
    RT_Metrics -->|Upserts Metrics| PG_RT
    
    Extract -->|Saves Raw Batch| Spark
    Spark -->|Writes Staging & Hourly| PG_DWH
    Spark --> DQ
    DQ -->|Validates DWH Schema| PG_DWH
    
    PG_RT -->|Queries Live Data| Streamlit
    PG_DWH -->|Queries Historical Data| Streamlit
```

---

## 🛠️ Technology Stack
- **Languages:** Python 3.10+
- **Message Broker:** Apache Kafka, Apache ZooKeeper
- **Orchestration:** Apache Airflow 2.8.1 (LocalExecutor)
- **Data Processing:** PySpark 3.5.1
- **Database / Data Warehouse:** PostgreSQL 15
- **Frontend / Visualization:** Streamlit
- **Infrastructure:** Docker & Docker Compose

## 🚀 Getting Started

### 1. Prerequisites
- Docker Desktop
- Python 3.10+ (for local simulator)

### 2. Environment Setup
```bash
# Clone the repository
git clone https://github.com/yossefabdelkarem9/traffic-monitoring-pipeline.git
cd traffic-monitoring-pipeline

# Configure environment variables
cp .env.example .env
```

### 3. Start the Infrastructure
```bash
# Build and start all services
docker compose up -d --build
```
> **Note:** The initial setup may take a few minutes as it provisions PostgreSQL databases, runs Airflow migrations, and creates the admin user.

### 4. Start the Traffic Simulator
In a separate terminal, run the simulator to start generating live traffic data:
```bash
pip install -r simulator/requirements.txt
python simulator/traffic_producer.py
```

### 5. Access the Interfaces
- **Streamlit Live Dashboard:** `http://localhost:8501`
- **Airflow Web UI:** `http://localhost:8080` *(Login: admin / admin)*

---

## 🧩 Reliability & Design Choices

1. **Independent Consumer Groups:** The real-time and batch pipelines consume from the exact same Kafka topic (`traffic-telemetry`) but use completely isolated consumer groups.
2. **Idempotency:** 
   - Real-time events use `event_id` as a Primary Key in Postgres to silently drop duplicates.
   - Batch pipelines use Airflow's `DAG_RUN_ID` as a `batch_id` to safely allow pipeline retries without creating duplicated historical aggregations.
3. **Automated Data Quality:** The pipeline features integrated Data Quality (DQ) checks that act as a circuit breaker. If validation (e.g., speed limit anomalies, NULL values) fails, the Airflow task fails.

---
*Created by [Yossef Ahmed](https://www.linkedin.com/in/yossef-ahmed/)*
