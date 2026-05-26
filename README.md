#  Real-Time Mobile Money Fraud Detection Engine

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=flat&logo=mongodb&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-174825?style=flat&logo=xgboost&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

<img width="1899" height="981" alt="image" src="https://github.com/user-attachments/assets/8e83d13f-8adf-439f-bf6a-8ba8401fec4a" />

An end-to-end Machine Learning and Data Engineering pipeline built to detect and block mobile money fraud (SIM-swaps, Account Takeovers, Velocity attacks) in **under 200 milliseconds**.

##  The Problem
In emerging markets, mobile money platforms (like M-Pesa, Wave, and OPay) process billions of transactions daily. Fraud teams often rely on batch-processed CSV exports, meaning by the time a SIM-swap or ghost transaction is flagged, the money is already gone. 

**The Solution:** This project implements a real-time streaming architecture that scores every single transaction via an XGBoost model before it settles, alerting Security Operations Center (SOC) teams instantly while generating automated business reports for the strategy team.

---

##  Architecture Flow

1. **The Telemetry Stream:** A Python simulator acts as the telco switch, pumping thousands of normal transactions into an **Apache Kafka** topic, occasionally injecting targeted fraud typologies.
2. **The Inference Engine:** A Kafka Consumer pulls events off the stream, instantly queries a **PostgreSQL Feature Store** to check the user's historical baseline (device ID, location), and feeds the enriched data into an **XGBoost ML Model** for probabilistic scoring.
3. **The Data Lakehouse:**
    * **MongoDB (Audit Lake):** Stores the raw, immutable JSON payloads and scores for compliance.
    * **PostgreSQL (Alert Store):** Stores flattened, processed transactions for real-time querying.
4. **The SOC Dashboard:** **Grafana** connects to PostgreSQL, providing a sub-second live view of critical alerts, blocked transactions, and active Account Takeovers.
5. **Analytics Engineering:** **dbt (data build tool)** runs batch transformations against the operational database to generate clean, aggregated daily business metrics (`analytics_marts`) for the CFO and Risk teams.

---

##  Tech Stack Highlights

* **Streaming:** Apache Kafka (KRaft mode) + `kafka-python-ng`
* **Machine Learning:** `xgboost`, `pandas`, `scikit-learn`
* **Database & Storage:** PostgreSQL (Feature Store / Relational), MongoDB (Document Lake)
* **Transformation & Analytics:** dbt (Data Build Tool)
* **Visualization:** Grafana
* **Orchestration:** Docker Compose

---

##  Quick Start Guide

### 1. Spin up the Infrastructure
Start the Kafka broker, databases, and UI tools via Docker:
```bash
docker-compose up -d
