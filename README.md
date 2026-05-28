# 📨 Portal Notification Service

A robust, resilient asynchronous background worker for processing notifications within the Portal ecosystem. Extracted as a standalone microservice, it consumes messages from Kafka and delivers notifications (e.g., Email via SMTP) with advanced failure handling mechanisms.

---

## 🌟 Key Features

* **Kafka-Driven Architecture:** Decoupled completely from Kafka SDKs via internal Clean Architecture abstractions. Tolerant Reader pattern applied.
* **Resilience & Fault Tolerance:**
  * **Circuit Breaker:** Protects against external service (SMTP) cascading failures using `gobreaker`.
  * **Exponential Backoff Retries:** Handles transient network failures when fetching from brokers or delivering.
* **Idempotency & Deduplication:** Ensures that duplicate Kafka messages do not result in duplicate emails sent to users.
* **Dead Letter Queue (DLQ):** Messages that fail continuously are tracked via local state for inspection.
* **Observability:** Exposes Prometheus metrics and Kubernetes-ready `/livez` & `/readiness` health checks.

---

## 🛠️ Technology Stack

* **Language:** Go 1.22+
* **Messaging:** Apache Kafka
* **Mail Delivery:** SMTP Mailer
* **Database (Delivery State):** PostgreSQL (GORM)
* **Resilience:** Circuit Breaker (`sony/gobreaker`)

---

## 🚀 Getting Started

### Prerequisites
* Go 1.22+
* Infrastructure running (Kafka, PostgreSQL, MailHog)

### 1. Environment Setup
Create a `.env` file in the root directory:
```env
DB_URL=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_NOTIFICATION_REQUESTED_TOPIC=portal.notification.requested
KAFKA_CONSUMER_GROUP=notification-service

# SMTP Configuration
SMTP_HOST=localhost
SMTP_PORT=1025
SMTP_USE_AUTH=false
SMTP_FROM=noreply@portal.local

# Circuit Breaker Config
SMTP_CB_MAX_REQUESTS=5
SMTP_CB_INTERVAL_SECONDS=60
SMTP_CB_TIMEOUT_SECONDS=30
SMTP_CB_FAILURE_THRESHOLD=5
```

### 2. Run the Worker
Install dependencies and run the application:
```bash
go mod tidy
go run ./cmd/notification/main.go
```
