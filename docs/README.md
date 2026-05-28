# Notification Service

This is an independent microservice responsible for consuming `notification.requested` events from Kafka and dispatching them to external channels (e.g., SMTP for email). 

It is designed to be highly resilient, fault-tolerant, and guarantees at-least-once delivery.

## 📚 Documentation Index
1. **[Architecture & Retry Logic](architecture.md)**: Details on idempotency, offset management, and database state.
2. **[Email Processing Flow](diagrams/email_processing_flow.md)**: Visual sequence diagram of how messages are consumed and retired.

## 🚀 Quick Start

### Prerequisites
- Running Kafka instance.
- Running PostgreSQL instance (for the notification local DB).
- Running Mailhog or an SMTP server.

### Environment Variables
Ensure the following variables are set (usually managed by Docker Compose):
```ini
DB_URL=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable
KAFKA_BROKERS=localhost:9092
KAFKA_NOTIFICATION_TOPIC=notification.requested
KAFKA_CONSUMER_GROUP=notification-worker-group
SMTP_HOST=localhost
SMTP_PORT=1025
```

### Running Locally
Navigate to this service's directory and run:
```bash
cd services/notification-service
go run ./cmd
```

## 🛠 Project Structure
- `cmd/`: Application entrypoint.
- `config/`: Configuration loading.
- `internal/`:
  - `app/`: Service bootstrap and wiring.
  - `channel/`: Delivery channel implementations (Email renderer, SMTP sender).
  - `infrastructure/`: Kafka Reader, SMTP client, Promethues metrics.
  - `model/`: Database entities (e.g., `NotificationDelivery`).
  - `repository/`: Data access for delivery state.
  - `worker/`: Core logic: `EmailWorker` (consumes Kafka) and `RetryWorker` (retries failed deliveries).
