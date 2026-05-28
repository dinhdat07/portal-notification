# Notification Service Architecture

The Notification Service is built to process events asynchronously with strong guarantees regarding **Idempotency** and **Eventual Consistency**.

## 🏗 Core Design Principles

### 1. Manual Kafka Offset Management
By default, Kafka consumers often auto-commit offsets periodically. This service sets `CommitInterval: 0`, completely disabling auto-commit.
- **WHY**: If the service auto-commits a message but crashes before sending the email, the message is lost forever.
- **HOW**: The worker explicitly calls `CommitMessages()` **only after** it has successfully persisted the delivery intent or outcome into its own PostgreSQL database.

### 2. Idempotent Consumer Pattern
Kafka guarantees "At-Least-Once" delivery, meaning a message might be delivered twice (e.g., if the consumer crashes after processing but before committing the offset).
- **WHY**: We cannot send the same welcome email to a user twice.
- **HOW**: Before processing a message, the worker creates a record in the `NotificationDelivery` table. If the database returns a unique constraint error (e.g., `EventID` already exists), the worker knows it's a duplicate. It skips processing the email but **still commits the Kafka offset** to move past the message.

### 3. Decoupling External Failures (SMTP) from Kafka
If the SMTP server goes down, we do not want the Kafka consumer to crash or block the partition (Head-of-Line Blocking).
- **WHY**: Blocking the partition would prevent other types of notifications from being sent.
- **HOW**: If `EmailSender.Send()` fails:
  1. The worker catches the error.
  2. It updates the database record to `Status = RetryScheduled` and sets a `NextRetryAt` timestamp using Exponential Backoff.
  3. It **commits the Kafka offset** (since the failure is now safely stored in the database).
  4. A separate `RetryWorker` runs in the background, polling the database for due retries and re-attempting the SMTP delivery.

## 🔄 Delivery States
A `NotificationDelivery` record transitions through several states:
1. `processing`: Inserted when the Kafka message is first read.
2. `sent`: Marked if the SMTP call succeeds.
3. `retry_scheduled`: Marked if the SMTP call fails.
4. `dead_letter`: Marked if the retry count exceeds `MaxRetry`. Requires manual intervention.
5. `expired`: Marked if the event's `ValidUntil` timestamp passes before it could be sent.
6. `superseded`: Marked if a newer event with the same business key arrives, making the old one obsolete.
