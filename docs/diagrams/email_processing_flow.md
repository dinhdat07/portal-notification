# Email Processing & Retry Flow

This sequence diagram details how the Notification Service ensures at-least-once delivery, idempotency, and background retries when interacting with Kafka and SMTP.

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant Worker as Email Worker
    participant DB as Delivery DB
    participant Retry as Retry Worker
    participant SMTP as SMTP Server

    %% Main Kafka Consumption Flow
    Note over Kafka,SMTP: 1. KAFKA CONSUMPTION (Main Thread)
    
    Kafka-->>Worker: FetchMessage() -> {EventID: "123", Type: "Welcome"}
    
    rect rgb(240, 248, 255)
        note right of Worker: Idempotency Check & State Persistence
        Worker->>DB: BEGIN TX
        Worker->>DB: INSERT NotificationDelivery (Status: Processing)
        
        alt Duplicate EventID?
            DB-->>Worker: Unique Constraint Error
            Worker->>Kafka: CommitOffset() (Skip processing, move forward)
            Note over Worker: Flow Ends Here (Idempotent)
        else New Event
            DB-->>Worker: OK
            Worker->>DB: COMMIT TX
        end
    end
    
    %% SMTP Interaction
    Worker->>SMTP: Send Email
    
    alt SMTP Success
        SMTP-->>Worker: 250 OK
        Worker->>DB: UPDATE status = 'sent'
        Worker->>Kafka: CommitOffset()
    else SMTP Failure (e.g. Timeout)
        SMTP-->>Worker: Timeout / Refused
        Note right of Worker: We do NOT block Kafka!
        Worker->>DB: UPDATE status = 'retry_scheduled', next_retry_at = T+30s
        Worker->>Kafka: CommitOffset() (State is saved in DB)
    end

    %% Background Retry Flow
    Note over Kafka,SMTP: 2. BACKGROUND RETRY FLOW (Separate Thread)
    
    loop Every 10 Seconds
        Retry->>DB: Claim due retries (status = 'retry_scheduled' AND next_retry_at <= NOW) FOR UPDATE
        DB-->>Retry: Returns Failed Delivery "123"
        
        Retry->>SMTP: Retry Send Email
        
        alt Retry Success
            SMTP-->>Retry: 250 OK
            Retry->>DB: UPDATE status = 'sent'
        else Retry Fails Again
            SMTP-->>Retry: Error
            alt Max Retries Exceeded?
                Retry->>DB: UPDATE status = 'dead_letter'
            else Still Retrying
                Retry->>DB: UPDATE next_retry_at = T+60s (Exponential Backoff)
            end
        end
    end
```
