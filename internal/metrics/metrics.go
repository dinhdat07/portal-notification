package metrics

type EmailMetrics interface {
	EventConsumed(notificationType string)
	EventInvalid(reason string)
	DeliveryCreated(notificationType string)
	DeliveryDuplicate(status string)
	EmailSent(notificationType string)
	EmailFailed(notificationType string)
	RetryScheduled(notificationType string)
	DeadLettered(notificationType string)
	Expired(notificationType string)
	Superseded(notificationType string, count int64)
}

type RetryMetrics interface {
	RetryBatchClaimed(count int)
	Expired(notificationType string)
	DeadLettered(notificationType string)
	EmailFailed(notificationType string)
	EmailSent(notificationType string)
	RetryScheduled(notificationType string)
}

type Metrics interface {
	EmailMetrics
	RetryMetrics
}
