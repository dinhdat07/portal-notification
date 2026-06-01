package metrics

type WorkerMetrics interface {
	EventConsumed(notificationType string)
	EventInvalid(reason string)
	DeliveryCreated(notificationType string)
	DeliveryDuplicate(status string)
	DeliverySent(notificationType string, channel string)
	DeliveryFailed(notificationType string, channel string)
	RetryScheduled(notificationType string)
	DeadLettered(notificationType string)
	Expired(notificationType string)
	Superseded(notificationType string, count int64)
}

type RetryMetrics interface {
	RetryBatchClaimed(count int)
	Expired(notificationType string)
	DeadLettered(notificationType string)
	DeliveryFailed(notificationType string, channel string)
	DeliverySent(notificationType string, channel string)
	RetryScheduled(notificationType string)
}

type Metrics interface {
	WorkerMetrics
	RetryMetrics
}
