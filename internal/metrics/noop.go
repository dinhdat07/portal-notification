package metrics

type NoopWorkerMetrics struct{}

var _ WorkerMetrics = NoopWorkerMetrics{}

func (NoopWorkerMetrics) EventConsumed(notificationType string)           {}
func (NoopWorkerMetrics) EventInvalid(reason string)                      {}
func (NoopWorkerMetrics) DeliveryCreated(notificationType string)         {}
func (NoopWorkerMetrics) DeliveryDuplicate(status string)                 {}
func (NoopWorkerMetrics) DeliverySent(notificationType string, channel string) {}
func (NoopWorkerMetrics) DeliveryFailed(notificationType string, channel string) {}
func (NoopWorkerMetrics) RetryScheduled(notificationType string)          {}
func (NoopWorkerMetrics) DeadLettered(notificationType string)            {}
func (NoopWorkerMetrics) Expired(notificationType string)                 {}
func (NoopWorkerMetrics) Superseded(notificationType string, count int64) {}

type NoopRetryMetrics struct{}

var _ RetryMetrics = NoopRetryMetrics{}

func (NoopRetryMetrics) RetryBatchClaimed(count int)            {}
func (NoopRetryMetrics) Expired(notificationType string)        {}
func (NoopRetryMetrics) DeadLettered(notificationType string)   {}
func (NoopRetryMetrics) DeliveryFailed(notificationType string, channel string) {}
func (NoopRetryMetrics) DeliverySent(notificationType string, channel string)   {}
func (NoopRetryMetrics) RetryScheduled(notificationType string) {}
