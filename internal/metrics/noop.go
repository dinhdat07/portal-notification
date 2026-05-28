package metrics

type NoopEmailMetrics struct{}

var _ EmailMetrics = NoopEmailMetrics{}

func (NoopEmailMetrics) EventConsumed(notificationType string)           {}
func (NoopEmailMetrics) EventInvalid(reason string)                      {}
func (NoopEmailMetrics) DeliveryCreated(notificationType string)         {}
func (NoopEmailMetrics) DeliveryDuplicate(status string)                 {}
func (NoopEmailMetrics) EmailSent(notificationType string)               {}
func (NoopEmailMetrics) EmailFailed(notificationType string)             {}
func (NoopEmailMetrics) RetryScheduled(notificationType string)          {}
func (NoopEmailMetrics) DeadLettered(notificationType string)            {}
func (NoopEmailMetrics) Expired(notificationType string)                 {}
func (NoopEmailMetrics) Superseded(notificationType string, count int64) {}

type NoopRetryMetrics struct{}

var _ RetryMetrics = NoopRetryMetrics{}

func (NoopRetryMetrics) RetryBatchClaimed(count int)            {}
func (NoopRetryMetrics) Expired(notificationType string)        {}
func (NoopRetryMetrics) DeadLettered(notificationType string)   {}
func (NoopRetryMetrics) EmailFailed(notificationType string)    {}
func (NoopRetryMetrics) EmailSent(notificationType string)      {}
func (NoopRetryMetrics) RetryScheduled(notificationType string) {}
