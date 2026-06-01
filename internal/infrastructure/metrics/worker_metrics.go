package metrics

import notificationmetrics "portal-notification/internal/metrics"

type PrometheusWorkerMetrics struct {
	reg *prometheusMetricsRegistry
}

var _ notificationmetrics.WorkerMetrics = (*PrometheusWorkerMetrics)(nil)

func (m *PrometheusWorkerMetrics) EventConsumed(notificationType string) {
	m.reg.notificationEventsConsumedTotal.Inc()
}

func (m *PrometheusWorkerMetrics) EventInvalid(reason string) {
	if reason == "" {
		reason = "unknown"
	}

	m.reg.notificationEventsInvalidTotal.WithLabelValues(reason).Inc()
}

func (m *PrometheusWorkerMetrics) DeliveryCreated(notificationType string) {
	m.reg.notificationDeliveryCreatedTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusWorkerMetrics) DeliveryDuplicate(status string) {
	m.reg.notificationDeliveryDuplicateTotal.WithLabelValues(normalizeLabel(status)).Inc()
}

func (m *PrometheusWorkerMetrics) DeliverySent(notificationType string, channel string) {
	m.reg.notificationDeliverySentTotal.WithLabelValues(normalizeLabel(notificationType), normalizeLabel(channel)).Inc()
}

func (m *PrometheusWorkerMetrics) DeliveryFailed(notificationType string, channel string) {
	m.reg.notificationDeliveryFailedTotal.WithLabelValues(normalizeLabel(notificationType), normalizeLabel(channel)).Inc()
}

func (m *PrometheusWorkerMetrics) RetryScheduled(notificationType string) {
	m.reg.notificationDeliveryRetryScheduledTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusWorkerMetrics) DeadLettered(notificationType string) {
	m.reg.notificationDeliveryDeadLetteredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusWorkerMetrics) Expired(notificationType string) {
	m.reg.notificationDeliveryExpiredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusWorkerMetrics) Superseded(notificationType string, count int64) {
	if count <= 0 {
		return
	}

	m.reg.notificationDeliverySupersededTotal.
		WithLabelValues(normalizeLabel(notificationType)).
		Add(float64(count))
}
