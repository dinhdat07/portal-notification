package metrics

import notificationmetrics "portal-notification/internal/metrics"

type PrometheusEmailMetrics struct {
	reg *prometheusMetricsRegistry
}

var _ notificationmetrics.EmailMetrics = (*PrometheusEmailMetrics)(nil)

func (m *PrometheusEmailMetrics) EventConsumed(notificationType string) {
	m.reg.notificationEventsConsumedTotal.Inc()
}

func (m *PrometheusEmailMetrics) EventInvalid(reason string) {
	if reason == "" {
		reason = "unknown"
	}

	m.reg.notificationEventsInvalidTotal.WithLabelValues(reason).Inc()
}

func (m *PrometheusEmailMetrics) DeliveryCreated(notificationType string) {
	m.reg.notificationDeliveryCreatedTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusEmailMetrics) DeliveryDuplicate(status string) {
	m.reg.notificationDeliveryDuplicateTotal.WithLabelValues(normalizeLabel(status)).Inc()
}

func (m *PrometheusEmailMetrics) EmailSent(notificationType string) {
	m.reg.notificationEmailSentTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusEmailMetrics) EmailFailed(notificationType string) {
	m.reg.notificationEmailFailedTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusEmailMetrics) RetryScheduled(notificationType string) {
	m.reg.notificationEmailRetryScheduledTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusEmailMetrics) DeadLettered(notificationType string) {
	m.reg.notificationEmailDeadLetteredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusEmailMetrics) Expired(notificationType string) {
	m.reg.notificationEmailExpiredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusEmailMetrics) Superseded(notificationType string, count int64) {
	if count <= 0 {
		return
	}

	m.reg.notificationEmailSupersededTotal.
		WithLabelValues(normalizeLabel(notificationType)).
		Add(float64(count))
}
