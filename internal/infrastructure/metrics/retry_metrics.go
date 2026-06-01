package metrics

import notificationmetrics "portal-notification/internal/metrics"

type PrometheusRetryMetrics struct {
	reg *prometheusMetricsRegistry
}

var _ notificationmetrics.RetryMetrics = (*PrometheusRetryMetrics)(nil)

func (m *PrometheusRetryMetrics) RetryBatchClaimed(count int) {
	if count <= 0 {
		return
	}

	m.reg.notificationRetryBatchClaimedTotal.Add(float64(count))
}

func (m *PrometheusRetryMetrics) Expired(notificationType string) {
	m.reg.notificationDeliveryExpiredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusRetryMetrics) DeadLettered(notificationType string) {
	m.reg.notificationDeliveryDeadLetteredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusRetryMetrics) DeliveryFailed(notificationType string, channel string) {
	m.reg.notificationDeliveryFailedTotal.WithLabelValues(normalizeLabel(notificationType), normalizeLabel(channel)).Inc()
}

func (m *PrometheusRetryMetrics) DeliverySent(notificationType string, channel string) {
	m.reg.notificationDeliverySentTotal.WithLabelValues(normalizeLabel(notificationType), normalizeLabel(channel)).Inc()
}

func (m *PrometheusRetryMetrics) RetryScheduled(notificationType string) {
	m.reg.notificationDeliveryRetryScheduledTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}
