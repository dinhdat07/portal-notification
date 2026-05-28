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
	m.reg.notificationEmailExpiredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusRetryMetrics) DeadLettered(notificationType string) {
	m.reg.notificationEmailDeadLetteredTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusRetryMetrics) EmailFailed(notificationType string) {
	m.reg.notificationEmailFailedTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusRetryMetrics) EmailSent(notificationType string) {
	m.reg.notificationEmailSentTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}

func (m *PrometheusRetryMetrics) RetryScheduled(notificationType string) {
	m.reg.notificationEmailRetryScheduledTotal.WithLabelValues(normalizeLabel(notificationType)).Inc()
}
