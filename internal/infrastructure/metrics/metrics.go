package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusMetricsRegistry struct {
	notificationEventsConsumedTotal      prometheus.Counter
	notificationEventsInvalidTotal       *prometheus.CounterVec
	notificationDeliveryCreatedTotal     *prometheus.CounterVec
	notificationDeliveryDuplicateTotal   *prometheus.CounterVec
	notificationEmailSentTotal           *prometheus.CounterVec
	notificationEmailFailedTotal         *prometheus.CounterVec
	notificationEmailRetryScheduledTotal *prometheus.CounterVec
	notificationEmailDeadLetteredTotal   *prometheus.CounterVec
	notificationEmailExpiredTotal        *prometheus.CounterVec
	notificationEmailSupersededTotal     *prometheus.CounterVec
	notificationRetryBatchClaimedTotal   prometheus.Counter
}

func NewPrometheusMetrics(registerer prometheus.Registerer) (*PrometheusEmailMetrics, *PrometheusRetryMetrics) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	reg := &prometheusMetricsRegistry{
		notificationEventsConsumedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_events_consumed_total",
			Help: "Total number of valid notification events consumed from Kafka.",
		}),
		notificationEventsInvalidTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_events_invalid_total",
			Help: "Total number of notification events that were invalid or failed to unmarshal.",
		}, []string{"reason"}),
		notificationDeliveryCreatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_created_total",
			Help: "Total number of new notification delivery records created in the database.",
		}, []string{"notification_type"}),
		notificationDeliveryDuplicateTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_duplicate_total",
			Help: "Total number of duplicate notification events detected and skipped.",
		}, []string{"status"}),
		notificationEmailSentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_email_sent_total",
			Help: "Total number of notification emails successfully sent via SMTP.",
		}, []string{"notification_type"}),
		notificationEmailFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_email_failed_total",
			Help: "Total number of notification email send attempts that failed.",
		}, []string{"notification_type"}),
		notificationEmailRetryScheduledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_email_retry_scheduled_total",
			Help: "Total number of notification deliveries scheduled for retry.",
		}, []string{"notification_type"}),
		notificationEmailDeadLetteredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_email_dead_lettered_total",
			Help: "Total number of notification deliveries marked as dead lettered.",
		}, []string{"notification_type"}),
		notificationEmailExpiredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_email_expired_total",
			Help: "Total number of notification deliveries marked as expired.",
		}, []string{"notification_type"}),
		notificationEmailSupersededTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_email_superseded_total",
			Help: "Total number of notification deliveries superseded by newer events.",
		}, []string{"notification_type"}),
		notificationRetryBatchClaimedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_retry_batch_claimed_total",
			Help: "Total number of notification deliveries claimed by the retry worker.",
		}),
	}

	registerer.MustRegister(
		reg.notificationEventsConsumedTotal,
		reg.notificationEventsInvalidTotal,
		reg.notificationDeliveryCreatedTotal,
		reg.notificationDeliveryDuplicateTotal,
		reg.notificationEmailSentTotal,
		reg.notificationEmailFailedTotal,
		reg.notificationEmailRetryScheduledTotal,
		reg.notificationEmailDeadLetteredTotal,
		reg.notificationEmailExpiredTotal,
		reg.notificationEmailSupersededTotal,
		reg.notificationRetryBatchClaimedTotal,
	)

	return &PrometheusEmailMetrics{reg: reg}, &PrometheusRetryMetrics{reg: reg}
}

func normalizeLabel(value string) string {
	if value == "" {
		return "unknown"
	}

	return value
}
