package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusMetricsRegistry struct {
	notificationEventsConsumedTotal      prometheus.Counter
	notificationEventsInvalidTotal       *prometheus.CounterVec
	notificationDeliveryCreatedTotal     *prometheus.CounterVec
	notificationDeliveryDuplicateTotal   *prometheus.CounterVec
	notificationDeliverySentTotal           *prometheus.CounterVec
	notificationDeliveryFailedTotal         *prometheus.CounterVec
	notificationDeliveryRetryScheduledTotal *prometheus.CounterVec
	notificationDeliveryDeadLetteredTotal   *prometheus.CounterVec
	notificationDeliveryExpiredTotal        *prometheus.CounterVec
	notificationDeliverySupersededTotal     *prometheus.CounterVec
	notificationRetryBatchClaimedTotal   prometheus.Counter
}

func NewPrometheusMetrics(registerer prometheus.Registerer) (*PrometheusWorkerMetrics, *PrometheusRetryMetrics) {
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
		notificationDeliverySentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_sent_total",
			Help: "Total number of notification deliveries successfully sent.",
		}, []string{"notification_type", "channel"}),
		notificationDeliveryFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_failed_total",
			Help: "Total number of notification delivery send attempts that failed.",
		}, []string{"notification_type", "channel"}),
		notificationDeliveryRetryScheduledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_retry_scheduled_total",
			Help: "Total number of notification deliveries scheduled for retry.",
		}, []string{"notification_type"}),
		notificationDeliveryDeadLetteredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_dead_lettered_total",
			Help: "Total number of notification deliveries marked as dead lettered.",
		}, []string{"notification_type"}),
		notificationDeliveryExpiredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_expired_total",
			Help: "Total number of notification deliveries marked as expired.",
		}, []string{"notification_type"}),
		notificationDeliverySupersededTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_superseded_total",
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
		reg.notificationDeliverySentTotal,
		reg.notificationDeliveryFailedTotal,
		reg.notificationDeliveryRetryScheduledTotal,
		reg.notificationDeliveryDeadLetteredTotal,
		reg.notificationDeliveryExpiredTotal,
		reg.notificationDeliverySupersededTotal,
		reg.notificationRetryBatchClaimedTotal,
	)

	return &PrometheusWorkerMetrics{reg: reg}, &PrometheusRetryMetrics{reg: reg}
}

func normalizeLabel(value string) string {
	if value == "" {
		return "unknown"
	}

	return value
}
