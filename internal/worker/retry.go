package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	notificationmetrics "portal-notification/internal/metrics"
	"portal-notification/internal/model"
	"portal-notification/internal/repository"
)

type RetryWorkerConfig struct {
	Interval                    time.Duration
	BatchSize                   int
	MaxRetry                    int
	DeliveryRetryInitialBackoff time.Duration
	DeliveryRetryMaxBackoff     time.Duration
	DeliveryRetryJitterRatio    float64
}

type RetryWorker struct {
	emailSender  EmailSender
	deliveryRepo repository.DeliveryRepository
	logger       *slog.Logger
	metrics      notificationmetrics.RetryMetrics
	cfg          RetryWorkerConfig
}

func NewRetryWorker(
	emailSender EmailSender,
	deliveryRepo repository.DeliveryRepository,
	logger *slog.Logger,
	metrics notificationmetrics.RetryMetrics,
	cfg RetryWorkerConfig,
) *RetryWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 3
	}
	if cfg.DeliveryRetryInitialBackoff <= 0 {
		cfg.DeliveryRetryInitialBackoff = 30 * time.Second
	}
	if cfg.DeliveryRetryMaxBackoff <= 0 {
		cfg.DeliveryRetryMaxBackoff = 30 * time.Minute
	}
	if cfg.DeliveryRetryJitterRatio < 0 {
		cfg.DeliveryRetryJitterRatio = 0.2
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = notificationmetrics.NoopRetryMetrics{}
	}

	return &RetryWorker{
		emailSender:  emailSender,
		deliveryRepo: deliveryRepo,
		logger:       logger,
		metrics:      metrics,
		cfg:          cfg,
	}
}

func (w *RetryWorker) Run(ctx context.Context) error {
	w.logger.Info("retry_worker_started", slog.Duration("interval", w.cfg.Interval), slog.Int("batch_size", w.cfg.BatchSize))

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("retry_worker_stopped")
			return nil
		case <-ticker.C:
			if err := w.processRetryQueue(ctx); err != nil {
				w.logger.Error("process_retry_queue_failed", slog.Any("error", err))
			}
		}
	}
}

func (w *RetryWorker) processRetryQueue(ctx context.Context) error {
	deliveries, err := w.deliveryRepo.ClaimRetryDue(ctx, w.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("claim retry due: %w", err)
	}

	if len(deliveries) == 0 {
		return nil
	}

	w.metrics.RetryBatchClaimed(len(deliveries))

	w.logger.Info("claimed_retry_deliveries", slog.Int("count", len(deliveries)))

	for _, delivery := range deliveries {
		if err := w.processDelivery(ctx, delivery); err != nil {
			w.logger.Error("retry_delivery_failed", slog.String("event_id", delivery.EventID), slog.Any("error", err))
		}
	}

	return nil
}

func (w *RetryWorker) processDelivery(ctx context.Context, delivery model.NotificationDelivery) error {
	if isExpired(delivery.ValidUntil) {
		w.logger.Info("delivery_expired_before_retry", slog.String("event_id", delivery.EventID))
		if err := w.deliveryRepo.MarkExpired(ctx, delivery.EventID, "delivery expired before retry attempt"); err != nil {
			return err
		}
		w.metrics.Expired(delivery.NotificationType)
		return nil
	}

	var data map[string]any
	if err := json.Unmarshal(delivery.Data, &data); err != nil {
		w.logger.Error("unmarshal_delivery_data_failed", slog.String("event_id", delivery.EventID), slog.Any("error", err))
		if err := w.deliveryRepo.MarkDeadLetter(ctx, delivery.EventID, fmt.Sprintf("unmarshal data: %v", err)); err != nil {
			return err
		}
		w.metrics.DeadLettered(delivery.NotificationType)
		return nil
	}

	w.logger.Info("retrying_delivery", slog.String("event_id", delivery.EventID), slog.Int("retry_count", delivery.RetryCount), slog.String("recipient_email", delivery.RecipientEmail))

	if err := w.emailSender.Send(
		ctx,
		delivery.Template,
		delivery.RecipientEmail,
		delivery.RecipientName,
		data,
	); err != nil {
		w.metrics.EmailFailed(delivery.NotificationType)
		if updateErr := w.handleRetryFailure(ctx, delivery, err.Error()); updateErr != nil {
			return fmt.Errorf("send email retry failed: %w; update delivery retry state failed: %v", err, updateErr)
		}
		return err
	}

	w.metrics.EmailSent(delivery.NotificationType)

	w.logger.Info("notification_email_sent",
		slog.String("event_id", delivery.EventID),
		slog.String("business_key", delivery.BusinessKey),
		slog.String("notification_type", delivery.NotificationType),
		slog.String("template", delivery.Template),
		slog.String("recipient_email", delivery.RecipientEmail),
	)

	w.logger.Info("retry_delivery_successful", slog.String("event_id", delivery.EventID))
	return w.deliveryRepo.MarkSent(ctx, delivery.EventID)
}

func (w *RetryWorker) handleRetryFailure(ctx context.Context, delivery model.NotificationDelivery, lastError string) error {
	if isExpired(delivery.ValidUntil) {
		if err := w.deliveryRepo.MarkExpired(ctx, delivery.EventID, lastError); err != nil {
			return err
		}
		w.metrics.Expired(delivery.NotificationType)
		return nil
	}

	nextRetryCount := delivery.RetryCount + 1
	if nextRetryCount >= delivery.MaxRetry {
		if err := w.deliveryRepo.MarkDeadLetter(ctx, delivery.EventID, lastError); err != nil {
			return err
		}
		w.metrics.DeadLettered(delivery.NotificationType)
		return nil
	}

	nextAt := nextRetryAt(
		nextRetryCount,
		w.cfg.DeliveryRetryInitialBackoff,
		w.cfg.DeliveryRetryMaxBackoff,
		w.cfg.DeliveryRetryJitterRatio,
	)

	if delivery.ValidUntil != nil && !nextAt.Before(*delivery.ValidUntil) {
		if err := w.deliveryRepo.MarkExpired(ctx, delivery.EventID, lastError); err != nil {
			return err
		}
		w.metrics.Expired(delivery.NotificationType)
		return nil
	}

	if err := w.deliveryRepo.ScheduleRetry(ctx, delivery.EventID, nextRetryCount, lastError, nextAt); err != nil {
		return err
	}
	w.metrics.RetryScheduled(delivery.NotificationType)
	return nil
}
