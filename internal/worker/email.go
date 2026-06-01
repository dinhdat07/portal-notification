package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"portal-notification/internal/channel"
	notificationmetrics "portal-notification/internal/metrics"
	"portal-notification/internal/model"
	"portal-notification/internal/repository"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// Message represents a message consumed from a message broker (e.g., Kafka).
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Original  any // Holds the underlying broker message for committing
}

// Consumer abstracts the message consumption process.
type Consumer interface {
	FetchMessage(ctx context.Context) (Message, error)
	CommitMessages(ctx context.Context, msgs ...Message) error
	Close() error
}

type Config struct {
	FetchRetryInitialBackoff    time.Duration
	FetchRetryMaxBackoff        time.Duration
	MaxRetry                    int
	DeliveryRetryInitialBackoff time.Duration
	DeliveryRetryMaxBackoff     time.Duration
	DeliveryRetryJitterRatio    float64
}

type Worker struct {
	consumer     Consumer
	router       *Router
	factories    map[string]channel.NotificationFactory
	deliveryRepo repository.DeliveryRepository
	txManager    repository.TxManager
	logger       *slog.Logger
	metrics      notificationmetrics.WorkerMetrics
	cfg          Config
}

func NewWorker(
	consumer Consumer,
	router *Router,
	factories map[string]channel.NotificationFactory,
	txManager repository.TxManager,
	deliveryRepo repository.DeliveryRepository,
	logger *slog.Logger,
	metrics notificationmetrics.WorkerMetrics,
	cfg Config,
) *Worker {
	if cfg.FetchRetryInitialBackoff <= 0 {
		cfg.FetchRetryInitialBackoff = time.Second
	}
	if cfg.FetchRetryMaxBackoff <= 0 {
		cfg.FetchRetryMaxBackoff = 30 * time.Second
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
		metrics = notificationmetrics.NoopWorkerMetrics{}
	}

	return &Worker{
		consumer:     consumer,
		router:       router,
		factories:    factories,
		deliveryRepo: deliveryRepo,
		txManager:    txManager,
		logger:       logger,
		metrics:      metrics,
		cfg:          cfg,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("email_notification_worker_started")

	backoff := w.cfg.FetchRetryInitialBackoff
	maxBackoff := w.cfg.FetchRetryMaxBackoff

	for {
		msg, err := w.consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				w.logger.Info("email_notification_worker_stopped")
				return nil
			}

			w.logger.WarnContext(ctx, "kafka_fetch_message_failed",
				slog.String("error", err.Error()),
				slog.Duration("retry_in", backoff),
			)

			select {
			case <-ctx.Done():
				w.logger.Info("email_notification_worker_stopped")
				return nil
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			continue
		}

		backoff = w.cfg.FetchRetryInitialBackoff

		w.logger.DebugContext(ctx, "kafka_message_fetched",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.String("key", string(msg.Key)),
			slog.Int("bytes", len(msg.Value)),
		)

		err = w.handleMessage(ctx, msg)
		if err != nil {
			w.logger.ErrorContext(ctx, "notification_message_handle_failed",
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset),
				slog.String("error", err.Error()),
			)

			if !errors.Is(err, ErrNonRetryable) {
				continue
			}
		}

		if err := w.consumer.CommitMessages(ctx, msg); err != nil {
			w.logger.ErrorContext(ctx, "kafka_commit_message_failed",
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset),
				slog.String("error", err.Error()),
			)

			return fmt.Errorf("commit kafka message: %w", err)
		}

		w.logger.DebugContext(ctx, "kafka_message_committed",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
		)
	}
}

func (w *Worker) handleMessage(ctx context.Context, msg Message) error {
	var event NotificationRequestedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		w.metrics.EventInvalid("unmarshal_failed")

		w.logger.WarnContext(ctx, "notification_event_unmarshal_failed",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)

		return fmt.Errorf("%w: unmarshal notification event: %v", ErrNonRetryable, err)
	}

	if err := validateEvent(event); err != nil {
		w.metrics.EventInvalid("validation_failed")

		w.logger.WarnContext(ctx, "notification_event_validation_failed",
			slog.String("event_id", event.EventID),
			slog.String("notification_type", event.NotificationType),
			slog.String("template", event.Template),
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)

		return fmt.Errorf("%w: validate notification event: %v", ErrNonRetryable, err)
	}

	w.metrics.EventConsumed(event.NotificationType)

	w.logger.InfoContext(ctx, "notification_event_consumed",
		slog.String("event_id", event.EventID),
		slog.String("business_key", event.BusinessKey),
		slog.String("notification_type", event.NotificationType),
		slog.String("template", event.Template),
		slog.String("recipient_user_id", event.Recipient.UserID),
		slog.String("recipient_email", event.Recipient.Email),
		slog.String("topic", msg.Topic),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
	)

	channels := w.router.DetermineChannels(event.NotificationType)
	if len(channels) == 0 {
		w.logger.WarnContext(ctx, "no_channels_determined_for_event", slog.String("notification_type", event.NotificationType))
		return nil
	}

	for _, channelType := range channels {
		factory, ok := w.factories[channelType]
		if !ok {
			w.logger.ErrorContext(ctx, "factory_not_found_for_channel", slog.String("channel", channelType))
			continue
		}

		var recipientStr string
		if channelType == ChannelEmail {
			recipientStr = event.Recipient.Email
			// add name to data for template rendering
			if event.Data == nil {
				event.Data = make(map[string]any)
			}
			event.Data["name"] = event.Recipient.Name
		}

		if err := factory.Validator().Validate(recipientStr); err != nil {
			w.logger.WarnContext(ctx, "channel_validation_failed", slog.String("channel", channelType), slog.String("error", err.Error()))
			continue
		}

		shouldSend, err := w.ensureDelivery(ctx, event, channelType)
		if err != nil {
			w.logger.ErrorContext(ctx, "ensure_delivery_failed", slog.String("channel", channelType), slog.String("error", err.Error()))
			return fmt.Errorf("ensure delivery for channel %s: %w", channelType, err)
		}

		if !shouldSend {
			w.logger.InfoContext(ctx, "notification_event_skipped",
				slog.String("event_id", event.EventID),
				slog.String("business_key", event.BusinessKey),
				slog.String("notification_type", event.NotificationType),
				slog.String("channel", channelType),
				slog.String("reason", "duplicate_or_terminal_state"),
			)
			continue
		}

		if isExpired(event.ValidUntil) {
			if err := w.deliveryRepo.MarkExpired(ctx, event.EventID, "delivery expired before first send"); err != nil {
				return fmt.Errorf("mark delivery expired for channel %s: %w", channelType, err)
			}

			w.metrics.Expired(event.NotificationType)

			w.logger.InfoContext(ctx, "notification_delivery_expired",
				slog.String("event_id", event.EventID),
				slog.String("business_key", event.BusinessKey),
				slog.String("notification_type", event.NotificationType),
				slog.String("channel", channelType),
				slog.String("reason", "expired_before_first_send"),
			)

			continue
		}

		if err := factory.RateLimiter().Wait(ctx); err != nil {
			w.logger.WarnContext(ctx, "rate_limiter_error", slog.String("channel", channelType), slog.String("error", err.Error()))
		}

		payload, err := factory.Template().Render(event.Template, event.Data)
		if err != nil {
			w.logger.ErrorContext(ctx, "template_render_failed", slog.String("channel", channelType), slog.String("error", err.Error()))
			w.handleSendFailure(ctx, event.EventID, err.Error())
			continue
		}

		if err := factory.Sender().Send(ctx, recipientStr, payload); err != nil {
			w.metrics.DeliveryFailed(event.NotificationType, channelType)
			if updateErr := w.handleSendFailure(ctx, event.EventID, err.Error()); updateErr != nil {
				return fmt.Errorf("send failed for %s but update delivery state failed: %v", channelType, updateErr)
			}

			w.logger.WarnContext(ctx, "notification_send_failed",
				slog.String("event_id", event.EventID),
				slog.String("business_key", event.BusinessKey),
				slog.String("notification_type", event.NotificationType),
				slog.String("template", event.Template),
				slog.String("channel", channelType),
				slog.String("recipient", recipientStr),
				slog.String("error", err.Error()),
			)

			continue
		}

		if err := w.deliveryRepo.MarkSent(ctx, event.EventID); err != nil {
			w.logger.ErrorContext(ctx, "mark_sent_failed", slog.String("channel", channelType), slog.String("error", err.Error()))
			return fmt.Errorf("mark sent for channel %s: %w", channelType, err)
		}

		w.metrics.DeliverySent(event.NotificationType, channelType)

		w.logger.InfoContext(ctx, "notification_delivery_sent",
			slog.String("event_id", event.EventID),
			slog.String("business_key", event.BusinessKey),
			slog.String("notification_type", event.NotificationType),
			slog.String("template", event.Template),
			slog.String("channel", channelType),
			slog.String("recipient", recipientStr),
		)
	}

	w.logger.InfoContext(ctx, "notification_event_handled",
		slog.String("event_id", event.EventID),
		slog.String("business_key", event.BusinessKey),
		slog.String("notification_type", event.NotificationType),
		slog.String("template", event.Template),
		slog.String("topic", msg.Topic),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
	)

	return nil
}

func (w *Worker) ensureDelivery(ctx context.Context, event NotificationRequestedEvent, channel string) (bool, error) {
	data, err := json.Marshal(event.Data)
	if err != nil {
		w.metrics.EventInvalid("marshal_delivery_data_failed")
		return false, fmt.Errorf("%w: marshal delivery data: %v", ErrNonRetryable, err)
	}

	now := time.Now().UTC()

	delivery := &model.NotificationDelivery{
		ID:               uuid.New(),
		EventID:          event.EventID,
		BusinessKey:      event.BusinessKey,
		NotificationType: event.NotificationType,
		Channel:          channel,
		RecipientEmail:   event.Recipient.Email,
		RecipientName:    event.Recipient.Name,
		Template:         event.Template,
		Data:             datatypes.JSON(data),
		Status:           model.DeliveryStatusProcessing,
		MaxRetry:         w.cfg.MaxRetry,
		ValidUntil:       event.ValidUntil,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	var shouldSend bool

	err = w.txManager.WithTx(ctx, func(ctx context.Context) error {
		err := w.deliveryRepo.CreateProcessing(ctx, delivery)
		if err == nil {
			rows, err := w.deliveryRepo.SupersedeRetryableByBusinessKey(
				ctx,
				event.BusinessKey,
				event.EventID,
				"superseded by newer notification event",
			)
			if err != nil {
				return fmt.Errorf("supersede previous retryable deliveries: %w", err)
			}

			shouldSend = true

			w.metrics.DeliveryCreated(event.NotificationType)

			w.logger.InfoContext(ctx, "notification_delivery_created",
				slog.String("event_id", event.EventID),
				slog.String("business_key", event.BusinessKey),
				slog.String("notification_type", event.NotificationType),
				slog.String("channel", model.DeliveryChannelEmail),
				slog.String("status", model.DeliveryStatusProcessing),
			)

			if rows > 0 {
				w.metrics.Superseded(event.NotificationType, rows)

				w.logger.InfoContext(ctx, "notification_delivery_superseded_previous_retryable",
					slog.String("event_id", event.EventID),
					slog.String("business_key", event.BusinessKey),
					slog.String("notification_type", event.NotificationType),
					slog.Int64("count", rows),
					slog.String("reason", "superseded by newer notification event"),
				)
			}

			return nil
		}

		if !errors.Is(err, repository.ErrDuplicateDelivery) {
			return fmt.Errorf("create notification delivery: %w", err)
		}

		existing, err := w.deliveryRepo.FindByEventID(ctx, event.EventID)
		if err != nil {
			return fmt.Errorf("find existing notification delivery: %w", err)
		}

		w.metrics.DeliveryDuplicate(existing.Status)

		w.logger.InfoContext(ctx, "notification_delivery_duplicate_detected",
			slog.String("event_id", event.EventID),
			slog.String("business_key", event.BusinessKey),
			slog.String("notification_type", event.NotificationType),
			slog.String("existing_status", existing.Status),
		)

		switch existing.Status {
		case model.DeliveryStatusSent,
			model.DeliveryStatusProcessing,
			model.DeliveryStatusRetryScheduled,
			model.DeliveryStatusDeadLetter,
			model.DeliveryStatusExpired,
			model.DeliveryStatusSuperseded:
			shouldSend = false
			return nil

		default:
			w.logger.ErrorContext(ctx, "notification_delivery_unknown_status",
				slog.String("event_id", event.EventID),
				slog.String("status", existing.Status),
			)

			return fmt.Errorf("unknown delivery status: %s", existing.Status)
		}
	})

	if err != nil {
		return false, err
	}

	return shouldSend, nil
}
func validateEvent(event NotificationRequestedEvent) error {
	if event.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if event.NotificationType == "" {
		return fmt.Errorf("notification_type is required")
	}
	return nil
}

func (w *Worker) handleSendFailure(ctx context.Context, eventID string, lastError string) error {
	delivery, err := w.deliveryRepo.FindByEventID(ctx, eventID)
	if err != nil {
		w.logger.ErrorContext(ctx, "notification_delivery_find_failed",
			slog.String("event_id", eventID),
			slog.String("error", err.Error()),
		)

		return fmt.Errorf("find delivery before retry scheduling: %w", err)
	}

	if isExpired(delivery.ValidUntil) {
		if err := w.deliveryRepo.MarkExpired(ctx, eventID, lastError); err != nil {
			return err
		}

		w.metrics.Expired(delivery.NotificationType)

		w.logger.InfoContext(ctx, "notification_delivery_expired",
			slog.String("event_id", eventID),
			slog.String("business_key", delivery.BusinessKey),
			slog.String("notification_type", delivery.NotificationType),
			slog.String("reason", "expired_after_send_failure"),
			slog.String("last_error", lastError),
		)

		return nil
	}

	nextRetryCount := delivery.RetryCount + 1
	if nextRetryCount >= delivery.MaxRetry {
		if err := w.deliveryRepo.MarkDeadLetter(ctx, eventID, lastError); err != nil {
			return err
		}

		w.metrics.DeadLettered(delivery.NotificationType)

		w.logger.ErrorContext(ctx, "notification_delivery_dead_lettered",
			slog.String("event_id", eventID),
			slog.String("business_key", delivery.BusinessKey),
			slog.String("notification_type", delivery.NotificationType),
			slog.Int("retry_count", nextRetryCount),
			slog.Int("max_retry", delivery.MaxRetry),
			slog.String("last_error", lastError),
		)

		return nil
	}

	nextAt := nextRetryAt(
		nextRetryCount,
		w.cfg.DeliveryRetryInitialBackoff,
		w.cfg.DeliveryRetryMaxBackoff,
		w.cfg.DeliveryRetryJitterRatio,
	)

	if delivery.ValidUntil != nil && !nextAt.Before(*delivery.ValidUntil) {
		if err := w.deliveryRepo.MarkExpired(ctx, eventID, lastError); err != nil {
			return err
		}

		w.metrics.Expired(delivery.NotificationType)

		w.logger.InfoContext(ctx, "notification_delivery_expired",
			slog.String("event_id", eventID),
			slog.String("business_key", delivery.BusinessKey),
			slog.String("notification_type", delivery.NotificationType),
			slog.String("reason", "next_retry_exceeds_valid_until"),
			slog.Time("next_retry_at", nextAt),
			slog.String("last_error", lastError),
		)

		return nil
	}

	if err := w.deliveryRepo.ScheduleRetry(ctx, eventID, nextRetryCount, lastError, nextAt); err != nil {
		return err
	}

	w.metrics.RetryScheduled(delivery.NotificationType)

	w.logger.WarnContext(ctx, "notification_delivery_retry_scheduled",
		slog.String("event_id", eventID),
		slog.String("business_key", delivery.BusinessKey),
		slog.String("notification_type", delivery.NotificationType),
		slog.Int("retry_count", nextRetryCount),
		slog.Int("max_retry", delivery.MaxRetry),
		slog.Time("next_retry_at", nextAt),
		slog.String("last_error", lastError),
	)

	return nil
}

func isExpired(validUntil *time.Time) bool {
	if validUntil == nil {
		return false
	}

	return !time.Now().UTC().Before(validUntil.UTC())
}

func nextRetryAt(
	retryCount int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	jitterRatio float64,
) time.Time {
	delay := initialBackoff
	if delay <= 0 {
		delay = 30 * time.Second
	}

	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Minute
	}

	for i := 1; i < retryCount; i++ {
		delay *= 2
		if delay > maxBackoff {
			delay = maxBackoff
			break
		}
	}

	delay = applyJitter(delay, jitterRatio)

	return time.Now().UTC().Add(delay)
}

func applyJitter(delay time.Duration, ratio float64) time.Duration {
	if ratio <= 0 {
		return delay
	}

	if ratio > 1 {
		ratio = 1
	}

	delta := float64(delay) * ratio
	minDelay := float64(delay) - delta
	maxDelay := float64(delay) + delta

	jittered := minDelay + rand.Float64()*(maxDelay-minDelay)
	if jittered < float64(time.Millisecond) {
		return time.Millisecond
	}

	return time.Duration(jittered)
}
