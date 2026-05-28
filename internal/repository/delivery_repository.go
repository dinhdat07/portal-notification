package repository

import (
	"context"
	"errors"
	"time"

	"portal-notification/internal/model"
)

var ErrDuplicateDelivery = errors.New("duplicate notification delivery")

type DeliveryRepository interface {
	CreateProcessing(ctx context.Context, delivery *model.NotificationDelivery) error
	FindByEventID(ctx context.Context, eventID string) (*model.NotificationDelivery, error)
	MarkSent(ctx context.Context, eventID string) error
	SupersedeRetryableByBusinessKey(ctx context.Context, businessKey string, excludeEventID string, reason string) (int64, error)
	ScheduleRetry(ctx context.Context, eventID string, retryCount int, lastError string, nextRetryAt time.Time) error

	MarkDeadLetter(ctx context.Context, eventID string, lastError string) error
	MarkExpired(ctx context.Context, eventID string, reason string) error
	MarkSuperseded(ctx context.Context, eventID string, reason string) error

	ClaimRetryDue(ctx context.Context, limit int) ([]model.NotificationDelivery, error)
}
