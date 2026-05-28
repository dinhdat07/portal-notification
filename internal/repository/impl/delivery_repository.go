package impl

import (
	"context"
	"errors"
	"time"

	"portal-notification/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GormDeliveryRepository struct {
	db *gorm.DB
}

func NewGormDeliveryRepository(db *gorm.DB) *GormDeliveryRepository {
	return &GormDeliveryRepository{db: db}
}

func (r *GormDeliveryRepository) CreateProcessing(ctx context.Context, delivery *model.NotificationDelivery) error {
	now := time.Now().UTC()
	if delivery.CreatedAt.IsZero() {
		delivery.CreatedAt = now
	}
	delivery.UpdatedAt = now

	err := r.db.WithContext(ctx).Create(delivery).Error
	if err == nil {
		return nil
	}

	return err
}

func (r *GormDeliveryRepository) SupersedeRetryableByBusinessKey(ctx context.Context, businessKey string, excludeEventID string, reason string) (int64, error) {
	if businessKey == "" {
		return 0, nil
	}

	now := time.Now().UTC()

	query := r.getDB(ctx).
		Model(&model.NotificationDelivery{}).
		Where("business_key = ?", businessKey).
		Where("status = ?", model.DeliveryStatusRetryScheduled)

	if excludeEventID != "" {
		query = query.Where("event_id <> ?", excludeEventID)
	}

	result := query.Updates(map[string]any{
		"status":        model.DeliveryStatusSuperseded,
		"last_error":    reason,
		"next_retry_at": nil,
		"superseded_at": now,
		"updated_at":    now,
	})

	if result.Error != nil {
		return 0, result.Error
	}

	return result.RowsAffected, nil
}

func (r *GormDeliveryRepository) FindByEventID(ctx context.Context, eventID string) (*model.NotificationDelivery, error) {
	var delivery model.NotificationDelivery

	err := r.db.WithContext(ctx).
		Where("event_id = ?", eventID).
		First(&delivery).Error

	if err != nil {
		return nil, err
	}

	return &delivery, nil
}

func (r *GormDeliveryRepository) MarkSent(ctx context.Context, eventID string) error {
	now := time.Now().UTC()

	result := r.db.WithContext(ctx).
		Model(&model.NotificationDelivery{}).
		Where("event_id = ?", eventID).
		Where("status = ?", model.DeliveryStatusProcessing).
		Updates(map[string]any{
			"status":        model.DeliveryStatusSent,
			"sent_at":       now,
			"last_error":    "",
			"next_retry_at": nil,
			"updated_at":    now,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (r *GormDeliveryRepository) ScheduleRetry(ctx context.Context, eventID string, retryCount int, lastError string, nextRetryAt time.Time) error {
	now := time.Now().UTC()

	result := r.db.WithContext(ctx).
		Model(&model.NotificationDelivery{}).
		Where("event_id = ?", eventID).
		Where("status IN ?", []string{
			model.DeliveryStatusProcessing,
			model.DeliveryStatusRetryScheduled,
		}).
		Updates(map[string]any{
			"status":         model.DeliveryStatusRetryScheduled,
			"retry_count":    retryCount,
			"next_retry_at":  nextRetryAt,
			"last_error":     lastError,
			"dead_letter_at": nil,
			"expired_at":     nil,
			"superseded_at":  nil,
			"updated_at":     now,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (r *GormDeliveryRepository) MarkDeadLetter(ctx context.Context, eventID string, lastError string) error {
	now := time.Now().UTC()

	result := r.db.WithContext(ctx).
		Model(&model.NotificationDelivery{}).
		Where("event_id = ?", eventID).
		Where("status IN ?", []string{
			model.DeliveryStatusProcessing,
			model.DeliveryStatusRetryScheduled,
		}).
		Updates(map[string]any{
			"status":         model.DeliveryStatusDeadLetter,
			"last_error":     lastError,
			"next_retry_at":  nil,
			"dead_letter_at": now,
			"updated_at":     now,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (r *GormDeliveryRepository) MarkExpired(ctx context.Context, eventID string, reason string) error {
	now := time.Now().UTC()

	result := r.db.WithContext(ctx).
		Model(&model.NotificationDelivery{}).
		Where("event_id = ?", eventID).
		Where("status IN ?", []string{
			model.DeliveryStatusProcessing,
			model.DeliveryStatusRetryScheduled,
		}).
		Updates(map[string]any{
			"status":        model.DeliveryStatusExpired,
			"last_error":    reason,
			"next_retry_at": nil,
			"expired_at":    now,
			"updated_at":    now,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (r *GormDeliveryRepository) MarkSuperseded(ctx context.Context, eventID string, reason string) error {
	now := time.Now().UTC()

	result := r.db.WithContext(ctx).
		Model(&model.NotificationDelivery{}).
		Where("event_id = ?", eventID).
		Where("status IN ?", []string{
			model.DeliveryStatusProcessing,
			model.DeliveryStatusRetryScheduled,
		}).
		Updates(map[string]any{
			"status":        model.DeliveryStatusSuperseded,
			"last_error":    reason,
			"next_retry_at": nil,
			"superseded_at": now,
			"updated_at":    now,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (r *GormDeliveryRepository) ClaimRetryDue(ctx context.Context, limit int) ([]model.NotificationDelivery, error) {
	var deliveries []model.NotificationDelivery
	now := time.Now().UTC()

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
				Options:  "SKIP LOCKED",
			}).
			Where("status = ?", model.DeliveryStatusRetryScheduled).
			Where("next_retry_at IS NOT NULL AND next_retry_at <= ?", now).
			Where("(valid_until IS NULL OR valid_until > ?)", now).
			Order("next_retry_at ASC").
			Limit(limit).
			Find(&deliveries).Error; err != nil {
			return err
		}

		if len(deliveries) == 0 {
			return nil
		}

		eventIDs := make([]string, 0, len(deliveries))
		for _, delivery := range deliveries {
			eventIDs = append(eventIDs, delivery.EventID)
		}

		result := tx.
			Model(&model.NotificationDelivery{}).
			Where("event_id IN ?", eventIDs).
			Where("status = ?", model.DeliveryStatusRetryScheduled).
			Updates(map[string]any{
				"status":     model.DeliveryStatusProcessing,
				"updated_at": now,
			})

		if result.Error != nil {
			return result.Error
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return deliveries, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (r *GormDeliveryRepository) getDB(ctx context.Context) *gorm.DB {
	if tx, ok := ctx.Value(txKey{}).(*gorm.DB); ok {
		return tx.WithContext(ctx)
	}
	return r.db.WithContext(ctx)
}
