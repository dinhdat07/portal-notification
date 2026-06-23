package impl

import (
	"context"
	"portal-notification/internal/model"
	"portal-notification/internal/repository"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GormEndpointRepository struct {
	db *gorm.DB
}

func NewGormEndpointRepository(db *gorm.DB) repository.EndpointRepository {
	return &GormEndpointRepository{db: db}
}

func (r *GormEndpointRepository) FindByUserAndProvider(ctx context.Context, userID uuid.UUID, provider model.EndpointProvider) (*model.NotificationEndpoint, error) {
	var endpoint model.NotificationEndpoint
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND provider = ? AND is_active = ?", userID, provider, true).
		First(&endpoint).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil // return nil if not found, let sender handle it
		}
		return nil, err
	}
	return &endpoint, nil
}

func (r *GormEndpointRepository) Upsert(ctx context.Context, endpoint *model.NotificationEndpoint) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "provider"}, {Name: "endpoint"}},
		DoUpdates: clause.AssignmentColumns([]string{"user_id", "device_name", "is_active", "verified_at", "updated_at"}),
	}).Create(endpoint).Error
}
