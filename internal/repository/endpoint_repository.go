package repository

import (
	"context"
	"portal-notification/internal/model"

	"github.com/google/uuid"
)

type EndpointRepository interface {
	FindByUserAndProvider(ctx context.Context, userID uuid.UUID, provider model.EndpointProvider) (*model.NotificationEndpoint, error)
	Upsert(ctx context.Context, endpoint *model.NotificationEndpoint) error
}
