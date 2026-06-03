package model

import (
	"time"

	"github.com/google/uuid"
)

type EndpointProvider string

const (
	EndpointProviderTelegram EndpointProvider = "TELEGRAM"
	EndpointProviderFirebase EndpointProvider = "FIREBASE"
)

type NotificationEndpoint struct {
	ID uuid.UUID `gorm:"type:uuid;primaryKey"`

	UserID uuid.UUID `gorm:"type:uuid;not null;index"`

	Provider EndpointProvider `gorm:"type:varchar(50);not null;uniqueIndex:idx_provider_endpoint"`

	Endpoint string `gorm:"type:varchar(512);not null;uniqueIndex:idx_provider_endpoint"`

	DeviceName *string `gorm:"type:varchar(255)"`

	IsActive bool `gorm:"not null;default:true"`

	VerifiedAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
