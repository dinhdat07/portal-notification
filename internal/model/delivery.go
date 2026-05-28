package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	DeliveryStatusProcessing     = "processing"
	DeliveryStatusSent           = "sent"
	DeliveryStatusRetryScheduled = "retry_scheduled"
	DeliveryStatusDeadLetter     = "dead_letter"
	DeliveryStatusExpired        = "expired"
	DeliveryStatusSuperseded     = "superseded"

	DeliveryChannelEmail = "email"
)

type NotificationDelivery struct {
	ID uuid.UUID `gorm:"type:uuid;primaryKey"`

	EventID     string `gorm:"not null;uniqueIndex"`
	BusinessKey string `gorm:"index"`

	NotificationType string `gorm:"not null"`
	Channel          string `gorm:"not null"`

	RecipientEmail string `gorm:"not null"`
	RecipientName  string

	Template string         `gorm:"not null"`
	Data     datatypes.JSON `gorm:"type:jsonb;not null;default:'{}'"`

	Status string `gorm:"not null;index"`

	LastError *string

	RetryCount  int        `gorm:"not null;default:0"`
	MaxRetry    int        `gorm:"not null;default:3"`
	NextRetryAt *time.Time `gorm:"index"`

	ValidUntil *time.Time `gorm:"index"`

	SentAt       *time.Time
	DeadLetterAt *time.Time
	ExpiredAt    *time.Time
	SupersededAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
