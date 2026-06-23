package worker

import "time"

const (
	EventTypeNotificationRequested = "notification.requested"
)

const (
	ChannelEmail    = "email"
	ChannelTelegram = "telegram"
	ChannelPush     = "push"
)

const (
	NotificationTypeVerifyEmail   = "verify_email"
	NotificationTypeResetPassword = "reset_password"
	NotificationTypeSetPassword   = "set_password"
	NotificationTypeAnnouncement  = "announcement"
)

const (
	TemplateVerifyEmail   = "verify_email"
	TemplateResetPassword = "reset_password"
	TemplateSetPassword   = "set_password"
	TemplateAnnouncement  = "announcement"
)

type NotificationRequestedEvent struct {
	EventID          string         `json:"event_id"`
	OccurredAt       time.Time      `json:"occurred_at"`
	NotificationType string         `json:"notification_type"`
	Recipient        Recipient      `json:"recipient"`
	Template         string         `json:"template"`
	Data             map[string]any `json:"data"`
	ValidUntil       *time.Time     `json:"valid_until"`
	BusinessKey      string         `json:"business_key"`
}

type Recipient struct {
	UserID string `json:"user_id,omitempty"`
	Email  string `json:"email,omitempty"`
	Name   string `json:"name,omitempty"`
}

type Metadata struct {
	Source        string `json:"source"`
	CorrelationID string `json:"correlation_id,omitempty"`
}
