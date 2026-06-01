package channel

import (
	"context"
)

// Validator validates the recipient information before processing.
type Validator interface {
	Validate(recipient string) error
}

// Template renders the final message payload using data.
type Template interface {
	Render(templateID string, data map[string]any) (any, error)
}

// Sender sends the fully rendered payload to the recipient.
type Sender interface {
	Send(ctx context.Context, recipient string, payload any) error
}

// RateLimiter manages the rate of sending notifications.
type RateLimiter interface {
	Wait(ctx context.Context) error
}

// NotificationFactory is the Abstract Factory interface.
type NotificationFactory interface {
	Validator() Validator
	Template() Template
	Sender() Sender
	RateLimiter() RateLimiter
}
