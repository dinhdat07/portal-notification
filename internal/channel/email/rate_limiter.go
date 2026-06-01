package email

import (
	"context"
)

type EmailRateLimiter struct{}

func NewEmailRateLimiter() *EmailRateLimiter {
	return &EmailRateLimiter{}
}

func (l *EmailRateLimiter) Wait(ctx context.Context) error {
	// For now, no rate limiting for email. Just pass through.
	return nil
}
