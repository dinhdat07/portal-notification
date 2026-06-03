package push

import (
	"context"
	"portal-notification/internal/channel"
)

type PushRateLimiter struct{}

func NewRateLimiter() *PushRateLimiter {
	return &PushRateLimiter{}
}

func (r *PushRateLimiter) Wait(ctx context.Context) error {
	return nil // Firebase SDK handles some its own retries/rate limits, or we could add it here
}

type PushFactory struct {
	validator   channel.Validator
	template    channel.Template
	sender      channel.Sender
	rateLimiter channel.RateLimiter
}

func NewFactory(validator channel.Validator, template channel.Template, sender channel.Sender, rateLimiter channel.RateLimiter) *PushFactory {
	return &PushFactory{
		validator:   validator,
		template:    template,
		sender:      sender,
		rateLimiter: rateLimiter,
	}
}

func (f *PushFactory) Validator() channel.Validator {
	return f.validator
}

func (f *PushFactory) Template() channel.Template {
	return f.template
}

func (f *PushFactory) Sender() channel.Sender {
	return f.sender
}

func (f *PushFactory) RateLimiter() channel.RateLimiter {
	return f.rateLimiter
}
