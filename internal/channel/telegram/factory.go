package telegram

import (
	"context"
	"portal-notification/internal/channel"
)

type TelegramRateLimiter struct{}

func NewRateLimiter() *TelegramRateLimiter {
	return &TelegramRateLimiter{}
}

func (r *TelegramRateLimiter) Wait(ctx context.Context) error {
	return nil // Basic rate limiter for now
}

type TelegramFactory struct {
	validator   channel.Validator
	template    channel.Template
	sender      channel.Sender
	rateLimiter channel.RateLimiter
}

func NewFactory(validator channel.Validator, template channel.Template, sender channel.Sender, rateLimiter channel.RateLimiter) *TelegramFactory {
	return &TelegramFactory{
		validator:   validator,
		template:    template,
		sender:      sender,
		rateLimiter: rateLimiter,
	}
}

func (f *TelegramFactory) Validator() channel.Validator {
	return f.validator
}

func (f *TelegramFactory) Template() channel.Template {
	return f.template
}

func (f *TelegramFactory) Sender() channel.Sender {
	return f.sender
}

func (f *TelegramFactory) RateLimiter() channel.RateLimiter {
	return f.rateLimiter
}
