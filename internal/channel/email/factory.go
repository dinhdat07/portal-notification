package email

import (
	"portal-notification/internal/channel"
)

type Factory struct {
	validator   channel.Validator
	template    channel.Template
	sender      channel.Sender
	rateLimiter channel.RateLimiter
}

func NewFactory(sender channel.Sender) *Factory {
	return &Factory{
		validator:   NewEmailValidator(),
		template:    NewEmailRenderer(),
		sender:      sender,
		rateLimiter: NewEmailRateLimiter(),
	}
}

func (f *Factory) Validator() channel.Validator {
	return f.validator
}

func (f *Factory) Template() channel.Template {
	return f.template
}

func (f *Factory) Sender() channel.Sender {
	return f.sender
}

func (f *Factory) RateLimiter() channel.RateLimiter {
	return f.rateLimiter
}
