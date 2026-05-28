package email

import (
	"context"
	"portal-notification/config"
	"time"

	"github.com/sony/gobreaker"
)

type CircuitBreakerProxy struct {
	next    Mailer
	breaker *gobreaker.CircuitBreaker
}

func NewCircuitBreakerProxy(next Mailer, cfg config.CircuitBreakerConfig) *CircuitBreakerProxy {

	breaker := gobreaker.NewCircuitBreaker(
		gobreaker.Settings{
			Name:        "email-mailer",
			MaxRequests: cfg.MaxRequests,
			Interval:    time.Duration(cfg.IntervalSeconds) * time.Second,
			Timeout:     time.Duration(cfg.TimeoutSeconds) * time.Second,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= cfg.ConsecutiveFailures
			},
		},
	)

	return &CircuitBreakerProxy{
		next:    next,
		breaker: breaker,
	}
}

func (p *CircuitBreakerProxy) Send(ctx context.Context, msg Message) error {
	_, err := p.breaker.Execute(func() (any, error) {
		return nil, p.next.Send(ctx, msg)
	})
	return err
}
