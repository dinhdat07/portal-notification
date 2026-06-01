package email

import (
	"context"
	"fmt"
)

type Mailer interface {
	Send(ctx context.Context, msg Message) error
}

type Sender struct {
	mailer Mailer
}

func NewSender(mailer Mailer) *Sender {
	return &Sender{
		mailer: mailer,
	}
}

func (s *Sender) Send(ctx context.Context, recipient string, payload any) error {
	if recipient == "" {
		return fmt.Errorf("email recipient is required")
	}

	msg, ok := payload.(Message)
	if !ok {
		return fmt.Errorf("invalid payload type for email sender")
	}

	msg.To = recipient

	if err := s.mailer.Send(ctx, msg); err != nil {
		return fmt.Errorf("send email: %w", err)
	}

	return nil
}
