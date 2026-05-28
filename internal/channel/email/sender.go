package email

import (
	"context"
	"fmt"
)

type Mailer interface {
	Send(ctx context.Context, msg Message) error
}

type Renderer interface {
	Render(template string, name string, data map[string]any) (Message, error)
}

type Sender struct {
	renderer Renderer
	mailer   Mailer
}

func NewSender(renderer Renderer, mailer Mailer) *Sender {
	return &Sender{
		renderer: renderer,
		mailer:   mailer,
	}
}

func (s *Sender) Send(
	ctx context.Context,
	template string,
	to string,
	name string,
	data map[string]any,
) error {
	if to == "" {
		return fmt.Errorf("email recipient is required")
	}

	msg, err := s.renderer.Render(template, name, data)
	if err != nil {
		return err
	}

	msg.To = to
	msg.Name = name

	if err := s.mailer.Send(ctx, msg); err != nil {
		return fmt.Errorf("send email: %w", err)
	}

	return nil
}
