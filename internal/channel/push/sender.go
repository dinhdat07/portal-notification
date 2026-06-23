package push

import (
	"context"
	"fmt"

	"portal-notification/internal/model"
	"portal-notification/internal/repository"

	"firebase.google.com/go/v4/messaging"
	"github.com/google/uuid"
)

type PushSender struct {
	repo     repository.EndpointRepository
	fcmClient *messaging.Client
}

func NewSender(repo repository.EndpointRepository, fcmClient *messaging.Client) *PushSender {
	return &PushSender{
		repo:      repo,
		fcmClient: fcmClient,
	}
}

func (s *PushSender) Send(ctx context.Context, recipient string, payload any) error {
	userID, err := uuid.Parse(recipient)
	if err != nil {
		return fmt.Errorf("invalid recipient UUID: %w", err)
	}

	endpoint, err := s.repo.FindByUserAndProvider(ctx, userID, model.EndpointProviderFirebase)
	if err != nil {
		return fmt.Errorf("failed to find firebase endpoint: %w", err)
	}
	if endpoint == nil {
		return fmt.Errorf("no firebase endpoint found for user %s", userID.String())
	}

	msg, ok := payload.(*messaging.Message)
	if !ok {
		return fmt.Errorf("payload must be a *messaging.Message")
	}

	msg.Token = endpoint.Endpoint

	_, err = s.fcmClient.Send(ctx, msg)
	if err != nil {
		return fmt.Errorf("firebase messaging send error: %w", err)
	}

	return nil
}
