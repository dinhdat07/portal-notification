package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"portal-notification/internal/model"
	"portal-notification/internal/repository"

	"github.com/google/uuid"
)

type TelegramSender struct {
	repo     repository.EndpointRepository
	botToken string
	apiURL   string
	client   *http.Client
}

func NewSender(repo repository.EndpointRepository, botToken string, apiURL string) *TelegramSender {
	return &TelegramSender{
		repo:     repo,
		botToken: botToken,
		apiURL:   apiURL,
		client:   &http.Client{},
	}
}

func (s *TelegramSender) Send(ctx context.Context, recipient string, payload any) error {
	userID, err := uuid.Parse(recipient)
	if err != nil {
		return fmt.Errorf("invalid recipient UUID: %w", err)
	}

	endpoint, err := s.repo.FindByUserAndProvider(ctx, userID, model.EndpointProviderTelegram)
	if err != nil {
		return fmt.Errorf("failed to find telegram endpoint: %w", err)
	}
	if endpoint == nil {
		return fmt.Errorf("no telegram endpoint found for user %s", userID.String())
	}

	text, ok := payload.(string)
	if !ok {
		return fmt.Errorf("payload must be a string")
	}

	reqBody := map[string]any{
		"chat_id":    endpoint.Endpoint,
		"text":       text,
		"parse_mode": "HTML",
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/bot%s/sendMessage", s.apiURL, s.botToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("telegram API error: %s", resp.Status)
	}

	return nil
}
