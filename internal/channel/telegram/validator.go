package telegram

import (
	"errors"

	"github.com/google/uuid"
)

type TelegramValidator struct{}

func NewValidator() *TelegramValidator {
	return &TelegramValidator{}
}

func (v *TelegramValidator) Validate(recipient string) error {
	if recipient == "" {
		return errors.New("recipient user ID is empty")
	}
	_, err := uuid.Parse(recipient)
	if err != nil {
		return errors.New("recipient is not a valid user ID UUID")
	}
	return nil
}
