package push

import (
	"errors"

	"github.com/google/uuid"
)

type PushValidator struct{}

func NewValidator() *PushValidator {
	return &PushValidator{}
}

func (v *PushValidator) Validate(recipient string) error {
	if recipient == "" {
		return errors.New("recipient user ID is empty")
	}
	_, err := uuid.Parse(recipient)
	if err != nil {
		return errors.New("recipient is not a valid user ID UUID")
	}
	return nil
}
