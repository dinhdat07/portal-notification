package email

import (
	"fmt"
	"regexp"
)

var emailRegex = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}$`)

type EmailValidator struct{}

func NewEmailValidator() *EmailValidator {
	return &EmailValidator{}
}

func (v *EmailValidator) Validate(recipient string) error {
	if recipient == "" {
		return fmt.Errorf("email recipient is required")
	}
	if !emailRegex.MatchString(recipient) {
		return fmt.Errorf("invalid email format: %s", recipient)
	}
	return nil
}
