package telegram

import (
	"fmt"
)

type TelegramTemplate struct{}

func NewTemplate() *TelegramTemplate {
	return &TelegramTemplate{}
}

func (t *TelegramTemplate) Render(templateID string, data map[string]any) (any, error) {
	switch templateID {
	case "announcement":
		title, _ := data["title"].(string)
		content, _ := data["content"].(string)
		// Basic formatting for Telegram HTML
		text := fmt.Sprintf("<b>%s</b>\n\n%s", title, content)
		return text, nil
	default:
		return "", fmt.Errorf("unsupported telegram template: %s", templateID)
	}
}
