package push

import (
	"fmt"
	"firebase.google.com/go/v4/messaging"
)

type PushTemplate struct{}

func NewTemplate() *PushTemplate {
	return &PushTemplate{}
}

func (t *PushTemplate) Render(templateID string, data map[string]any) (any, error) {
	switch templateID {
	case "announcement":
		title, _ := data["title"].(string)
		content, _ := data["content"].(string)
		
		msg := &messaging.Message{
			Notification: &messaging.Notification{
				Title: title,
				Body:  content,
			},
		}
		return msg, nil
	default:
		return nil, fmt.Errorf("unsupported push template: %s", templateID)
	}
}
