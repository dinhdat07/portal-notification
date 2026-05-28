package email

import (
	"fmt"
	"html"
	"strings"
)

type EmailRenderer struct{}

func NewEmailRenderer() *EmailRenderer {
	return &EmailRenderer{}
}

func (r *EmailRenderer) Render(template string, name string, data map[string]any) (Message, error) {
	url, _ := data["url"].(string)
	username, _ := data["username"].(string)

	displayName := fallbackName(name)
	if displayName == "there" && strings.TrimSpace(username) != "" {
		displayName = username
	}

	switch template {
	case TemplateVerifyEmail:
		return renderVerifyEmail(displayName, url), nil
	case TemplateResetPassword:
		return renderResetPassword(displayName, url), nil
	case TemplateSetPassword:
		return renderSetPassword(displayName, url), nil
	default:
		return Message{}, fmt.Errorf("unsupported email template: %s", template)
	}
}

func renderVerifyEmail(name string, url string) Message {
	subject := "Verify your email"

	textBody := fmt.Sprintf(
		"Hello %s,\n\nPlease verify your email by clicking the link below:\n%s\n\nIf you did not create this account, you can ignore this email.\n",
		fallbackName(name),
		url,
	)

	htmlBody := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<title>Verify your email</title>
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #222;">
<p>Hello %s,</p>
<p>Please verify your email by clicking the button below:</p>
<p>
	<a href="%s" style="display:inline-block;padding:10px 16px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;">
	Verify Email
	</a>
</p>
<p>Or open this link manually:</p>
<p><a href="%s">%s</a></p>
<p>If you did not create this account, you can ignore this email.</p>
</body>
</html>`,
		html.EscapeString(fallbackName(name)),
		html.EscapeString(url),
		html.EscapeString(url),
		html.EscapeString(url),
	)

	return Message{
		Subject:  subject,
		TextBody: textBody,
		HTMLBody: htmlBody,
	}
}

func renderResetPassword(name string, url string) Message {
	subject := "Reset your password"

	textBody := fmt.Sprintf(
		"Hello %s,\n\nYou requested to reset your password.\nClick the link below to continue:\n%s\n\nIf you did not request this, you can ignore this email.\n",
		fallbackName(name),
		url,
	)

	htmlBody := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<title>Reset your password</title>
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #222;">
<p>Hello %s,</p>
<p>You requested to reset your password.</p>
<p>
	<a href="%s" style="display:inline-block;padding:10px 16px;background:#dc2626;color:#fff;text-decoration:none;border-radius:6px;">
	Reset Password
	</a>
</p>
<p>Or open this link manually:</p>
<p><a href="%s">%s</a></p>
<p>If you did not request this, you can ignore this email.</p>
</body>
</html>`,
		html.EscapeString(fallbackName(name)),
		html.EscapeString(url),
		html.EscapeString(url),
		html.EscapeString(url),
	)

	return Message{
		Subject:  subject,
		TextBody: textBody,
		HTMLBody: htmlBody,
	}
}

func renderSetPassword(name string, url string) Message {
	subject := "Set your password"

	textBody := fmt.Sprintf(
		"Hello %s,\n\nPlease set your password by clicking the link below:\n%s\n\nIf you were not expecting this email, you can ignore it.\n",
		fallbackName(name),
		url,
	)

	htmlBody := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<title>Set your password</title>
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #222;">
<p>Hello %s,</p>
<p>Please set your password by clicking the button below:</p>
<p>
	<a href="%s" style="display:inline-block;padding:10px 16px;background:#16a34a;color:#fff;text-decoration:none;border-radius:6px;">
	Set Password
	</a>
</p>
<p>Or open this link manually:</p>
<p><a href="%s">%s</a></p>
<p>If you were not expecting this email, you can ignore it.</p>
</body>
</html>`,
		html.EscapeString(fallbackName(name)),
		html.EscapeString(url),
		html.EscapeString(url),
		html.EscapeString(url),
	)

	return Message{
		Subject:  subject,
		TextBody: textBody,
		HTMLBody: htmlBody,
	}
}

func fallbackName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "there"
	}
	return name
}
