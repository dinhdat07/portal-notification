package app

import (
	"context"
	"time"

	kafkax "portal-notification/internal/infrastructure/kafka"
	smtpx "portal-notification/internal/infrastructure/smtp"
	"portal-notification/internal/observability"
)

func (a *App) readinessReport(ctx context.Context) observability.Report {
	checks := map[string]string{
		"db":    observability.StatusError,
		"kafka": observability.StatusError,
		"smtp":  observability.StatusError,
	}

	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if a.DB != nil {
		if sqlDB, err := a.DB.DB(); err == nil && sqlDB.PingContext(checkCtx) == nil {
			checks["db"] = observability.StatusOK
		}
	}

	if err := kafkax.Ping(checkCtx, a.KafkaBrokers); err == nil {
		checks["kafka"] = observability.StatusOK
	}

	if err := smtpx.Ping(checkCtx, a.SMTPHost, a.SMTPPort); err == nil {
		checks["smtp"] = observability.StatusOK
	}

	return observability.NewReport(checks)
}
