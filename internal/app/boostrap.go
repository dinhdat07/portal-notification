package app

import (
	"context"
	"fmt"
	"log/slog"
	"portal-notification/config"
	emailchannel "portal-notification/internal/channel/email"
	kafkax "portal-notification/internal/infrastructure/kafka"
	logger "portal-notification/internal/infrastructure/logger"
	metricsx "portal-notification/internal/infrastructure/metrics"
	smtpx "portal-notification/internal/infrastructure/smtp"
	"portal-notification/internal/model"
	"portal-notification/internal/repository/impl"
	emailworker "portal-notification/internal/worker"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func New() (*App, error) {
	cfg := config.Load()

	slogLogger := logger.New(logger.Config{
		Env:    cfg.Logger.Env,
		Level:  cfg.Logger.Level,
		Format: cfg.Logger.Format,
	})
	slog.SetDefault(slogLogger)

	slogLogger.Info("starting_notification_service_bootstrap")

	slogLogger.Info("notification_service_config_loaded",
		slog.Any("kafka_brokers", cfg.Kafka.Brokers),
		slog.String("notification_topic", cfg.Kafka.NotificationRequestedTopic),
		slog.String("consumer_group", cfg.Kafka.ConsumerGroup),
		slog.String("smtp_host", cfg.SMTP.Host),
		slog.String("smtp_port", cfg.SMTP.Port),
		slog.Bool("smtp_use_auth", cfg.SMTP.UseAuth),
		slog.Bool("smtp_use_tls", cfg.SMTP.UseTLS),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := kafkax.Ping(ctx, cfg.Kafka.Brokers); err != nil {
		return nil, fmt.Errorf("verify kafka connection: %w", err)
	}

	if err := smtpx.Ping(ctx, cfg.SMTP.Host, cfg.SMTP.Port); err != nil {
		return nil, fmt.Errorf("verify smtp connection: %w", err)
	}

	db, err := gorm.Open(postgres.Open(cfg.DBUrl), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&model.NotificationDelivery{}); err != nil {
		return nil, fmt.Errorf("auto migrate notification deliveries: %w", err)
	}

	reader := kafkax.NewReader(
		cfg.Kafka.Brokers,
		[]string{cfg.Kafka.NotificationRequestedTopic},
		cfg.Kafka.ConsumerGroup,
	)
	slogLogger.Info("kafka_reader_initialized")

	deliveryRepo := impl.NewGormDeliveryRepository(db)
	txManager := impl.NewGormTxManager(db)

	smtpMailer := smtpx.NewMailer(smtpx.Config{
		Host:     cfg.SMTP.Host,
		Port:     cfg.SMTP.Port,
		UseAuth:  cfg.SMTP.UseAuth,
		UseTLS:   cfg.SMTP.UseTLS,
		Username: cfg.SMTP.Username,
		Password: cfg.SMTP.Password,
		From:     cfg.SMTP.From,
		FromName: cfg.SMTP.FromName,
	})
	slogLogger.Info("smtp_mailer_initialized")

	emailCBProxy := emailchannel.NewCircuitBreakerProxy(
		smtpMailer,
		cfg.SMTP.CircuitBreaker,
	)
	slogLogger.Info("email_circuit_breaker_initialized")
	emailMetrics, retryMetrics := metricsx.NewPrometheusMetrics(prometheus.DefaultRegisterer)

	emailRenderer := emailchannel.NewEmailRenderer()
	emailSender := emailchannel.NewSender(emailRenderer, emailCBProxy)
	slogLogger.Info("email_sender_initialized")

	consumer := kafkax.NewConsumer(reader)

	worker := emailworker.NewWorker(consumer, emailSender, txManager, deliveryRepo, slogLogger, emailMetrics,
		emailworker.Config{
			FetchRetryInitialBackoff:    cfg.Worker.FetchRetryInitialBackoff,
			FetchRetryMaxBackoff:        cfg.Worker.FetchRetryMaxBackoff,
			MaxRetry:                    cfg.Worker.MaxRetry,
			DeliveryRetryInitialBackoff: cfg.Worker.DeliveryRetryInitialBackoff,
			DeliveryRetryMaxBackoff:     cfg.Worker.DeliveryRetryMaxBackoff,
			DeliveryRetryJitterRatio:    cfg.Worker.DeliveryRetryJitterRatio,
		},
	)
	slogLogger.Info("email_notification_worker_initialized")

	retryWorker := emailworker.NewRetryWorker(emailSender, deliveryRepo, slogLogger, retryMetrics,
		emailworker.RetryWorkerConfig{
			Interval:                    cfg.Worker.RetryWorkerInterval,
			BatchSize:                   cfg.Worker.RetryWorkerBatchSize,
			MaxRetry:                    cfg.Worker.MaxRetry,
			DeliveryRetryInitialBackoff: cfg.Worker.DeliveryRetryInitialBackoff,
			DeliveryRetryMaxBackoff:     cfg.Worker.DeliveryRetryMaxBackoff,
			DeliveryRetryJitterRatio:    cfg.Worker.DeliveryRetryJitterRatio,
		},
	)
	slogLogger.Info("email_notification_retry_worker_initialized")

	return &App{
		EmailWorker:  worker,
		RetryWorker:  retryWorker,
		KafkaReader:  reader,
		DB:           db,
		MetricsPort:  cfg.MetricsPort,
		KafkaBrokers: cfg.Kafka.Brokers,
		SMTPHost:     cfg.SMTP.Host,
		SMTPPort:     cfg.SMTP.Port,
	}, nil
}
