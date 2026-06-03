package app

import (
	"context"
	"fmt"
	"log/slog"
	"portal-notification/config"
	"portal-notification/internal/channel"
	emailchannel "portal-notification/internal/channel/email"
	kafkax "portal-notification/internal/infrastructure/kafka"
	logger "portal-notification/internal/infrastructure/logger"
	metricsx 	"portal-notification/internal/infrastructure/metrics"
	"portal-notification/internal/infrastructure/database"
	smtpx "portal-notification/internal/infrastructure/smtp"
	"portal-notification/internal/model"
	"portal-notification/internal/repository/impl"
	emailworker "portal-notification/internal/worker"
	"portal-notification/internal/channel/telegram"
	"portal-notification/internal/channel/push"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
	"github.com/prometheus/client_golang/prometheus"
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

	db, err := database.GetInstance(cfg.DBUrl)
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&model.NotificationDelivery{}, &model.NotificationEndpoint{}); err != nil {
		return nil, fmt.Errorf("auto migrate: %w", err)
	}

	reader := kafkax.NewReader(
		cfg.Kafka.Brokers,
		[]string{cfg.Kafka.NotificationRequestedTopic, cfg.Kafka.EndpointRegisteredTopic},
		cfg.Kafka.ConsumerGroup,
	)
	slogLogger.Info("kafka_reader_initialized")

	deliveryRepo := impl.NewGormDeliveryRepository(db)
	endpointRepo := impl.NewGormEndpointRepository(db)
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
	workerMetrics, retryMetrics := metricsx.NewPrometheusMetrics(prometheus.DefaultRegisterer)

	emailSender := emailchannel.NewSender(emailCBProxy)
	emailFactory := emailchannel.NewFactory(emailSender)

	var fcmClient *messaging.Client
	if cfg.Firebase.CredentialsFile != "" {
		opt := option.WithAuthCredentialsFile(option.ServiceAccount, cfg.Firebase.CredentialsFile)
		fbApp, err := firebase.NewApp(ctx, nil, opt)
		if err != nil {
			slogLogger.Warn("failed to initialize firebase app", slog.String("error", err.Error()))
		} else {
			fcmClient, err = fbApp.Messaging(ctx)
			if err != nil {
				slogLogger.Warn("failed to initialize firebase messaging", slog.String("error", err.Error()))
			}
		}
	} else {
		slogLogger.Warn("firebase_credentials_file_not_set")
	}

	telegramFactory := telegram.NewFactory(
		telegram.NewValidator(),
		telegram.NewTemplate(),
		telegram.NewSender(endpointRepo, cfg.Telegram.BotToken, cfg.Telegram.APIURL),
		telegram.NewRateLimiter(),
	)

	pushFactory := push.NewFactory(
		push.NewValidator(),
		push.NewTemplate(),
		push.NewSender(endpointRepo, fcmClient),
		push.NewRateLimiter(),
	)

	factories := map[string]channel.NotificationFactory{
		emailworker.ChannelEmail:    emailFactory,
		emailworker.ChannelTelegram: telegramFactory,
		emailworker.ChannelPush:     pushFactory,
	}

	router := emailworker.NewRouter()

	consumer := kafkax.NewConsumer(reader)

	worker := emailworker.NewWorker(
		consumer,
		router,
		factories,
		txManager,
		deliveryRepo,
		endpointRepo,
		slogLogger,
		workerMetrics,
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

	retryWorker := emailworker.NewRetryWorker(factories, deliveryRepo, slogLogger, retryMetrics,
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
