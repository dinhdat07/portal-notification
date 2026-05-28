package config

type LoggerConfig struct {
	Env    string
	Level  string
	Format string
}

type Config struct {
	Kafka       KafkaConfig
	SMTP        SMTPConfig
	Worker      WorkerConfig
	DBUrl       string
	Logger      LoggerConfig
	MetricsPort string
}

func Load() Config {
	loadEnv()
	return Config{
		Kafka:  LoadKafkaConfig(),
		SMTP:   LoadSMTPConfig(),
		Worker: LoadWorkerConfig(),
		DBUrl:  getEnv("DB_URL", ""),
		Logger: LoggerConfig{
			Env:    getEnv("ENV", "development"),
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "text"),
		},
		MetricsPort: getEnv("NOTIFICATION_METRICS_PORT", "9101"),
	}
}
