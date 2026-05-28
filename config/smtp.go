package config

type CircuitBreakerConfig struct {
	MaxRequests         uint32
	IntervalSeconds     int
	TimeoutSeconds      int
	ConsecutiveFailures uint32
}

type SMTPConfig struct {
	Host    string
	Port    string
	UseAuth bool
	UseTLS  bool

	Username string
	Password string
	From     string
	FromName string

	CircuitBreaker CircuitBreakerConfig
}

func LoadSMTPConfig() SMTPConfig {
	return SMTPConfig{
		Host:     getEnv("SMTP_HOST", "localhost"),
		Port:     getEnv("SMTP_PORT", "1025"),
		UseAuth:  getEnv("SMTP_USE_AUTH", "false") == "true",
		UseTLS:   getEnv("SMTP_USE_TLS", "false") == "true",
		Username: getEnv("SMTP_USERNAME", ""),
		Password: getEnv("SMTP_PASSWORD", ""),
		From:     getEnv("SMTP_FROM", "noreply@portal.local"),
		FromName: getEnv("SMTP_FROM_NAME", "Portal System"),

		CircuitBreaker: CircuitBreakerConfig{
			MaxRequests:         uint32(getIntEnv("SMTP_CB_MAX_REQUESTS", 5)),
			IntervalSeconds:     getIntEnv("SMTP_CB_INTERVAL_SECONDS", 60),
			TimeoutSeconds:      getIntEnv("SMTP_CB_TIMEOUT_SECONDS", 30),
			ConsecutiveFailures: uint32(getIntEnv("SMTP_CB_FAILURE_THRESHOLD", 5)),
		},
	}
}
