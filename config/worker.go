package config

import (
	"strconv"
	"time"
)

type WorkerConfig struct {
	FetchRetryInitialBackoff time.Duration
	FetchRetryMaxBackoff     time.Duration
	FetchRetryJitterRatio    float64
	MaxRetry                 int

	RetryWorkerInterval         time.Duration
	RetryWorkerBatchSize        int
	DeliveryRetryInitialBackoff time.Duration
	DeliveryRetryMaxBackoff     time.Duration
	DeliveryRetryJitterRatio    float64
}

func LoadWorkerConfig() WorkerConfig {
	return WorkerConfig{
		FetchRetryInitialBackoff: time.Duration(getIntEnv("EMAIL_WORKER_FETCH_RETRY_INITIAL_BACKOFF_SECONDS", 1)) * time.Second,
		FetchRetryMaxBackoff:     time.Duration(getIntEnv("EMAIL_WORKER_FETCH_RETRY_MAX_BACKOFF_SECONDS", 30)) * time.Second,
		FetchRetryJitterRatio:    getFloatEnv("EMAIL_WORKER_FETCH_RETRY_JITTER_RATIO", 0.2),
		MaxRetry:                 getIntEnv("NOTIFICATION_DELIVERY_MAX_RETRY", 3),

		RetryWorkerInterval:         getDurationEnv("NOTIFICATION_RETRY_WORKER_INTERVAL", 10*time.Second),
		RetryWorkerBatchSize:        getIntEnv("NOTIFICATION_RETRY_WORKER_BATCH_SIZE", 10),
		DeliveryRetryInitialBackoff: getDurationEnv("NOTIFICATION_DELIVERY_RETRY_INITIAL_BACKOFF", 30*time.Second),
		DeliveryRetryMaxBackoff:     getDurationEnv("NOTIFICATION_DELIVERY_RETRY_MAX_BACKOFF", 30*time.Minute),
		DeliveryRetryJitterRatio:    getFloatEnv("NOTIFICATION_DELIVERY_RETRY_JITTER_RATIO", 0.2),
	}
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	val := getEnv(key, "")
	if val == "" {
		return fallback
	}
	if d, err := time.ParseDuration(val); err == nil {
		return d
	}
	if seconds, err := strconv.Atoi(val); err == nil {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}
