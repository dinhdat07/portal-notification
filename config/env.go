package config

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/joho/godotenv"
)

var loadEnvOnce sync.Once

func loadEnv() {
	loadEnvOnce.Do(func() {
		envFile := os.Getenv("ENV_FILE")
		if envFile != "" {
			_ = godotenv.Load(envFile)
			return
		}

		_ = godotenv.Load()
	})
}
func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}

	return result
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func getIntEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func getFloatEnv(key string, fallback float64) float64 {
	loadEnv()

	val := os.Getenv(key)
	if val == "" {
		return fallback
	}

	parsed, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return fallback
	}

	return parsed
}
