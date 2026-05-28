package config

type KafkaConfig struct {
	Brokers                    []string
	NotificationRequestedTopic string
	ConsumerGroup              string
}

func LoadKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers: splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9092")),
		NotificationRequestedTopic: getEnv(
			"KAFKA_NOTIFICATION_REQUESTED_TOPIC",
			"notification.requested",
		),
		ConsumerGroup: getEnv(
			"KAFKA_NOTIFICATION_CONSUMER_GROUP",
			"notification-service",
		),
	}
}
