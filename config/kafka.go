package config

type KafkaConfig struct {
	Brokers                    []string
	NotificationRequestedTopic string
	EndpointRegisteredTopic    string
	ConsumerGroup              string
}

func LoadKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers: splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9092")),
		NotificationRequestedTopic: getEnv(
			"KAFKA_NOTIFICATION_REQUESTED_TOPIC",
			"notification.requested",
		),
		EndpointRegisteredTopic: getEnv(
			"KAFKA_ENDPOINT_REGISTERED_TOPIC",
			"notification.endpoint.registered",
		),
		ConsumerGroup: getEnv(
			"KAFKA_NOTIFICATION_CONSUMER_GROUP",
			"notification-service",
		),
	}
}
